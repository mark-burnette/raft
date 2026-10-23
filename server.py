import socket
import threading
import sys
import errno
import random
import time

import os

from sock_utils import send, recv
from messages import ok, redirect, error

from kv_store import KeyValueStore
from replicator import Replicator
from storage import PersistentState, PersistentLog, Snapshot, data_dir

from role import Role


HEARTBEAT_INTERVAL = 1
ELECTION_TIMEOUT = (2.5, 3.5)
# how long a client request may wait on a majority before we give up on it
CLIENT_TIMEOUT = 5
# take a snapshot once this many entries have been applied since the last one
SNAPSHOT_INTERVAL = 10


class Server:
    def __init__(self, addr):
        self.s = None
        self.running = True

        self.addr = addr
        self.kvs = KeyValueStore()

        # guards all Raft state
        # methods ending in _locked expect the caller to already hold it.
        self.lock = threading.Lock()
        # notified whenever the election deadline moves, so the timer thread rechecks it
        self.election_cond = threading.Condition(self.lock)
        # notified whenever entries are applied, a heartbeat is acked, the term changes or we
        # stop, so waiting clients recheck
        self.client_cond = threading.Condition(self.lock)

        # durable: reloaded after a restart. only change term and voted_for through
        # _set_term_locked, which saves them; self.log saves itself as it changes
        path = data_dir(addr)
        self.state = PersistentState(os.path.join(path, 'state.json'))
        self.term = self.state.term
        self.voted_for = self.state.voted_for
        self.log = PersistentLog(os.path.join(path, 'log.jsonl'))
        self.snapshot = Snapshot(os.path.join(path, 'snapshot.json'))

        # we save a snapshot before compacting the log, so a crash between the two leaves
        # the log covering entries the snapshot already has; finish the job
        self.log.compact(self.snapshot.index, self.snapshot.term)

        # volatile: after a restart we start from the snapshot and re-apply the log after
        # it as we relearn the commit index from the leader. a snapshot only ever holds
        # applied entries, so everything it covers is committed
        self.kvs.restore(self.snapshot.data)

        # index of highest log entry known to be committed (-1 if none)
        self.commit_index = self.snapshot.index
        # index of highest log entry applied to state machine (-1 if none)
        self.last_applied = self.snapshot.index

        # {log index: term} of the entry a client is waiting on, and the results of those
        # entries once applied (the term check stops a client getting the result of a
        # different entry that replaced theirs)
        self.waiting = {}
        self.results = {}

        # for each server, holds index of next log entry to send to that server
        self.next_index = {}
        # for each server, holds index of highest log entry known to be replicated on server
        self.match_index = {}

        # every AppendEntries we send as leader carries a fresh seq, which followers echo
        # back; acked_seq holds the highest one each follower has answered. a read is safe
        # once a majority has answered one sent after it arrived (Raft §8)
        self.seq = 0
        self.acked_seq = {}

        self.role = Role.FOLLOWER
        self.leader = None
        self.votes = set()

        self.election_deadline = None
        with self.lock:
            self._reset_election_timer_locked()

        self.replicators = {}
        self.connect_to_servers()

    # ---- election ----

    def _reset_election_timer_locked(self):
        self.election_deadline = time.monotonic() + random.uniform(*ELECTION_TIMEOUT)
        self.election_cond.notify_all()

    def run_election_timer(self):
        while True:
            try:
                with self.lock:
                    # sleep until the deadline passes without being pushed back
                    while self.running:
                        if self.role == Role.LEADER:
                            # leaders don't time out; stepping down resets the timer and wakes us
                            self.election_cond.wait()
                            continue

                        remaining = self.election_deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self.election_cond.wait(remaining)

                    if not self.running:
                        return

                    rpc = self._start_election_locked()

                for rep in self.replicators.values():
                    rep.send_rpc(rpc)

                # we have sent the votes; on_vote will decide whether we have won the election
            except Exception as e:
                print("run_election_timer()", e)

    # change and save the durable term and vote. every change to them goes through here
    def _set_term_locked(self, term, voted_for):
        self.state.save(term, voted_for)
        self.term, self.voted_for = term, voted_for

    def _start_election_locked(self):
        self._set_term_locked(self.term + 1, self.addr)
        self.role = Role.CANDIDATE
        self.leader = None
        self.votes = {self.addr}

        # if this election stalls (e.g. split vote), start another
        self._reset_election_timer_locked()

        print(f"{self.addr}: starting election for term {self.term}")

        # a single-node cluster wins on its own vote
        self._check_votes_locked()

        log_term, log_index = self._last_log_locked()
        return {
            'op': 'request_vote',
            'term': self.term,
            'log_term': log_term,
            'log_index': log_index,
        }

    # whether count servers (including us) make up a majority of the cluster
    def _is_majority(self, count):
        return count > (len(self.replicators) + 1) // 2

    def _check_votes_locked(self):
        if self._is_majority(len(self.votes)):
            self._become_leader_locked()

    def _become_leader_locked(self):
        self.role = Role.LEADER
        self.leader = self.addr

        for addr in self.next_index:
            self.next_index[addr] = self.log.last_index() + 1
            self.match_index[addr] = -1
        self.acked_seq = {addr: -1 for addr in self.replicators}

        # commit entries from old term by replicating a no-op (Raft §8)
        self.log.append((self.term, {'op': 'noop'}))
        self.match_index[self.addr] = self.log.last_index()
        self._advance_commit_index_locked()

        print(f"{self.addr} is the leader for term {self.term}!")

        threading.Thread(target=self.heartbeat,
                         args=(self.term,), daemon=True).start()

    def _update_term_locked(self, term):
        # any message carrying a newer term means we're out of date: adopt it and revert to follower
        if term <= self.term:
            return

        was_leader = self.role == Role.LEADER

        self._set_term_locked(term, None)
        self.role = Role.FOLLOWER
        self.leader = None
        self.votes.clear()

        # clients waiting on our entries can no longer count on them committing
        self.client_cond.notify_all()

        if was_leader:
            # our deadline went stale while we were leading
            self._reset_election_timer_locked()

    def _last_log_locked(self):
        return self.log.last_term(), self.log.last_index()

    # candidate is requesting our vote
    def on_request_vote(self, candidate, req):
        try:
            with self.lock:
                self._update_term_locked(req['term'])

                def vote(granted, reason=''):
                    rpc = {'op': 'vote', 'term': self.term, 'granted': granted}
                    if not granted:
                        rpc['reason'] = reason
                    return rpc

                # if candidate is expired, don't vote
                if req['term'] < self.term:
                    return vote(False, 'outdated term')

                # check for out-of-date log
                log_term, log_index = self._last_log_locked()

                if req['log_term'] < log_term:
                    return vote(False, 'outdated log term')

                if req['log_term'] == log_term and req['log_index'] < log_index:
                    return vote(False, 'outdated log index')

                if self.voted_for is not None and self.voted_for != candidate:
                    return vote(False, 'already voted')

                self._set_term_locked(self.term, candidate)
                # granting a vote counts as hearing from a viable candidate
                self._reset_election_timer_locked()

                print(f"{self.addr[1]} -- VOTED -> {candidate[1]}")
                return vote(True)
        except Exception as e:
            print("on_request_vote()", e)

    # a peer has answered our request_vote
    def on_vote(self, voter, req):
        try:
            with self.lock:
                self._update_term_locked(req['term'])

                if (self.role != Role.CANDIDATE or
                        req['term'] != self.term or
                        not req['granted']):
                    return

                self.votes.add(voter)
                print(
                    f"({self.addr[1]}): {len(self.votes)} >? {(len(self.replicators)+1)//2}")
                self._check_votes_locked()
        except Exception as e:
            print("on_vote()", e)

    # ---- log replication ----

    def heartbeat(self, term):
        # runs for as long as we stay leader of this term
        while True:
            try:
                with self.lock:
                    if (not self.running or
                            self.role != Role.LEADER or
                            self.term != term):
                        return

                    rpcs = self._build_replication_all_locked()

                self._send_all(rpcs)

                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                print("heartbeat()", e)

    # the next message to bring addr's log up to date: usually AppendEntries, but an
    # InstallSnapshot if the entries it needs have been compacted away (Raft §7)
    def _build_replication_locked(self, addr):
        self.seq += 1

        if self.next_index[addr] <= self.snapshot.index:
            return {
                'op': 'install_snapshot',
                'id': self.addr,
                'body': {
                    'term': self.term,
                    'seq': self.seq,
                    'last_included_index': self.snapshot.index,
                    'last_included_term': self.snapshot.term,
                    'data': self.snapshot.data,  # TODO: send in chunks
                }
            }

        prev_log_index = self.next_index[addr]-1

        return {
            'op': 'append_entries',
            'id': self.addr,
            'body': {
                'term': self.term,
                'seq': self.seq,
                'prev_log_index': prev_log_index,
                'prev_log_term': self.log.term_at(prev_log_index),
                # TODO: batching
                'entries': self.log.entries_from(self.next_index[addr]),
                'leader_commit': self.commit_index
            }
        }

    def _build_replication_all_locked(self):
        return {addr: self._build_replication_locked(addr)
                for addr in self.replicators}

    # call without self.lock held
    def _send_all(self, rpcs):
        for addr, rpc in rpcs.items():
            self.replicators[addr].send_rpc(rpc)

    # whether a majority (counting us) has answered an AppendEntries with seq >= seq
    def _quorum_acked_locked(self, seq):
        acks = 1 + sum(acked >= seq for acked in self.acked_seq.values())
        return self._is_majority(acks)

    # whether an entry from our own term has committed. until one has, our commit_index
    # may be behind what earlier leaders committed (Raft §8)
    def _committed_in_term_locked(self):
        return (self.commit_index >= 0 and
                self.log.term_at(self.commit_index) == self.term)

    def _advance_commit_index_locked(self):
        # the highest index replicated on a majority of servers
        indices = sorted(self.match_index.values(), reverse=True)
        n = indices[len(indices) // 2]

        # only entries from our own term are committed by counting replicas (Raft §5.4.2)
        if n > self.commit_index and self.log.term_at(n) == self.term:
            self.commit_index = n
            self._apply_committed_locked()

    # apply newly committed entries to the state machine, in log order
    def _apply_committed_locked(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            term, cmd = self.log[self.last_applied]

            # the no-op exists only for Raft's sake (see _become_leader_locked)
            result = None if cmd['op'] == 'noop' else self.kvs.apply(cmd)
            if self.waiting.get(self.last_applied) == term:
                self.results[self.last_applied] = result

        if self.last_applied - self.snapshot.index >= SNAPSHOT_INTERVAL:
            self._take_snapshot_locked()

        self.client_cond.notify_all()

    # replace the applied part of the log with a snapshot of the state machine (Raft §7).
    # each server does this on its own; nothing about it is coordinated
    def _take_snapshot_locked(self):
        index = self.last_applied
        term = self.log.term_at(index)

        # snapshot first, then compact: if we crash in between, __init__ finishes the job
        self.snapshot.save(index, term, self.kvs.snapshot())
        self.log.compact(index, term)

    def connect_to_servers(self):
        try:
            with open(f'config/{self.addr[1]}_registry.txt', 'r') as f:
                for line in f:
                    server = line.split(' ')

                    addr = (server[0], int(server[1]))

                    self.next_index[addr] = 0
                    self.match_index[addr] = -1

                    if addr == self.addr:
                        continue

                    # set up all replicators once at startup so that we don't need a lock
                    r = Replicator(self, addr)
                    self.replicators[addr] = r

                    threading.Thread(target=r.run).start()
        except Exception as e:
            print("Server.find_servers()", e)

    # returns the reply for the client: one of ok(), redirect() or error()
    def handle_client_cmd(self, cmd):
        try:
            cmd = KeyValueStore.parse(cmd)
            if cmd is None:
                return error("unknown command")

            # reads don't change state, so they skip the log
            if KeyValueStore.is_read(cmd):
                return self._handle_read(cmd)
            return self._handle_write(cmd)
        except Exception as e:
            print("handle_client_cmd()", e)
            return error("internal error")

    # before answering a read from our own store, confirm that a majority still follows us
    # and that we've applied everything committed before the read arrived (Raft §8)
    def _handle_read(self, cmd):
        with self.lock:
            # only the leader knows what has committed; anyone else could serve stale reads
            if self.role != Role.LEADER:
                return redirect(self.leader)

            term = self.term
            # every AppendEntries from here on carries seq >= target, so an answer to any of
            # them proves that follower still accepted us after this read arrived
            target = self.seq + 1
            rpcs = self._build_replication_all_locked()

        self._send_all(rpcs)

        with self.lock:
            confirmed = self.client_cond.wait_for(
                lambda: (self.term != term or not self.running or
                         (self._committed_in_term_locked() and
                          self._quorum_acked_locked(target))),
                timeout=CLIENT_TIMEOUT)

            if not self.running:
                return error("server shutting down")
            if self.term != term:
                # someone else leads now; reads are safe to retry there
                return redirect(self.leader)
            if not confirmed:
                return error("timed out waiting for a majority to confirm our leadership")

            # entries are applied the moment they commit, so the store already reflects
            # everything committed before the read arrived
            return ok(self.kvs.apply(cmd))

    # append a write to the log and reply once it has been applied
    def _handle_write(self, cmd):
        with self.lock:
            if self.role != Role.LEADER:
                return redirect(self.leader)

            self.log.append((self.term, cmd))
            index, term = self.log.last_index(), self.term
            self.waiting[index] = term

            self.match_index[self.addr] = index
            self._advance_commit_index_locked()

            rpcs = self._build_replication_all_locked()

        self._send_all(rpcs)

        with self.lock:
            # wait for our entry to be applied. a new term means it may never commit, and
            # without a majority it can't, so don't wait forever
            self.client_cond.wait_for(lambda: (self.last_applied >= index or
                                               self.term != term or
                                               not self.running),
                                      timeout=CLIENT_TIMEOUT)
            del self.waiting[index]

            if index in self.results:
                return ok(self.results.pop(index))

            # lost leadership, timed out or shutting down: the entry could still commit later,
            # so the client can't safely assume either way
            return error("couldn't confirm the command committed; it may or may not take effect")

    # leader is replicating its log to us
    def on_append_entries(self, rpc):
        try:
            body = rpc['body']
            leader_term = body['term']
            prev_log_index = body['prev_log_index']
            prev_log_term = body['prev_log_term']
            entries = body['entries']
            leader_commit = body['leader_commit']

            with self.lock:
                self._update_term_locked(leader_term)

                # if leader has expired
                if leader_term < self.term:
                    return {
                        'op': 'append_entries_response',
                        'success': False,
                        'reason': 'leader_expired',
                        'term': self.term,
                    }

                # this is the legitimate leader for our term
                self.role = Role.FOLLOWER
                self.leader = tuple(rpc['id'])
                self._reset_election_timer_locked()

                # our log may run past what this message covers, but only up to here is it
                # known to match the leader's
                last_new = prev_log_index + len(entries)

                if prev_log_index < self.log.snapshot_index:
                    # everything up to our snapshot is committed, so it already matches the
                    # leader's log; only the entries after it need checking and merging
                    entries = entries[self.log.snapshot_index -
                                      prev_log_index:]
                    prev_log_index = self.log.snapshot_index
                elif (prev_log_index > self.log.last_index() or
                        self.log.term_at(prev_log_index) != prev_log_term):
                    # we have missing or erroneous log entries
                    return {
                        'op': 'append_entries_response',
                        'success': False,
                        'reason': 'bad_entries',
                        'term': self.term,
                        'seq': body['seq'],
                    }

                self._merge_entries_locked(prev_log_index + 1, entries)

                if leader_commit > self.commit_index:
                    # never backwards: a message overlapping our snapshot can have a
                    # last_new below what we've already committed
                    self.commit_index = max(self.commit_index,
                                            min(leader_commit, last_new))
                    self._apply_committed_locked()

                return {
                    'op': 'append_entries_response',
                    'success': True,
                    'term': self.term,
                    'seq': body['seq'],
                    'match_index': last_new
                }
        except Exception as e:
            print("on_append_entries()", e)

    def _merge_entries_locked(self, start, entries):
        for i, entry in enumerate(entries):
            index = start + i
            if index <= self.log.last_index():
                if self.log.term_at(index) == entry[0]:
                    continue
                self.log.truncate(index)

            self.log.extend([tuple(e) for e in entries[i:]])
            return

    # the leader has compacted entries we still need, so it sent its snapshot instead
    # (Raft §7). answered like an AppendEntries, so on_append_entries_response handles both
    def on_install_snapshot(self, rpc):
        try:
            body = rpc['body']
            index = body['last_included_index']
            term = body['last_included_term']

            with self.lock:
                self._update_term_locked(body['term'])

                if body['term'] < self.term:
                    return {
                        'op': 'append_entries_response',
                        'success': False,
                        'reason': 'leader_expired',
                        'term': self.term,
                    }

                self.role = Role.FOLLOWER
                self.leader = tuple(rpc['id'])
                self._reset_election_timer_locked()

                # if we've already applied everything it covers, it tells us nothing new
                if index > self.commit_index:
                    # snapshot first, then compact, as in _take_snapshot_locked
                    self.snapshot.save(index, term, body['data'])
                    self.log.compact(index, term)

                    self.kvs.restore(body['data'])
                    self.commit_index = self.last_applied = index

                # either way our log now matches the leader's up to index: the snapshot's
                # entries are all committed
                return {
                    'op': 'append_entries_response',
                    'success': True,
                    'term': self.term,
                    'seq': body['seq'],
                    'match_index': index
                }
        except Exception as e:
            print("on_install_snapshot()", e)

    # a follower has answered our append_entries; returns a retry rpc if one is needed
    def on_append_entries_response(self, follower, resp):
        try:
            with self.lock:
                self._update_term_locked(resp['term'])

                # we stepped down, or this response is from an earlier term
                if self.role != Role.LEADER or resp['term'] != self.term:
                    return None

                # any answer in our term, success or not, means this follower still accepts
                # us as leader; reads waiting on a majority may now proceed
                if resp['seq'] > self.acked_seq[follower]:
                    self.acked_seq[follower] = resp['seq']
                    self.client_cond.notify_all()

                if resp['success']:
                    self.next_index[follower] = resp['match_index']+1
                    # most responses answer heartbeats; no need to recalculate commit index
                    if resp['match_index'] > self.match_index[follower]:
                        self.match_index[follower] = resp['match_index']
                        self._advance_commit_index_locked()
                    return None

                if resp['reason'] == 'bad_entries':
                    # back up by one and retry
                    self.next_index[follower] = max(
                        0, self.next_index[follower]-1)
                    return self._build_replication_locked(follower)
        except Exception as e:
            print("on_append_entries_response()", e)

    # callback for incoming clients
    def handle_client(self, conn, addr):
        with conn:
            while self.running:
                try:
                    req = recv(conn)
                    if not req:
                        print(f"{addr} disconnected.")
                        break

                    op = req['op']

                    match op:
                        case x if x in ['get', 'set', 'delete']:
                            reply = self.handle_client_cmd(req)
                            send(conn, reply)
                        case _:
                            send(conn, error("unknown command"))

                    req = None
                except TimeoutError:
                    pass

    # answer a peer's handshake: always reply, so the dialer is never left waiting
    def accept_peer(self, conn, peer):
        r = self.replicators.get(peer)

        # only lower addresses dial us. a redial replaces any connection we still have to
        # that peer (see Replicator.attach), so there's no need to check for one here
        accepted = r is not None and not r.initiator

        try:
            send(conn, {
                'op': 'ack',
                'response': 'accept' if accepted else 'reject'
            })
        except OSError:
            accepted = False

        # attach only after the ack, so no RPC can reach the dialer ahead of it
        if not accepted or r.attach(conn) is None:
            conn.close()

    def run(self):
        # main connection-accepting loop
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            with self.s:
                self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                self.s.bind(self.addr)
                self.s.listen()

                print(f"Set up on {self.addr}")

                election_thread = threading.Thread(
                    target=self.run_election_timer, daemon=True)
                election_thread.start()

                while self.running:
                    try:
                        conn, addr = self.s.accept()

                        # authentication loop
                        req = None
                        while req is None:
                            req = recv(conn)
                            if not req:
                                print(f"{addr} disconnected.")
                                break

                            if req['op'] != 'authenticate':
                                send(
                                    conn, {'op': 'error', 'msg': 'unknown request'})
                                req = None

                        if not req:
                            conn.close()
                            continue

                        # other party has authenticated
                        if req['role'] == 'server':
                            self.accept_peer(conn, tuple(req['ret_addr']))
                        else:
                            threading.Thread(target=self.handle_client,
                                             args=(conn, addr)).start()
                    except TimeoutError:
                        pass
        except Exception as e:
            print("Server.run()", e)
        finally:
            print("Closing server...")

    def stop(self):
        try:
            with self.lock:
                self.running = False
                self.election_cond.notify_all()
                self.client_cond.notify_all()
                # a handler that runs after this fails to write and is caught, so a stopped
                # server can never change what a restarted one loads
                self.log.close()

            for rep in self.replicators.values():
                rep.stop()

            if self.s:
                self.s.shutdown(socket.SHUT_RDWR)
        except OSError as e:
            # EBADF: run() already closed the socket on its way out
            if e.errno not in (errno.ENOTCONN, errno.EBADF):
                raise
        finally:
            try:
                if self.s:
                    self.s.close()
            except OSError:
                pass


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"usage: python3 ./server.py [port]")
        exit()

    server = Server(('localhost', int(sys.argv[1])))
    try:
        server.run()
    except KeyboardInterrupt:
        pass
    finally:
        # run() only owns the accept loop; without this, the replicator and heartbeat
        # threads outlive it and the process keeps acting as a (headless) node
        server.stop()
