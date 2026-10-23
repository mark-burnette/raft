import pytest
import time

import sys
from pathlib import Path

cur_dir = Path(__file__).resolve().parent
parent_dir = cur_dir.parent

sys.path.append(str(parent_dir))

from cluster import use_config, start, wait_for_leader, stop_all  # nopep8
from client import Client  # nopep8
from storage import PersistentState, PersistentLog, Snapshot  # nopep8
import server as server_module  # nopep8


PORTS = (10000, 10001, 10002)


class TestPersistentState():
    def testDefaultsWithoutAFile(self, tmp_path):
        state = PersistentState(str(tmp_path / 'state.json'))
        assert (state.term, state.voted_for) == (0, None)

    def testRoundTrip(self, tmp_path):
        path = str(tmp_path / 'state.json')
        PersistentState(path).save(7, ('localhost', 10001))

        state = PersistentState(path)
        assert (state.term, state.voted_for) == (7, ('localhost', 10001))

        state.save(8, None)
        assert PersistentState(path).voted_for is None


NOOP = {'op': 'noop'}


def setcmd(key, val):
    return {'op': 'set', 'key': key, 'val': val}


class TestPersistentLog():
    def testAppendTruncateAndReload(self, tmp_path):
        path = str(tmp_path / 'log.jsonl')
        log = PersistentLog(path)
        assert (log.last_index(), log.last_term()) == (-1, 0)

        log.append((1, NOOP))
        log.extend([(1, setcmd('a', '1')), (1, setcmd('b', '2'))])
        log.truncate(2)
        log.append((2, {'op': 'delete', 'key': 'a'}))
        log.close()

        expected = [(1, NOOP), (1, setcmd('a', '1')), (2, {'op': 'delete', 'key': 'a'})]
        reloaded = PersistentLog(path)
        assert list(reloaded) == expected
        assert (reloaded.last_index(), reloaded.last_term()) == (2, 2)
        assert reloaded[2] == expected[2] and reloaded.entries_from(1) == expected[1:]

        # truncating after a reload must cut at the right byte offsets too
        reloaded.truncate(1)
        reloaded.append((3, NOOP))
        reloaded.close()
        assert list(PersistentLog(path)) == [(1, NOOP), (3, NOOP)]

    def testPartialLastLineIsDropped(self, tmp_path):
        path = tmp_path / 'log.jsonl'
        log = PersistentLog(str(path))
        log.append((1, NOOP))
        log.close()

        # what a crash halfway through writing the next entry leaves behind
        with open(path, 'ab') as f:
            f.write(b'[1, {"op": "se')

        log = PersistentLog(str(path))
        assert list(log) == [(1, NOOP)]

        # and the partial line is gone from the file, so new entries aren't glued onto it
        log.append((2, NOOP))
        log.close()
        assert list(PersistentLog(str(path))) == [(1, NOOP), (2, NOOP)]

    def testCompactKeepsIndexesAbsolute(self, tmp_path):
        path = str(tmp_path / 'log.jsonl')
        log = PersistentLog(path)
        log.extend([(1, setcmd(str(i), str(i))) for i in range(5)])

        log.compact(2, 1)
        assert (log.snapshot_index, log.snapshot_term) == (2, 1)
        assert list(log) == [(1, setcmd('3', '3')), (1, setcmd('4', '4'))]
        assert log.last_index() == 4 and log[3] == (1, setcmd('3', '3'))
        # the last compacted entry's term is still known, for AppendEntries' prev_log_term
        assert log.term_at(2) == 1
        with pytest.raises(IndexError):
            log[2]

        # appends and truncates after a compaction still land at the right offsets
        log.truncate(4)
        log.append((2, NOOP))
        log.close()

        reloaded = PersistentLog(path)
        assert reloaded.snapshot_index == 2
        assert list(reloaded) == [(1, setcmd('3', '3')), (2, NOOP)]
        assert reloaded.last_index() == 4

    def testCompactPastAConflictDropsEverything(self, tmp_path):
        log = PersistentLog(str(tmp_path / 'log.jsonl'))
        log.extend([(1, NOOP), (1, NOOP), (1, NOOP)])

        # a leader's snapshot whose last entry disagrees with ours (different term), or
        # reaches past our log: nothing we have can be trusted to follow on from it
        log.compact(1, 2)
        assert (log.snapshot_index, list(log), log.last_index()) == (1, [], 1)

        log.compact(9, 3)
        assert (log.snapshot_index, list(log), log.last_index()) == (9, [], 9)


class TestSnapshot():
    def testRoundTrip(self, tmp_path):
        path = str(tmp_path / 'snapshot.json')
        snapshot = Snapshot(path)
        assert (snapshot.index, snapshot.term, snapshot.data) == (-1, 0, {})

        snapshot.save(19, 3, {'a': '1'})
        reloaded = Snapshot(path)
        assert (reloaded.index, reloaded.term, reloaded.data) == (19, 3, {'a': '1'})


class TestRecovery():
    def testClusterRestartKeepsData(self, tmp_path, monkeypatch):
        use_config(tmp_path, monkeypatch, PORTS)

        nodes = [start(p) for p in PORTS]
        try:
            servers = [s for s, _ in nodes]
            leader = wait_for_leader(servers, timeout=15)
            assert leader is not None

            client = Client(leader)
            for line in ['set a 1', 'set b 2', 'delete a']:
                assert client.request(Client.parse(line))['status'] == 'ok'
            client.close()

            before = {}
            for server in servers:
                with server.lock:
                    before[server.addr] = (server.term, server.voted_for)
        finally:
            stop_all(*zip(*nodes))

        # bring every server back from nothing but what it wrote to disk
        nodes = [start(p) for p in PORTS]
        try:
            servers = [s for s, _ in nodes]

            for server in servers:
                with server.lock:
                    # term and vote come straight back. (an election may already have
                    # started and moved them on, but never backwards)
                    term, voted_for = before[server.addr]
                    assert server.term >= term
                    if server.term == term:
                        assert server.voted_for == voted_for

            leader = wait_for_leader(servers, timeout=15)
            assert leader is not None

            # the store starts empty; the new leader re-applies the saved log once its
            # no-op commits
            client = Client(leader)
            assert client.request(Client.parse('get b')) == {
                'status': 'ok', 'result': '2'}
            assert client.request(Client.parse('get a')) == {
                'status': 'ok', 'result': None}
            client.close()
        finally:
            stop_all(*zip(*nodes))


def write_keys(leader, n):
    client = Client(leader)
    for i in range(n):
        assert client.request(Client.parse(f'set k{i} {i}'))['status'] == 'ok'
    client.close()
    return {f'k{i}': str(i) for i in range(n)}


def wait_for(condition, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.2)
    return condition()


def store_of(server):
    with server.lock:
        return dict(server.kvs.store)


class TestSnapshotting():
    def testSnapshotsCompactTheLogAndSurviveRestart(self, tmp_path, monkeypatch):
        use_config(tmp_path, monkeypatch, PORTS)
        interval = server_module.SNAPSHOT_INTERVAL

        nodes = [start(p) for p in PORTS]
        try:
            servers = [s for s, _ in nodes]
            leader = wait_for_leader(servers, timeout=15)
            assert leader is not None

            # the leader's no-op plus these: enough for every server to snapshot twice
            expected = write_keys(leader, 2 * interval + 5)
            assert wait_for(lambda: all(store_of(s) == expected for s in servers), 5)

            for server in servers:
                with server.lock:
                    # every server snapshots on its own, and keeps fewer than an
                    # interval's worth of entries after its latest snapshot
                    assert server.snapshot.index >= interval - 1
                    assert server.log.last_index() - server.log.snapshot_index < interval

                # and the file on disk is just as short: a header plus those entries
                lines = (tmp_path / 'data' / str(server.addr[1]) /
                         'log.jsonl').read_text().splitlines()
                assert len(lines) <= interval
        finally:
            stop_all(*zip(*nodes))

        nodes = [start(p) for p in PORTS]
        try:
            servers = [s for s, _ in nodes]

            # straight from disk, before any election: the store is the snapshot's
            for server in servers:
                with server.lock:
                    assert server.snapshot.index >= interval - 1
                    assert server.last_applied == server.snapshot.index
                    assert server.kvs.store == server.snapshot.data

            # and once a leader re-applies the log after the snapshot, everything is back
            leader = wait_for_leader(servers, timeout=15)
            assert leader is not None
            assert wait_for(lambda: all(store_of(s) == expected for s in servers), 5)
        finally:
            stop_all(*zip(*nodes))

    def testLaggingFollowerCatchesUpFromSnapshot(self, tmp_path, monkeypatch):
        use_config(tmp_path, monkeypatch, PORTS)
        interval = server_module.SNAPSHOT_INTERVAL

        installs = []
        orig_install = server_module.Server.on_install_snapshot

        def counting_install(self, rpc):
            installs.append(self.addr)
            return orig_install(self, rpc)

        monkeypatch.setattr(server_module.Server,
                            'on_install_snapshot', counting_install)

        nodes = {p: start(p) for p in PORTS}
        try:
            leader = wait_for_leader([s for s, _ in nodes.values()], timeout=15)
            assert leader is not None

            # take a follower down, and commit enough without it that the leader compacts
            # away entries the follower never received
            lagging = next(p for p in PORTS if p != leader[1])
            server, thread = nodes[lagging]
            server.stop()
            thread.join()

            expected = write_keys(leader, 2 * interval + 5)
            leader_server = nodes[leader[1]][0]
            with leader_server.lock:
                assert leader_server.snapshot.index > server.log.last_index()

            nodes[lagging] = start(lagging)
            follower = nodes[lagging][0]

            assert wait_for(lambda: store_of(follower) == expected, 15)
            assert ('localhost', lagging) in installs
        finally:
            stop_all(*zip(*nodes.values()))
