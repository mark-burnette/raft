# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A from-scratch Raft consensus implementation backing a replicated key-value store. It is pure Python (3.10+ for `match`; the venv uses 3.14), uses only the standard library, and talks over raw TCP sockets. The only dependency is pytest, installed in `.venv`.

## Commands

- Run a node: `python3 server.py <port>`. It reads `config/<port>_registry.txt` and keeps its durable state in `data/<port>/`, both relative to the cwd. Delete `data/` to start a cluster from scratch (and after any change to the on-disk format).
- Interactive client: `python3 client.py` (starts at `localhost:10000` and follows redirects to the leader). Commands: `get k`, `set k v`, `delete k`
- Run tests: `.venv/bin/pytest testing/`
- Single test: `.venv/bin/pytest testing/test_leadership.py::TestLeadership::testFullClusterElections -s`

Tests start real servers on ports 10000–10004, so they are slow (about 10–20s each) and fail if those ports are taken. They use `testing/cluster.py`. `use_config` writes a temporary `config/` and chdirs into it, so each test also gets its own `data/`. Without that, servers would reload terms and logs left on disk by earlier tests. `wait_for_leader` polls instead of sleeping, and `stop_all` stops servers without hanging.

## Architecture

- **`server.py` / `Server`**: one Raft node, and home of all the Raft logic. It holds the term, voted_for and the log (durable, see `storage.py`), commit_index (-1 = none), next_index/match_index (keyed by address and including its own address), role, leader and votes. `run()` accepts connections and dispatches on the first `authenticate` message. Peers (`role: server`, `ret_addr`) get attached to the existing `Replicator` for that address. Clients get their own `handle_client` thread. Incoming RPCs are handled by `on_append_entries`, `on_install_snapshot`, `on_append_entries_response`, `on_request_vote` and `on_vote`, each of which returns the reply to send back, or `None`. Background threads: `run_election_timer` (waits on `election_cond` until `election_deadline`, a randomized 2.5–3.5s timeout that is pushed back by valid AppendEntries and granted votes) and `heartbeat(term)` (only on the leader, sends `_build_replication_locked`'s message to every peer every 1s, exits once the node is no longer leader of that term).
- **`replicator.py` / `Replicator`**: one per peer, all created at startup from the registry file. Each owns the single bidirectional socket to its peer. Only the lower address dials (`initiator`): its `connect()` does the authenticate/ack handshake and then `attach()`es the socket. The higher side blocks in `wait_for_peer()` until `Server.accept_peer()` acks the incoming connection and attaches it. Because only one side dials, two peers can never dial each other at once. `Replicator.run()` loops: establish the connection (initiator redials every `RETRY_INTERVAL`, other side waits), `serve()` it until it drops, `detach()`, repeat. An incoming redial replaces any socket still attached, so the newest connection always wins. `serve()` only forwards each message to the matching `Server.on_*` handler and sends back the reply; the replicator holds no Raft state. The server's own threads send through `send_rpc()`.
- **`sock_utils.py`**: wire format is a 4-byte big-endian length prefix followed by a JSON payload. Every message is a dict with an `op` key. JSON turns tuples into lists, so addresses and log entries are converted back with `tuple(...)` on receipt.
- **`storage.py`**: the durable state (Raft Figure 2 and §7). Every file is written before the reply that depends on it.
  - `PersistentState` keeps `term` and `voted_for` in `state.json`, saved by writing a temporary file and renaming it over the old one. Change them only through `Server._set_term_locked`, which saves them.
  - `Snapshot` (`snapshot.json`) holds the store as of `index`/`term`, saved the same way. Once `SNAPSHOT_INTERVAL` (10) entries have been applied since the last one, `_take_snapshot_locked` saves a new snapshot and then compacts the log. Each server does this on its own.
  - `PersistentLog` is `self.log`: the entries after the snapshot. `log.jsonl` starts with a header line naming the snapshot index and term the log follows on from, then one JSON entry per line. `append`/`extend`/`truncate`/`compact` fsync before returning, and loading drops a partial last line left by a crash.
  - **Log indexes are absolute.** Entry `i` keeps its index after the entries before it are compacted. Use `last_index()`, `term_at(i)` (correct at `snapshot_index` and at -1), `entries_from(i)` and `log[i]` (only for `i > snapshot_index`). Never use `len(self.log)` as an index: iteration and `len` only see the entries still stored.
  - The log is still saved on disk even with snapshots. Its tail holds entries that aren't applied, or even committed, yet, and a follower may already have acknowledged them.
  - A snapshot is saved before the log is compacted. `Server.__init__` calls `log.compact(snapshot.index, snapshot.term)`, which finishes the compaction if a crash came between the two. It then restores the store and sets `commit_index`/`last_applied` from the snapshot, and re-applies the rest of the log as it relearns the commit index.
  - When a follower needs entries the leader has compacted (`next_index <= snapshot.index`), `_build_replication_locked` sends `install_snapshot` instead of AppendEntries, with the whole snapshot in one message. The follower replies in the AppendEntries response shape, so `on_append_entries_response` handles both. `compact` keeps the follower's entries after the snapshot only if its entry at the snapshot index has the snapshot's term; otherwise it drops the whole log (Raft §7).
  - Followers add entries with `_merge_entries_locked`: skip entries they already have, and truncate only from the first conflicting term (Raft §5.3), never otherwise. Accordingly, `on_append_entries` reports `prev_log_index + len(entries)` as its match index. It also skips any part of a message covered by its own snapshot, since those entries are committed and must already match.
- **`kv_store.py` / `KeyValueStore`**: the state machine, with no Raft knowledge and no lock of its own (`Server.lock` guards it). `parse` turns a client request into a command dict such as `{'op': 'set', 'key': 'a', 'val': '1'}`, `is_read` says whether it can skip the log, and `apply` runs it and returns the result, and `snapshot`/`restore` copy the whole store out and back in. Log entries are `(term, cmd)`. Each new leader also appends a `{'op': 'noop'}` entry so earlier terms' entries can commit (Raft §8); `_apply_committed_locked` skips it. Whenever `commit_index` advances, on the leader (`_advance_commit_index_locked`) or a follower (`on_append_entries`), `_apply_committed_locked` applies each newly committed entry.
- **Client commands** (`handle_client_cmd`): every reply is JSON built by `ok(result)`, `redirect(leader)` or `error(msg)` from `messages.py`, which the client shares. Non-leaders reply `redirect` with the leader they know of, or `null` mid-election. `Client.request` follows redirects, retries after `RETRY_DELAY` when the leader is unknown or a connection drops, and falls back to its `home` address.
  - `get` (`_handle_read`) skips the log but uses Raft §8's read-index check. Every AppendEntries carries a fresh `seq`, which followers echo back into `acked_seq`. The read sends a round of AppendEntries and is answered once a majority has acked a `seq` from that round and an entry from the leader's own term has committed (`_quorum_acked_locked`, `_committed_in_term_locked`).
  - `set`/`delete` (`_handle_write`) are appended and replicated, then the client thread waits until the entry is applied and replies with `KeyValueStore.apply`'s result (`waiting`/`results` match each entry to its client).
  - Both wait on `client_cond` for at most `CLIENT_TIMEOUT`. `wait_for` releases `Server.lock`, so only that client's thread blocks. A read that times out, or a write that times out or loses leadership, gets an `error`: the write's outcome is unknown. A read that loses leadership gets a `redirect`, since reads are safe to retry.
- **`config/<port>_registry.txt`**: cluster membership, one `host port` per line, including the node itself. All five files are currently identical.

## Current state / caveats

- No client request IDs (Raft §8): when the client resends after a dropped connection, a write may be applied twice. That's harmless for `set`/`delete`, which give the same result if applied twice, but a non-idempotent command would need deduplication.
- The client knows only its `home` address. If that server is down when the client starts or falls back, it can't find the leader.
- Snapshots are sent whole in one message, not in chunks, and the leader re-sends one with each heartbeat until the follower catches up. Fine for this store's size, not for a large one.
- Errors are generally caught and printed as `print("funcname()", e)` rather than raised, so failures show up only in stdout. Run pytest with `-s` to see them.

## Concurrency

- All Raft state on `Server` is guarded by the single `Server.lock`, and `election_cond` and `client_cond` are Conditions on that same lock. Methods ending in `_locked` expect the caller to already hold it.
- **Never do socket I/O while holding `Server.lock`.** Build the RPCs while holding it, release it, then send (see `heartbeat`, `_handle_write`).
- Disk writes (`storage.py`) are the deliberate exception: they happen while holding the lock, so durable state is on disk before any reply that depends on it goes out. Unlike socket I/O, they can't wait on another node, so they can't deadlock; they only add fsync latency.
- Each `Replicator` has its own `sock_lock`/`send_lock`, which only protect its socket. Never acquire `Server.lock` while holding them. `sock_lock` follows the same no-I/O rule as `Server.lock`: holding it across the handshake used to deadlock two peers that dialed each other at once (`testing/test_connections.py`).
