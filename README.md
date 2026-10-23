# Raft Key-Value Store

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that backs a small replicated key-value store. It is pure Python, uses only the standard
library, and talks over raw TCP.

## Requirements

- Python 3.10 or newer
- pytest, only for running the tests

```sh
python3 -m venv .venv
.venv/bin/pip install pytest
```

## Running a cluster

The cluster is described by `config/<port>_registry.txt`, one `host port` per line, including
the node itself. The included config is a five-node cluster on `localhost:10000`–`10004`.

Start each node in its own terminal, from the repository root:

```sh
python3 server.py 10000
python3 server.py 10001
python3 server.py 10002
python3 server.py 10003
python3 server.py 10004
```

A leader is elected a few seconds after a majority (3 of 5) is up. Stop a node with Ctrl+C.

Each node keeps its durable state (term, vote, log and snapshot) in `data/<port>/`. A
restarted node picks up where it left off. To start the cluster from scratch, stop every
node and delete `data/`.

## Using the client

```sh
python3 client.py
```

The client connects to `localhost:10000` and forwards each request to the current leader.
If that node is down, the client can't find the cluster. Commands:

| Command       | Effect                                             |
|---------------|----------------------------------------------------|
| `set key val` | Store `val` under `key` (`val` may contain spaces) |
| `get key`     | Print the value, or `None` if unset                |
| `delete key`  | Remove `key`                                       |

Example session:

```
client: set greeting hello world
greeting set to hello world
client: get greeting
hello world
client: delete greeting
greeting deleted
```

To see failover, kill the leader's terminal while the client is running. The remaining nodes
elect a new leader within a few seconds, and the client's next request goes to it.

## Running the tests

```sh
.venv/bin/pytest testing/
```

Run a single test, showing server output:

```sh
.venv/bin/pytest testing/test_leadership.py::TestLeadership::testFullClusterElections -s
```

The tests start real servers on ports 10000–10004, so stop any running nodes first. Each test
takes about 10–20 seconds. Servers print errors instead of raising them, so pass `-s` to see
them.

| File                    | Covers                                                                                        |
|-------------------------|-----------------------------------------------------------------------------------------------|
| `test_leadership.py`    | Elections, leader failover, a killed leader process                                           |
| `test_state_machine.py` | Writes apply on every node, reads skip the log, redirects, a leader without a majority refuses |
| `test_storage.py`       | On-disk state, log and snapshot files; cluster restarts; snapshotting; a lagging follower catching up from a snapshot |
| `test_connections.py`   | Two peers dialing each other at once, reconnecting after a restart                            |

## Project layout

| File            | Role                                                         |
|-----------------|--------------------------------------------------------------|
| `server.py`     | A Raft node: elections, replication, commit, client requests |
| `replicator.py` | The connection to one peer, with reconnects                  |
| `storage.py`    | Durable term/vote, the on-disk log, and snapshots            |
| `kv_store.py`   | The key-value state machine                                  |
| `client.py`     | Interactive client                                           |
| `messages.py`   | Client reply format, shared by server and client             |
| `sock_utils.py` | Wire format: a 4-byte length prefix followed by JSON         |

## Limitations

- Fixed membership (no cluster reconfiguration)
- No client request IDs, so non-idempotent commands could be applied twice
- Snapshots are sent whole rather than in chunks
- The client only knows one starting address
