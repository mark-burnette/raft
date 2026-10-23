# helpers for tests that run a small cluster in-process from a temporary registry

import threading
import time

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from server import Server  # nopep8


def use_config(tmp_path, monkeypatch, ports):
    # servers read config/<port>_registry.txt relative to the cwd
    (tmp_path / 'config').mkdir()
    for port in ports:
        (tmp_path / 'config' / f'{port}_registry.txt').write_text(
            ''.join(f'localhost {p}\n' for p in ports))
    monkeypatch.chdir(tmp_path)


def start(port):
    server = Server(('localhost', port))
    thread = threading.Thread(target=server.run)
    thread.start()
    return server, thread


def agreed_leader(servers):
    # the leader every server agrees on, or None if they disagree or have none.
    # followers only learn the leader from its AppendEntries, so agreement also proves
    # the leader can reach every one of them
    seen = set()
    for server in servers:
        with server.lock:
            seen.add((server.term, server.leader))

    return seen.pop()[1] if len(seen) == 1 else None


def wait_for_leader(servers, timeout):
    # poll instead of sleeping a fixed time: a split vote can need another election
    # round or two
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leader = agreed_leader(servers)
        if leader is not None:
            return leader
        time.sleep(0.2)
    return None


def stop_all(servers, threads):
    # a regression here can leave stop() blocked on a held lock; don't let that hang the test
    stopper = threading.Thread(
        target=lambda: [s.stop() for s in servers], daemon=True)
    stopper.start()
    stopper.join(timeout=5)
    assert not stopper.is_alive(), "Server.stop() hung"

    for thread in threads:
        thread.join()
