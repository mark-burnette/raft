import pytest
import threading
import time

import re
import signal
import subprocess
import sys
from pathlib import Path

cur_dir = Path(__file__).resolve().parent
parent_dir = cur_dir.parent

sys.path.append(str(parent_dir))

from server import Server  # nopep8
from client import Client  # nopep8
from cluster import use_config  # nopep8


PORTS = range(10000, 10005)


class TestLeadership():
    @pytest.fixture(autouse=True)
    def fresh_cluster(self, tmp_path, monkeypatch):
        # each test gets its own config/ and data/, so no test inherits another's terms,
        # votes or logs from disk
        use_config(tmp_path, monkeypatch, PORTS)

    def testFullClusterElections(self):
        servers = []
        threads = []
        for i in range(5):
            servers.append(Server(('localhost', 10000+i)))
            threads.append(threading.Thread(target=servers[i].run))
            threads[i].start()

        time.sleep(10)

        # test that they all have the same leader
        leaders = set()
        for server in servers:
            with server.lock:
                leaders.add((server.term, server.leader))

        print(leaders)

        for server in servers:
            server.stop()

        for thread in threads:
            thread.join()

        assert len(leaders) == 1

    def testLeaderFailover(self):
        servers = []
        threads = []
        for i in range(5):
            servers.append(Server(('localhost', 10000+i)))
            threads.append(threading.Thread(target=servers[i].run))
            threads[i].start()

        time.sleep(10)

        with servers[0].lock:
            old_term, old_leader = servers[0].term, servers[0].leader

        # disconnect the leader; the remaining four should elect a new one
        idx = next(i for i, s in enumerate(servers) if s.addr == old_leader)
        servers[idx].stop()
        threads[idx].join()
        survivors = servers[:idx] + servers[idx+1:]

        time.sleep(10)

        leaders = set()
        for server in survivors:
            with server.lock:
                leaders.add((server.term, server.leader))

        print(leaders)

        for server in survivors:
            server.stop()

        for thread in threads:
            thread.join()

        assert old_leader is not None
        assert len(leaders) == 1

        new_term, new_leader = leaders.pop()
        assert new_leader is not None and new_leader != old_leader
        assert new_term > old_term

    def testLeaderProcessInterrupted(self, tmp_path):
        # same as testLeaderFailover, but with real server.py processes and a
        # Ctrl-C (SIGINT) to the leader, rather than calling Server.stop() directly
        ports = PORTS
        logs = {p: tmp_path / f"{p}.log" for p in ports}
        procs = {}

        def leaders_seen(port):
            # (term, leader port) for every "is the leader" line in a log
            return [(int(term), int(addr))
                    for addr, term in re.findall(r"'localhost', (\d+)\) is the leader for term (\d+)",
                                                 logs[port].read_text())]

        try:
            for p in ports:
                procs[p] = subprocess.Popen(
                    [sys.executable, '-u', str(parent_dir / 'server.py'), str(p)],
                    cwd=tmp_path, stdout=logs[p].open('w'), stderr=subprocess.STDOUT,
                    # make sure SIGINT behaves like a real Ctrl-C even if our parent ignores it
                    preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL))

            time.sleep(10)

            old_term, old_leader = max(t for p in ports for t in leaders_seen(p))

            procs[old_leader].send_signal(signal.SIGINT)
            try:
                procs[old_leader].wait(timeout=10)
            except subprocess.TimeoutExpired:
                pytest.fail("leader process kept running after SIGINT")

            time.sleep(10)

            new_term, new_leader = max(t for p in ports for t in leaders_seen(p))

            assert new_term > old_term
            assert new_leader != old_leader
        finally:
            for proc in procs.values():
                proc.kill()
                proc.wait()
