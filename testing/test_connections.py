import pytest
import threading
import time

import sys
from pathlib import Path

cur_dir = Path(__file__).resolve().parent
parent_dir = cur_dir.parent

sys.path.append(str(parent_dir))

from cluster import use_config, start, wait_for_leader, stop_all  # nopep8
from replicator import Replicator, RETRY_INTERVAL  # nopep8


PORTS = (10000, 10001)  # 10000 is the initiator for this pair


class TestConnections():
    def testSimultaneousDial(self, tmp_path, monkeypatch):
        # two peers that both try to connect at the same instant must still end up
        # connected. this used to deadlock: each side held its sock_lock while waiting
        # for an ack that the other side's accept loop needed that lock to send
        use_config(tmp_path, monkeypatch, PORTS)

        # hold every outgoing dial until both listeners are up, then release them together
        go = threading.Event()
        orig_connect = Replicator.connect

        def synced_connect(self):
            go.wait()
            return orig_connect(self)

        monkeypatch.setattr(Replicator, 'connect', synced_connect)

        servers, threads = zip(*(start(p) for p in PORTS))
        try:
            time.sleep(0.5)
            go.set()

            assert wait_for_leader(servers, timeout=15) is not None
        finally:
            stop_all(servers, threads)

    @pytest.mark.parametrize('restarted', PORTS)
    def testReconnectAfterRestart(self, tmp_path, monkeypatch, restarted):
        # whichever side of the pair restarts, the initiator must (re)dial and both
        # sides must go back to serving the new connection
        use_config(tmp_path, monkeypatch, PORTS)

        nodes = {p: start(p) for p in PORTS}
        try:
            assert wait_for_leader(
                [s for s, _ in nodes.values()], timeout=15) is not None

            server, thread = nodes[restarted]
            server.stop()
            thread.join()
            nodes[restarted] = start(restarted)

            # the initiator may dial before the new listener is up and have to wait out
            # one retry, then the pair may need to hold an election
            assert wait_for_leader(
                [s for s, _ in nodes.values()], timeout=RETRY_INTERVAL + 15) is not None
        finally:
            servers, threads = zip(*nodes.values())
            stop_all(servers, threads)
