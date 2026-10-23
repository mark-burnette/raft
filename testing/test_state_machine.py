import pytest
import time

import sys
from pathlib import Path

cur_dir = Path(__file__).resolve().parent
parent_dir = cur_dir.parent

sys.path.append(str(parent_dir))

from cluster import use_config, start, wait_for_leader, stop_all  # nopep8
from client import Client  # nopep8
from sock_utils import send, recv  # nopep8
import server as server_module  # nopep8


PORTS = (10000, 10001, 10002)


def request(client, line):
    return client.request(Client.parse(line))


def result(client, line):
    reply = request(client, line)
    assert reply['status'] == 'ok', reply
    return reply['result']


def wait_for_stores(servers, expected, timeout):
    # followers apply an entry once a later AppendEntries tells them it committed,
    # so they can trail the leader by up to a heartbeat
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stores = []
        for server in servers:
            with server.lock:
                stores.append(dict(server.kvs.store))
        if all(store == expected for store in stores):
            return stores
        time.sleep(0.2)
    return stores


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    use_config(tmp_path, monkeypatch, PORTS)
    nodes = [start(p) for p in PORTS]
    servers, threads = zip(*nodes)
    try:
        leader = wait_for_leader(servers, timeout=15)
        assert leader is not None
        yield servers, leader
    finally:
        stop_all(servers, threads)


class TestStateMachine():
    def testCommandsApplyOnEveryServer(self, cluster):
        servers, leader = cluster
        client = Client(leader)

        # writes are answered once committed and applied, with the state machine's result
        assert result(client, 'set a 1') == "a set to 1"
        assert result(client, 'set b 2') == "b set to 2"
        assert result(client, 'delete a') == "a deleted"

        # so a read straight after them already sees them
        assert result(client, 'get b') == '2'
        assert result(client, 'get a') is None

        stores = wait_for_stores(servers, {'b': '2'}, timeout=5)
        assert stores == [{'b': '2'}] * len(servers)

    def testReadsAreNotReplicated(self, cluster):
        servers, leader = cluster
        client = Client(leader)

        result(client, 'set a 1')
        wait_for_stores(servers, {'a': '1'}, timeout=5)

        def logged_ops():
            ops = []
            for server in servers:
                with server.lock:
                    ops.append([cmd['op'] for _, cmd in server.log])
            return ops

        before = logged_ops()
        assert result(client, 'get a') == '1'
        time.sleep(1.5)  # long enough for a heartbeat to carry anything that was logged

        # every log is the leader's election no-op plus the one write, and the get added nothing
        assert logged_ops() == before == [['noop', 'set']] * len(servers)

    def testFollowersRedirect(self, cluster):
        servers, leader = cluster
        follower = next(s.addr for s in servers if s.addr != leader)
        client = Client(follower)

        # talk to the follower directly, without the client's redirect handling
        for cmd in [{'op': 'set', 'key': 'a', 'val': '1'}, {'op': 'get', 'key': 'a'}]:
            send(client.s, cmd)
            assert recv(client.s) == {'status': 'redirect', 'leader': list(leader)}

        for server in servers:
            with server.lock:
                assert server.kvs.store == {}

    def testClientFollowsRedirect(self, cluster):
        servers, leader = cluster
        follower = next(s.addr for s in servers if s.addr != leader)
        client = Client(follower)

        assert result(client, 'set a 1') == "a set to 1"
        assert client.addr == leader
        assert result(client, 'get a') == '1'

        assert wait_for_stores(servers, {'a': '1'}, timeout=5) == [
            {'a': '1'}] * len(servers)

    def testLeaderWithoutMajorityRefuses(self, cluster, monkeypatch):
        servers, leader = cluster
        monkeypatch.setattr(server_module, 'CLIENT_TIMEOUT', 1)
        client = Client(leader)

        assert result(client, 'set a 1') == "a set to 1"
        assert result(client, 'get a') == '1'

        # cut the leader off from the rest of the cluster. it still believes it leads, but
        # it can no longer prove that to itself, so it must not serve reads (they could be
        # stale) or claim writes committed (they can't)
        for server in servers:
            if server.addr != leader:
                server.stop()

        for line in ['get a', 'set a 2']:
            reply = request(client, line)
            assert reply['status'] == 'error', reply
