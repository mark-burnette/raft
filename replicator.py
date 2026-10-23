import socket
import threading
from sock_utils import send, recv


RETRY_INTERVAL = 5
HANDSHAKE_TIMEOUT = 5


def close_socket(sock):
    # shutdown first so a thread blocked in recv() on this socket wakes up
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


# owns the connection to one peer. all Raft logic lives in Server; this class only
# moves messages between the socket and the server's on_* handlers
class Replicator():
    def __init__(self, server, addr):
        self.running = True
        self.server = server
        self.addr = addr

        # only the lower address dials, so two peers can never connect to each other
        # at the same time; the higher one waits for Server.run to attach() the
        # incoming connection instead
        self.initiator = server.addr < addr

        self.sock = None
        # guards self.sock only; never held during network I/O
        self.sock_lock = threading.Lock()
        # notified when a socket is attached or the replicator is stopped
        self.sock_cond = threading.Condition(self.sock_lock)
        self.send_lock = threading.Lock()

    # dial the peer until it accepts our handshake; returns the socket, or None if stopped
    def connect(self):
        while self.running:
            sock = None
            try:
                sock = socket.create_connection(
                    self.addr, timeout=HANDSHAKE_TIMEOUT)

                # nobody else can see this socket yet, so no locks needed
                send(sock, {
                    'op': 'authenticate',
                    'role': 'server',
                    'ret_addr': self.server.addr,
                })

                ack = recv(sock)
                if ack and ack.get('response') == 'accept':
                    sock.settimeout(None)
                    return self.attach(sock)
            except OSError as e:
                # print(f"Replicator.connect({self.addr})", e)
                pass

            if sock:
                sock.close()

            # wait before retrying, but wake right away if stop() is called
            with self.sock_cond:
                self.sock_cond.wait_for(
                    lambda: not self.running, RETRY_INTERVAL)

        return None

    def wait_for_peer(self):
        with self.sock_cond:
            while self.running and self.sock is None:
                self.sock_cond.wait()
            return self.sock

    # install an authenticated socket, replacing any older one
    def attach(self, sock):
        with self.sock_cond:
            if self.running:
                to_close, self.sock = self.sock, sock
                self.sock_cond.notify_all()
            else:
                to_close, sock = sock, None

        # the initiator only redials once it has given up on the old connection, so the
        # newest one wins. closing the old one wakes the thread still reading from it
        if to_close is not None:
            close_socket(to_close)
        return sock

    # forget a dead socket, unless it has already been replaced by a newer one
    def detach(self, sock):
        with self.sock_lock:
            if self.sock is sock:
                self.sock = None
        close_socket(sock)

    # runs the replicator until peer disconnects; will then try to reconnect or wait for the peer
    def run(self):
        while self.running:
            sock = self.connect() if self.initiator else self.wait_for_peer()
            if sock is None:
                return

            try:
                self.serve(sock)
            except Exception as e:
                print(f"Replicator.run({self.addr})", e)

            # print(f"Replicator for {self.addr} disconnected.")
            self.detach(sock)

    def serve(self, sock):
        while self.running:
            req = recv(sock)
            if not req:
                return

            # print(f"{self.addr[1]} -> {self.server.addr[1]}: {req!r}")

            match req['op']:
                case 'append_entries':
                    reply = self.server.on_append_entries(req)
                case 'install_snapshot':
                    reply = self.server.on_install_snapshot(req)
                case 'append_entries_response':
                    reply = self.server.on_append_entries_response(
                        self.addr, req)
                case 'request_vote':
                    reply = self.server.on_request_vote(self.addr, req)
                case 'vote':
                    reply = self.server.on_vote(self.addr, req)
                case _:
                    reply = None

            if reply is not None:
                self.send(sock, reply)

    def stop(self):
        with self.sock_cond:
            self.running = False
            self.sock_cond.notify_all()
            sock = self.sock

        if sock:
            close_socket(sock)

    def send_rpc(self, rpc):
        with self.sock_lock:
            sock = self.sock

        if sock is None:
            return

        try:
            self.send(sock, rpc)
        except OSError as e:
            print(f"send_rpc({self.addr})", e)

    def send(self, conn, rpc):
        # print(f"{self.server.addr[1]} -> {self.addr[1]}: {rpc}")
        with self.send_lock:
            send(conn, rpc)
