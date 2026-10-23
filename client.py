import socket
import time
from sock_utils import send, recv
from messages import error


RETRY_DELAY = 0.5
MAX_ATTEMPTS = 20


class Client:
    def __init__(self, addr):
        # the server we start from, and fall back to if the one we're talking to goes away
        self.home = addr
        self.s = None
        self.addr = None

        self.connect(addr)

    def connect(self, addr):
        self.close()

        s = socket.create_connection(addr)
        send(s, {
            'op': 'authenticate',
            'role': 'client'
        })
        self.s, self.addr = s, addr

    def close(self):
        if self.s:
            self.s.close()
        self.s = None

    # send cmd to the leader, following redirects; returns the server's reply, which has a
    # 'status' of 'ok' (with a 'result') or 'error' (with a 'msg')
    def request(self, cmd):
        for _ in range(MAX_ATTEMPTS):
            try:
                if self.s is None:
                    self.connect(self.home)

                send(self.s, cmd)
                reply = recv(self.s)
            except OSError:
                reply = None

            if reply is None:
                # the server went away; start over from home. set and delete are idempotent,
                # so resending one that may already have committed is harmless
                self.close()
                time.sleep(RETRY_DELAY)
                continue

            if reply['status'] != 'redirect':
                return reply

            if reply['leader'] is None:
                # an election is underway; ask again once it has had a chance to finish
                time.sleep(RETRY_DELAY)
                continue

            try:
                self.connect(tuple(reply['leader']))
            except OSError:
                self.close()
                time.sleep(RETRY_DELAY)

        return error('could not reach the leader')

    # turn "set k v", "get k" or "delete k" into a command; None if it's malformed
    @staticmethod
    def parse(line):
        data = line.split(' ')
        if len(data) < 2:
            return None

        cmd = {
            'op': data[0],
            'key': data[1],
        }

        if len(data) > 2:
            if cmd['op'] == 'get':
                return None

            cmd['val'] = ' '.join(data[2:])

        return cmd

    @staticmethod
    def show(reply):
        if reply['status'] == 'ok':
            print(reply['result'])
        else:
            print(f"error: {reply['msg']}")

    def interact(self):
        try:
            while True:
                cmd = self.parse(input("client: "))
                if cmd is None:
                    print("unknown command")
                    continue

                self.show(self.request(cmd))
        finally:
            self.close()

    def test(self, cmds):
        for c in cmds:
            cmd = self.parse(c)
            if cmd is None:
                print("unknown command")
                continue

            self.show(self.request(cmd))


if __name__ == '__main__':
    client = Client(('localhost', 10000))
    client.interact()
