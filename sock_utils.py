import json
import threading


def recv_exactly(conn, nbytes):
    try:
        buf = b''

        while nbytes > 0:
            chunk = conn.recv(nbytes)
            if not chunk:
                return None

            buf += chunk
            nbytes -= len(chunk)

        return buf
    except Exception as e:
        raise IOError("recv_exactly()", e)


def recv_size(conn):
    try:
        sz_buf = recv_exactly(conn, 4)
        if not sz_buf:
            return None

        return int.from_bytes(sz_buf, byteorder='big')

    except Exception as e:
        print("recv_size()", e)


def recv(conn):
    try:
        msg_sz = recv_size(conn)
        if msg_sz is None:
            return None

        msg = recv_exactly(conn, msg_sz)
        if msg is None:
            return None

        # print(f"recvd: {msg.decode()!r}")

        return json.loads(msg.decode())
    except Exception as e:
        print("recv()", e)


def send(conn, msg):
    data = json.dumps(msg).encode()
    frame = len(data).to_bytes(4, 'big') + data
    conn.sendall(frame)
