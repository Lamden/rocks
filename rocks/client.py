from zcomm import services
from rocks import constants
import zmq


def get(socket_id: services.SocketStruct,
        msg: list,
        ctx: zmq.Context,
        timeout=500,
        linger=2000,
        retries=10):

    if retries < 0:
        return None

    socket = ctx.socket(zmq.DEALER)

    socket.setsockopt(zmq.LINGER, linger)
    try:
        socket.connect(str(socket_id))

        print(f'sending {msg}')

        socket.send_multipart(msg)

        response = socket.recv_multipart()

        print(f'got {response}')

        socket.close()

        return response

    except Exception as e:
        socket.close()
        return get(socket_id, msg, ctx, timeout, linger, retries-1)


class RocksDBClient:
    def __init__(self, socket_id=constants.DEFAULT_SOCKET, ctx=zmq.Context()):
        self.socket = socket_id
        self.ctx = ctx

    def server_call(self, msg):
        res = get(self.socket, msg, self.ctx)
        return res

    def get(self, key):
        r = self.server_call([constants.GET_COMMAND, key])

        if r == b'':
            return None
        return r

    def set(self, key, value):
        print(key, value)
        return self.server_call([constants.SET_COMMAND, key, value])

    def delete(self, key):
        return self.server_call([constants.DEL_COMMAND, key])

    def seek(self, prefix):
        return self.server_call([constants.SEEK_ITER_COMMAND, prefix])

    def next(self):
        return self.server_call([constants.NEXT_COMMAND])

    def flush(self):
        return self.server_call([constants.FLUSH_COMMAND])
