from zcomm import services
from rocks import constants
import zmq


class RocksServerOfflineError(Exception):
    pass


def get(socket_id: services.SocketStruct,
        msg: list,
        ctx: zmq.Context,
        timeout=100,
        linger=20):

    socket = ctx.socket(zmq.DEALER)

    socket.setsockopt(zmq.LINGER, linger)
    socket.connect(str(socket_id))
    socket.send_multipart(msg)
    event = socket.poll(timeout=timeout, flags=zmq.POLLIN)
    if event:
        response = socket.recv_multipart()

        socket.close()

        return response[0]
    raise RocksServerOfflineError


class RocksDBClient:
    def __init__(self, socket_id=constants.DEFAULT_SOCKET, ctx=zmq.Context()):
        self.socket = socket_id
        self.ctx = ctx

        self.pinged = False

    def server_call(self, msg):
        if not self.pinged:
            get(self.socket, [constants.PING_COMMAND], self.ctx)
            self.pinged = True

        res = get(self.socket, msg, self.ctx)
        return res

    def get(self, key):
        r = self.server_call([constants.GET_COMMAND, key])

        if r == b'':
            return None
        return r

    def set(self, key, value):
        return self.server_call([constants.SET_COMMAND, key, value])

    def delete(self, key):
        return self.server_call([constants.DEL_COMMAND, key])

    def seek(self, prefix):
        return self.server_call([constants.SEEK_ITER_COMMAND, prefix])

    def next(self):
        return self.server_call([constants.NEXT_COMMAND])

    def flush(self):
        return self.server_call([constants.FLUSH_COMMAND])

    def ping(self):
        self.server_call([constants.PING_COMMAND])