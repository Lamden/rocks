from zcomm import services
from rocks import constants
import asyncio
import zmq


async def get(socket_id: services.SocketStruct,
              msg: list,
              ctx: zmq.Context,
              timeout=500,
              linger=2000,
              retries=10,
              dealer=True):

    if retries < 0:
        return None

    if dealer:
        socket = ctx.socket(zmq.DEALER)
    else:
        socket = ctx.socket(zmq.REQ)

    socket.setsockopt(zmq.LINGER, linger)
    try:
        # Allow passing an existing socket to save time on initializing a _new one and waiting for connection.
        socket.connect(str(socket_id))

        await socket.send_multipart(msg)

        event = await socket.poll(timeout=timeout, flags=zmq.POLLIN)
        if event:
            response = await socket.recv()

            socket.close()

            return response
        else:
            socket.close()
            return None

    except Exception as e:
        socket.close()
        return await get(socket_id, msg, ctx, timeout, linger, retries-1)


class RocksDBClient:
    def __init__(self, socket_id=constants.DEFAULT_SOCKET, ctx=constants.DEFAULT_ZMQ_CONTEXT):
        self.socket = socket_id
        self.ctx = ctx

    def server_call(self, msg):
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(get(self.socket, msg, self.ctx))
        return res

    def get(self, key):
        return self.server_call([constants.GET_COMMAND, key])

    def set(self, key, value):
        return self.server_call([constants.SET_COMMAND, key, value])

    def delete(self, key):
        return self.server_call([constants.DEL_COMMAND, key])

