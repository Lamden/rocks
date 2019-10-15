from zcomm import services
import rocksdb
from rocks import constants
import asyncio
import zmq

class MultipartRequestReplyService(services.RequestReplyService):
    async def serve(self):
        self.socket = self.ctx.socket(zmq.REP)
        self.socket.setsockopt(zmq.LINGER, self.linger)
        self.socket.bind(self.address)

        self.running = True

        while self.running:
            try:
                event = await self.socket.poll(timeout=self.poll_timeout, flags=zmq.POLLIN)
                if event:
                    msg = await self.socket.recv_multipart()
                    result = self.handle_msg(msg)

                    if result is None:
                        result = b''

                    await self.socket.poll(timeout=self.poll_timeout, flags=zmq.POLLOUT)
                    await self.socket.send(result)

            except zmq.error.ZMQError as e:
                self.socket = self.ctx.socket(zmq.REP)
                self.socket.setsockopt(zmq.LINGER, self.linger)
                self.socket.bind(self.address)

        self.socket.close()

    def handle_msg(self, msg):
        return msg

    def stop(self):
        self.running = False


class MultiPartAsyncInbox(services.AsyncInbox):
    async def serve(self):
        self.setup_socket()

        self.running = True

        while self.running:
            try:
                event = await self.socket.poll(timeout=self.poll_timeout, flags=zmq.POLLIN)
                if event:
                    message = await self.socket.recv_multipart()
                    asyncio.ensure_future(self.handle_msg(message[0], message[1:]))

            except zmq.error.ZMQError:
                self.socket.close()
                self.setup_socket()

        self.socket.close()

    async def return_msg(self, _id, msg):
        sent = False
        while not sent:
            try:
                await self.socket.send_multipart([_id, msg])
                sent = True
            except zmq.error.ZMQError:
                self.socket.close()
                self.setup_socket()


class RocksDBServer(MultipartRequestReplyService):
    def __init__(self, filename: str,
                 options: rocksdb.Options=rocksdb.Options(create_if_missing=True),
                 socket_id=services.SocketStruct(services.Protocols.TCP, '*', 11111),
                 ctx=constants.DEFAULT_ZMQ_CONTEXT,
                 linger=2000,
                 poll_timeout=50):

        self.db = rocksdb.DB(db_name=filename, opts=options)

        super().__init__(socket_id=socket_id,
                         ctx=ctx,
                         linger=linger,
                         poll_timeout=poll_timeout)

        self.prefix = None
        self.iterator = self.db.iterkeys()
        self.iterator.seek_to_first()

    def handle_msg(self, msg):
        command = msg[0]

        # BASIC COMMANDS #
        if command == constants.GET_COMMAND:
            v = self.get(msg[1])
            return v

        elif command == constants.SET_COMMAND:
            k, v = msg[1:]
            self.db.put(k, v)
            return constants.OK_RESPONSE

        elif command == constants.DEL_COMMAND:
            self.db.delete(msg[1])
            self.db.get(msg[1])
            return constants.OK_RESPONSE

        # # #

        # ITERATOR COMMANDS #
        elif command == constants.SEEK_ITER_COMMAND:
            p = msg[1]
            self.prefix = p
            self.iterator = self.db.iterkeys()
            self.iterator.seek(p)
            return constants.OK_RESPONSE

        elif command == constants.NEXT_COMMAND:
            try:
                k = next(self.iterator)
                return k
            except StopIteration:
                return constants.STOP_ITER_RESPONSE

        # # #

        elif command == constants.FLUSH_COMMAND:
            self.flush()
            return constants.OK_RESPONSE

        else:
            return constants.BAD_RESPONSE

    def get(self, key):
        v = self.db.get(key)
        if v is None:
            v = b''
        return v

    def flush(self):
        it = self.db.iterkeys()
        it.seek_to_first()

        for key in list(it):
            self.db.delete(key)
            self.db.get(key)