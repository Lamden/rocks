from zcomm import services
import rocksdb
from rocks import constants
import asyncio
import zmq


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


class RocksDBServer(MultiPartAsyncInbox):
    def __init__(self, filename: str,
                 options: rocksdb.Options=rocksdb.Options(create_if_missing=True),
                 socket_id=constants.DEFAULT_SOCKET,
                 ctx=constants.DEFAULT_ZMQ_CONTEXT,
                 linger=2000,
                 poll_timeout=50):

        self.db = rocksdb.DB(db_name=filename, opts=options)

        super().__init__(socket_id=socket_id,
                         ctx=ctx,
                         linger=linger,
                         poll_timeout=poll_timeout)

        self.current_iterator = self.db.iterkeys()
        self.current_iterator.seek_to_first()

    async def handle_msg(self, _id, msg):
        command = msg[0]
        print(f'got {msg}')

        # BASIC COMMANDS #
        if command == constants.GET_COMMAND:
            v = self.get(msg[1])
            await super().return_msg(_id, v)

        elif command == constants.SET_COMMAND:
            k, v = msg[1:]
            self.db.put(k, v)
            await super().return_msg(_id, constants.OK_RESPONSE)

        elif command == constants.DEL_COMMAND:
            self.db.delete(msg[1])
            await super().return_msg(_id, constants.OK_RESPONSE)

        # # #

        # ITERATOR COMMANDS #
        elif command == constants.SEEK_ITER_COMMAND:
            p = msg[1]
            self.current_iterator.seek(p)
            await super().return_msg(_id, constants.OK_RESPONSE)

        elif command == constants.NEXT_COMMAND:
            try:
                k = next(self.current_iterator)
                await super().return_msg(_id, k)
            except StopIteration:
                await super().return_msg(_id, constants.STOP_ITER_RESPONSE)

        # # #

        elif command == constants.FLUSH_COMMAND:
            print('flush!')
            self.flush()
            await super().return_msg(_id, constants.OK_RESPONSE)

        else:
            await super().return_msg(_id, constants.BAD_RESPONSE)

    def get(self, key):
        v = self.db.get(key)
        if v is None:
            v = b''
        return v

    def flush(self):
        it = self.db.iterkeys()
        it.seek_to_first()

        for key in list(it):
            print(key)
            self.db.delete(key)
