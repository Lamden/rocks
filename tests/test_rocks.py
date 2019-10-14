from unittest import TestCase
import asyncio
import zmq.asyncio
from rocks.server import MultiPartAsyncInbox
from rocks.client import RocksDBClient
from rocks import constants
from zcomm import services


async def stop_server(s, timeout):
    await asyncio.sleep(timeout)
    s.stop()


class TestRocksServer(TestCase):

    def test_set_key_succeeds(self):
        c = RocksDBClient()

        r = c.set(b'test', b'123')

        self.assertEqual(r, constants.OK_RESPONSE)

    def test_get_succeeds(self):
        c = RocksDBClient()

        r = c.get(b'test')

        self.assertEqual(r, b'123')

    def test_delete_succeeds(self):
        c = RocksDBClient()

        r = c.delete(b'test')

        self.assertEqual(r, constants.OK_RESPONSE)

        r = c.get(b'test')

        self.assertEqual(r, b'')


class TestMultipartServer(TestCase):
    def setUp(self):
        self.ctx = zmq.asyncio.Context()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.ctx.destroy()
        self.loop.close()

    def test_sending_message_returns_it(self):
        m = MultiPartAsyncInbox(services._socket('tcp://127.0.0.1:10000'), self.ctx, linger=500, poll_timeout=500)

        async def get(msg):
            socket = self.ctx.socket(zmq.DEALER)
            socket.connect('tcp://127.0.0.1:10000')

            await socket.send(msg)

            res = await socket.recv()

            return res

        tasks = asyncio.gather(
            m.serve(),
            get(b'howdy'),
            stop_server(m, 0.2),
        )

        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(tasks)

        self.assertEqual(res[1], b'howdy')