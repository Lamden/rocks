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
    def setUp(self):
        self.c = RocksDBClient()
        self.c.flush()

    def tearDown(self):
        self.c.flush()

    def test_flush_works(self):

        self.c.set(b'test', b'123')
        self.c.set(b'x', b'123')
        self.c.set(b'h', b'123')
        self.c.set(b'1', b'123')
        self.c.set(b'3', b'123')
        self.c.set(b'5', b'123')
        self.c.set(b'g', b'123')
        self.c.set(b'v', b'123')
        self.c.set(b's', b'123')
        self.c.set(b'z', b'123')

        self.assertEqual(self.c.get(b'test'), b'123')

        self.c.flush()

        self.assertIsNone(self.c.get(b'test'))
        self.assertIsNone(self.c.get(b'x'))
        self.assertIsNone(self.c.get(b'h'))
        self.assertIsNone(self.c.get(b'1'))
        self.assertIsNone(self.c.get(b'3'))
        self.assertIsNone(self.c.get(b'5'))
        self.assertIsNone(self.c.get(b'g'))
        self.assertIsNone(self.c.get(b'v'))
        self.assertIsNone(self.c.get(b's'))
        self.assertIsNone(self.c.get(b'z'))

    def test_set_key_succeeds(self):
        r = self.c.set(b'test', b'123')
        self.assertEqual(r, constants.OK_RESPONSE)

    def test_get_succeeds(self):
        r = self.c.set(b'test', b'123')
        self.assertEqual(r, constants.OK_RESPONSE)

        r = self.c.get(b'test')
        self.assertEqual(r, b'123')

    def test_delete_succeeds(self):
        r = self.c.set(b'test', b'123')
        self.assertEqual(r, constants.OK_RESPONSE)

        r = self.c.delete(b'test')
        self.assertEqual(r, constants.OK_RESPONSE)

        r = self.c.get(b'test')
        self.assertEqual(r, None)

    def test_seek_succeeds(self):
        r = self.c.seek(b'123')
        self.assertEqual(r, constants.OK_RESPONSE)

    def test_seek_and_next_succeeds_once(self):
        self.c.set(b'test1', b'123')
        self.c.set(b'test2', b'12')
        self.c.set(b'test3', b'1')
        self.c.set(b'test4', b'124')
        self.c.set(b'test5', b'125')

        self.c.seek(b'test1')

        self.assertEqual(self.c.next(), b'test1')
        self.assertEqual(self.c.next(), b'test2')
        self.assertEqual(self.c.next(), b'test3')
        self.assertEqual(self.c.next(), b'test4')
        self.assertEqual(self.c.next(), b'test5')


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