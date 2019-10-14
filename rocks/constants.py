from zcomm import services
import zmq.asyncio
import os
GET_COMMAND = b'\x00'
SET_COMMAND = b'\x01'
DEL_COMMAND = b'\x02'
OK_RESPONSE = b'\x03'
BAD_RESPONSE = b'\x04'

DEFAULT_ZMQ_CONTEXT = zmq.asyncio.Context()
DEFAULT_SOCKET = services._socket('tcp://127.0.0.1:11111')

DEFAULT_DIRECTORY = os.path.expanduser('~/rocks')