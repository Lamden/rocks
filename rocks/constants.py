from zcomm import services
import zmq.asyncio
import os

# BASIC COMMANDS #
GET_COMMAND = b'\x00'
SET_COMMAND = b'\x01'
DEL_COMMAND = b'\x02'

# BASIC RESPONSES #
OK_RESPONSE = b'\x03'
BAD_RESPONSE = b'\x04'

# # #

# ITERATOR COMMANDS #
SEEK_ITER_COMMAND = b'\x05'
NEXT_COMMAND = b'\x08'

# ITERATOR RESPONSES #
STOP_ITER_RESPONSE = b'\x09'

# # #

FLUSH_COMMAND = b'\x0a'

DEFAULT_ZMQ_CONTEXT = zmq.asyncio.Context()
DEFAULT_SOCKET = services._socket('tcp://127.0.0.1:11111')

DEFAULT_DIRECTORY = os.path.expanduser('~/rocks')