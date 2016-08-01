import gevent

from gevent import queue
from wal_e import channel


def test_channel_shim():
    v = tuple(int(x) for x in gevent.__version__.split('.'))
    print('Version info:', gevent.__version__, v)

    if v >= (0, 13) and v < (1, 0):
        assert isinstance(channel.Channel(), queue.Queue)
    elif v >= (1, 0):
        assert isinstance(channel.Channel(), queue.Channel)
    else:
        assert False, 'Unexpected version ' + gevent.__version__
