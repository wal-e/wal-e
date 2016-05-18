# Detailed handling of pipe buffering
#
# This module attempts to reduce the number of system calls to
# non-blocking pipes.  It does this by careful control over buffering
# and nonblocking pipe operations.

import collections
import errno
import fcntl
import gevent
import gevent.socket
import os

PIPE_BUF_BYTES = None
OS_PIPE_SZ = None


def _configure_buffer_sizes():
    """Set up module globals controlling buffer sizes"""
    global PIPE_BUF_BYTES
    global OS_PIPE_SZ

    PIPE_BUF_BYTES = 65536
    OS_PIPE_SZ = None

    # Teach the 'fcntl' module about 'F_SETPIPE_SZ', which is a Linux-ism,
    # but a good one that can drastically reduce the number of syscalls
    # when dealing with high-throughput pipes.
    if not hasattr(fcntl, 'F_SETPIPE_SZ'):
        import platform

        if platform.system() == 'Linux':
            fcntl.F_SETPIPE_SZ = 1031

    # If Linux procfs (or something that looks like it) exposes its
    # maximum F_SETPIPE_SZ, adjust the default buffer sizes.
    try:
        with open('/proc/sys/fs/pipe-max-size', 'r') as f:
            # Figure out OS pipe size, but in case it is unusually large
            # or small restrain it to sensible values.
            OS_PIPE_SZ = min(int(f.read()), 1024 * 1024)
            PIPE_BUF_BYTES = max(OS_PIPE_SZ, PIPE_BUF_BYTES)
    except:
        pass


_configure_buffer_sizes()


def set_buf_size(fd):
    """Set up os pipe buffer size, if applicable"""
    if OS_PIPE_SZ and hasattr(fcntl, 'F_SETPIPE_SZ'):
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, OS_PIPE_SZ)


def _setup_fd(fd):
    """Common set-up code for initializing a (pipe) file descriptor"""

    # Make the file nonblocking (but don't lose its previous flags)
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    set_buf_size(fd)


class ByteDeque(object):
    """Data structure for delayed defragmentation of submitted bytes"""
    def __init__(self):
        self._dq = collections.deque()
        self.byteSz = 0

    def add(self, b):
        self._dq.append(b)
        self.byteSz += len(b)

    def get(self, n):
        assert n <= self.byteSz, 'caller responsibility to ensure enough bytes'

        if (n == self.byteSz and len(self._dq) == 1 and
            isinstance(self._dq[0], bytes)):
            # Fast-path: if the deque has one element of the right
            # size *and* type (fragmentation can result in 'buffer'
            # objects pushed back on the deque) return it and avoid a
            # copy.
            self.byteSz = 0
            return self._dq.popleft()

        out = bytearray(n)
        remaining = n
        while remaining > 0:
            part = memoryview(self._dq.popleft())
            delta = remaining - len(part)
            offset = n - remaining

            if delta == 0:
                out[offset:] = part
                remaining = 0
            elif delta > 0:
                out[offset:] = part
                remaining = delta
            elif delta < 0:
                cleave = len(part) + delta
                out[offset:] = part[:cleave]
                self._dq.appendleft(part[cleave:])
                remaining = 0
            else:
                assert False

        self.byteSz -= n

        assert len(out) == n
        return bytes(out)

    def get_all(self):
        return self.get(self.byteSz)


class NonBlockBufferedReader(object):
    """A buffered pipe reader that adheres to the Python file protocol"""

    def __init__(self, fp):
        self._fp = fp
        self._fd = fp.fileno()
        self._bd = ByteDeque()
        self.got_eof = False

        _setup_fd(self._fd)

    def _read_chunk(self, sz):
        chunk = None
        try:
            chunk = os.read(self._fd, sz)
            self._bd.add(chunk)
        except EnvironmentError as e:
            if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                assert chunk is None
                gevent.socket.wait_read(self._fd)
            else:
                raise

        self.got_eof = (chunk == b'')

    def read(self, size=None):
        # Handle case of "read all".
        if size is None:

            # Read everything.
            while not self.got_eof:
                self._read_chunk(PIPE_BUF_BYTES)

            # Defragment and return the contents.
            return self._bd.get_all()
        elif size > 0:
            while True:
                if self._bd.byteSz >= size:
                    # Enough bytes already buffered.
                    return self._bd.get(size)
                elif self._bd.byteSz <= size and self.got_eof:
                    # Not enough bytes buffered, but the stream is
                    # over, so return what has been gotten.
                    return self._bd.get_all()
                else:
                    # Not enough bytes buffered and stream is still
                    # open: read more bytes.
                    assert not self.got_eof

                    if size == PIPE_BUF_BYTES:
                        # Many PIPE_BUF_BYTES reads are done in WAL-E
                        # to move around data in bulk.
                        #
                        # Use that as a hint that another
                        # PIPE_BUF_BYTES-sized .read() will occur
                        # soon.  The goal is to trigger the
                        # less-copy-intensive fast-path in the
                        # ByteDeque frequently.
                        #
                        # To do that, attempt to align the read
                        # syscalls to the kernel with Python reads,
                        # even if that means issuing a shorter read
                        # than usual.
                        to_read = PIPE_BUF_BYTES - self._bd.byteSz
                        self._read_chunk(to_read)
                    else:
                        self._read_chunk(PIPE_BUF_BYTES)
        else:
            assert False

    def close(self):
        if self.closed:
            return

        # Delegate close to self._fp -- it'll try to do it during its
        # destructor which is why delegation is used rather than
        # manipulation of the fd directly.
        self._fp.close()
        try:
            del self._fp
        except AttributeError:
            pass

        try:
            del self._bd
        except AttributeError:
            pass

        # Invalidate state and flag total completion of the close
        # operation.
        self._fd = -1

    def fileno(self):
        return self._fd

    @property
    def closed(self):
        return self._fd == -1


class NonBlockBufferedWriter(object):
    """A buffered pipe writer that adheres to the Python file protocol"""

    def __init__(self, fp):
        self._fp = fp
        self._fd = fp.fileno()
        self._bd = ByteDeque()

        _setup_fd(self._fd)

    def _partial_flush(self, max_retain):
        byts = self._bd.get_all()
        cursor = memoryview(byts)

        flushed = False
        while len(cursor) > max_retain:
            try:
                n = os.write(self._fd, cursor)
                flushed = True
                cursor = memoryview(cursor)[n:]
            except EnvironmentError as e:
                if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                    gevent.socket.wait_write(self._fd)
                else:
                    raise

        assert self._bd.byteSz == 0
        if len(cursor) > 0:
            self._bd.add(cursor)

        return flushed

    def write(self, data):
        self._bd.add(data)

        flushed = True
        while flushed and self._bd.byteSz > PIPE_BUF_BYTES:
            # Flush down to a small amount of buffered bytes as to
            # avoid memory-copy intensive defragmentations.
            #
            # The tradeoff being made here is the price of a syscall
            # (where larger buffers are better) vs. the price of
            # copying some memory.
            flushed = self._partial_flush(65535)

    def flush(self):
        while self._bd.byteSz > 0:
            self._partial_flush(0)

    def fileno(self):
        return self._fd

    def close(self):
        if self.closed:
            return

        # Delegate close to self._fp -- it'll try to do it during its
        # destructor which is why delegation is used rather than
        # manipulation of the fd directly.
        self._fp.close()
        try:
            del self._fp
        except AttributeError:
            pass

        try:
            del self._bd
        except AttributeError:
            pass

        # Invalidate state and flag total completion of the close
        # operation.
        self._fd = -1

    @property
    def closed(self):
        return self._fd == -1
