#!/usr/bin/env python
"""
Utilities for handling subprocesses.

Mostly necessary only because of http://bugs.python.org/issue1652.

"""
import copy
import errno
import fcntl
import gevent
import gevent.socket
import os
import signal
from cStringIO import StringIO

from wal_e import subprocess
from wal_e.subprocess import PIPE

# This is not used in this module, but is imported by dependent
# modules, so do this to quiet pyflakes.
assert PIPE

# Determine the maximum number of bytes that can be written atomically
# to a pipe
PIPE_BUF_BYTES = os.pathconf('.', os.pathconf_names['PC_PIPE_BUF'])


class NonBlockPipeFileWrap(object):
    def __init__(self, fp):
        # Make the file nonblocking (but don't lose its previous flags)
        flags = fcntl.fcntl(fp, fcntl.F_GETFL)
        fcntl.fcntl(fp, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        self._fp = fp

    def read(self, size=None):
        # Some adaptation from gevent's examples/processes.py
        accum = StringIO()
        fd = self._fp.fileno()

        while size is None or accum.tell() < size:
            if size is None:
                max_read = PIPE_BUF_BYTES
            else:
                max_read = min(PIPE_BUF_BYTES, size - accum.tell())

            # Put a retry around this one write syscall to take care
            # of EAGAIN.
            succeeded = False
            while not succeeded:
                chunk = None

                try:
                    chunk = os.read(fd, max_read)
                    succeeded = True
                except EnvironmentError, ex:
                    if ex.errno == errno.EAGAIN:
                        assert chunk is None
                        gevent.socket.wait_read(fd)
                    else:
                        raise

            # 'chunk' should be assigned at least once if no exception
            # was raised, even if an immediate EOF is found.
            assert chunk is not None

            # End of the stream: leave the loop.
            if chunk == '':
                break

            accum.write(chunk)

        return accum.getvalue()

    def write(self, data):
        # Some adaptation from gevent's examples/processes.py
        buf = StringIO(data)
        bytes_total = len(data)
        bytes_written = 0
        fd = self._fp.fileno()

        while bytes_written < bytes_total:
            chunk = buf.read(PIPE_BUF_BYTES)

            # Put a retry around this one write syscall to take care
            # of EAGAIN.
            succeeded = False
            while not succeeded:
                try:
                    bytes_written += os.write(fd, chunk)
                    succeeded = True
                except EnvironmentError, ex:
                    if ex.errno == errno.EAGAIN:
                        gevent.socket.wait_write(fd)
                    else:
                        raise

    def fileno(self):
        return self._fp.fileno()

    def close(self):
        return self._fp.close()

    def flush(self):
        return self._fp.flush()

    @property
    def closed(self):
        return self._fp.closed


def subprocess_setup(f=None):
    """
    SIGPIPE reset for subprocess workaround

    Python installs a SIGPIPE handler by default. This is usually not
    what non-Python subprocesses expect.

    Calls an optional "f" first in case other code wants a preexec_fn,
    then restores SIGPIPE to what most Unix processes expect.

    http://bugs.python.org/issue1652

    """

    def wrapper(*args, **kwargs):
        if f is not None:
            f(*args, **kwargs)

        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    return wrapper


class PopenShim(object):
    def __init__(self, sleep_time=1, max_tries=None):
        self.sleep_time = sleep_time
        self.max_tries = max_tries

    def __call__(self, *args, **kwargs):
        """
        Same as subprocess.Popen, but restores SIGPIPE

        This bug is documented (See subprocess_setup) but did not make
        it to standard library.  Could also be resolved by using the
        python-subprocess32 backport and using it appropriately (See
        'restore_signals' keyword argument to Popen)
        """

        kwargs['preexec_fn'] = subprocess_setup(kwargs.get('preexec_fn'))

        # Call Popen, but be persistent in the face of ENOMEM.
        #
        # The utility of this is that on systems with overcommit off,
        # the momentary spike in committed virtual memory from fork()
        # can be large, but is cleared soon thereafter because
        # 'subprocess' uses an 'exec' system call.  Without retrying,
        # the the backup process would lose all its progress
        # immediately with no recourse, which is undesirable.
        #
        # Because the ENOMEM error happens on fork() before any
        # meaningful work can be done, one thinks this retry would be
        # safe, and without side effects.  Because fork is being
        # called through 'subprocess' and not directly here, this
        # program has to rely on the semantics of the exceptions
        # raised from 'subprocess' to avoid retries in unrelated
        # scenarios, which could be dangerous.
        tries = 0

        while True:
            try:
                proc = subprocess.Popen(*args, **kwargs)
            except OSError, e:
                if e.errno == errno.ENOMEM:
                    should_retry = (self.max_tries is not None and
                                    tries >= self.max_tries)

                    if should_retry:
                        raise

                    gevent.sleep(self.sleep_time)
                    tries += 1
                    continue

                raise
            else:
                break

        return proc

popen_sp = PopenShim()


def popen_nonblock(*args, **kwargs):
    """
    Create a process in the same way as popen_sp, but patch the file
    descriptors so they can can be accessed from Python/gevent
    in a non-blocking manner.
    """

    proc = popen_sp(*args, **kwargs)

    # Patch up the process object to use non-blocking I/O that yields
    # to the gevent hub.
    for fp_symbol in ['stdin', 'stdout', 'stderr']:
        value = getattr(proc, fp_symbol)

        if value is not None:
            # this branch is only taken if a descriptor is sent in
            # with 'PIPE' mode.
            setattr(proc, fp_symbol, NonBlockPipeFileWrap(value))

    return proc


def pipe(*args):
    """
    Takes as parameters several dicts, each with the same
    parameters passed to popen.

    Runs the various processes in a pipeline, connecting
    the stdout of every process except the last with the
    stdin of the next process.

    Adapted from http://www.enricozini.org/2009/debian/python-pipes/

    """
    if len(args) < 2:
        raise ValueError("pipe needs at least 2 processes")

    # Set stdout=PIPE in every subprocess except the last
    for i in args[:-1]:
        i["stdout"] = subprocess.PIPE

    # Runs all subprocesses connecting stdins and stdouts to create the
    # pipeline. Closes stdouts to avoid deadlocks.
    popens = [popen_sp(**args[0])]
    for i in range(1, len(args)):
        args[i]["stdin"] = popens[i - 1].stdout
        popens.append(popen_sp(**args[i]))
        popens[i - 1].stdout.close()

    # Returns the array of subprocesses just created
    return popens


def pipe_wait(popens):
    """
    Given an array of Popen objects returned by the
    pipe method, wait for all processes to terminate
    and return the array with their return values.

    Taken from http://www.enricozini.org/2009/debian/python-pipes/

    """
    # Avoid mutating the passed copy
    popens = copy.copy(popens)
    results = [0] * len(popens)
    while popens:
        last = popens.pop(-1)
        results[len(popens)] = last.wait()
    return results
