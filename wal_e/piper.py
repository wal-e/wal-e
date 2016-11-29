#!/usr/bin/env python
"""
Utilities for handling subprocesses.

Mostly necessary only because of http://bugs.python.org/issue1652.

"""
import copy
import errno
import gevent
import gevent.socket
import subprocess

from subprocess import PIPE
from wal_e import pipebuf

# This is not used in this module, but is imported by dependent
# modules, so do this to quiet pyflakes.
assert PIPE


class PopenShim(object):
    def __init__(self, sleep_time=1, max_tries=None):
        self.sleep_time = sleep_time
        self.max_tries = max_tries

    def __call__(self, *args, **kwargs):
        """Call Popen, but be persistent in the face of ENOMEM.

        The utility of this is that on systems with overcommit off,
        the momentary spike in committed virtual memory from fork()
        can be large, but is cleared soon thereafter because
        'subprocess' uses an 'exec' system call.  Without retrying,
        the the backup process would lose all its progress
        immediately with no recourse, which is undesirable.

        Because the ENOMEM error happens on fork() before any
        meaningful work can be done, one thinks this retry would be
        safe, and without side effects.  Because fork is being
        called through 'subprocess' and not directly here, this
        program has to rely on the semantics of the exceptions
        raised from 'subprocess' to avoid retries in unrelated
        scenarios, which could be dangerous.

        """
        tries = 0

        while True:
            try:
                proc = subprocess.Popen(*args, **kwargs)
            except OSError as e:
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
    descriptors so they can be accessed from Python/gevent
    in a non-blocking manner.
    """

    proc = popen_sp(*args, **kwargs)

    if proc.stdin:
        proc.stdin = pipebuf.NonBlockBufferedWriter(proc.stdin)

    if proc.stdout:
        proc.stdout = pipebuf.NonBlockBufferedReader(proc.stdout)

    if proc.stderr:
        proc.stderr = pipebuf.NonBlockBufferedReader(proc.stderr)

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
