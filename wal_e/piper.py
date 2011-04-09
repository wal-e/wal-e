#!/usr/bin/env python
"""
Utilities for handling subprocesses.

Mostly necessary only because of http://bugs.python.org/issue1652.

"""

import copy
import signal
import subprocess

from subprocess import PIPE

def subprocess_setup(f=None):
    """
    SIGPIPE reset for subprocess workaround

    Python installs a SIGPIPE handler by default. This is usually not
    what non-Python subprocesses expect.

    Calls an optional "f" first in case other code wants a preexec_fn,
    then restores SIGPIPE to what most Unix processes expect.

    http://bugs.python.org/issue1652
    http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009-07-02-python-sigpipe.html

    """

    def wrapper(*args, **kwargs):
        if f is not None:
            f(*args, **kwargs)

        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    return wrapper


def popen_sp(*args, **kwargs):
    """
    Same as subprocess.Popen, but restores SIGPIPE

    This bug is documented (See subprocess_setup) but did not make it
    to standard library.  Could also be resolved by using the
    python-subprocess32 backport and using it appropriately (See
    'restore_signals' keyword argument to Popen)

    """

    kwargs['preexec_fn'] = subprocess_setup(kwargs.get('preexec_fn'))
    return subprocess.Popen(*args, **kwargs)


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
        raise ValueError, "pipe needs at least 2 processes"
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
