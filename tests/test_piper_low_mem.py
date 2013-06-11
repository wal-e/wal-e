import errno
import os

import gevent
import pytest

from wal_e import piper
from wal_e import subprocess


def invoke_program():
    with open(os.devnull, 'w') as devnull:
        piper.popen_sp(['python', '--version'],
                       stdout=devnull, stderr=devnull)


def test_normal():
    invoke_program()


class OomTimes(object):
    def __init__(self, real, n):
        self.real = real
        self.n = n

    def __call__(self, *args, **kwargs):
        if self.n == 0:
            self.real(*args, **kwargs)
        else:
            self.n -= 1
            e = OSError('faked oom')
            e.errno = errno.ENOMEM
            raise e


def pytest_generate_tests(metafunc):
    if "oomtimes" in metafunc.funcargnames:
        # Test OOM being delivered a varying number of times.
        scenarios = [OomTimes(subprocess.Popen, n) for n in [0, 1, 2, 10]]
        metafunc.parametrize("oomtimes", scenarios)


def test_low_mem(oomtimes, gevent_fastsleep, monkeypatch):
    monkeypatch.setattr(subprocess, 'Popen', oomtimes)
    invoke_program()


def test_advanced_shim(oomtimes, monkeypatch):
    monkeypatch.setattr(subprocess, 'Popen', oomtimes)

    old_n = oomtimes.n

    def reset():
        oomtimes.n = old_n

    def invoke(max_tries):
        with open(os.devnull, 'w') as devnull:
            popen = piper.PopenShim(sleep_time=0, max_tries=max_tries)
            popen(['python', '--version'],
                  stdout=devnull, stderr=devnull)

    if oomtimes.n >= 1:
        with pytest.raises(OSError) as e:
            invoke(oomtimes.n - 1)

        assert e.value.errno == errno.ENOMEM
    else:
        invoke(oomtimes.n - 1)

    reset()

    invoke(oomtimes.n)
    reset()

    invoke(oomtimes.n + 1)
    reset()


@pytest.fixture()
def gevent_fastsleep(monkeypatch):
    """Stub out gevent.sleep to only yield briefly.

    In production one may want to wait a bit having no work to do to
    avoid spinning, but during testing this adds quite a bit of time.
    """
    old_sleep = gevent.sleep

    def fast_sleep(tm):
        # Ignore time passed and just yield.
        old_sleep(0)

    monkeypatch.setattr(gevent, 'sleep', fast_sleep)


def test_fast_sleep(gevent_fastsleep):
    """Annoy someone who causes fast-sleep test patching to regress.

    Someone could break the test-only monkey-patching of gevent.sleep
    without noticing and costing quite a bit of aggravation aggregated
    over time waiting in tests, added bit by bit.

    To avoid that, add this incredibly huge/annoying delay that can
    only be avoided by monkey-patch to catch the regression.
    """
    gevent.sleep(300)
