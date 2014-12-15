import gevent
import pytest


@pytest.fixture(autouse=True)
def fast_wait(monkeypatch):
    """Stub out gevent calls that take timeouts to wait briefly.

    In production one may want to wait a bit having no work to do to
    avoid spinning, but during testing this adds quite a bit of time.

    """
    old_sleep = gevent.sleep
    old_joinall = gevent.joinall
    old_killall = gevent.killall

    def fast_wait(tm):
        # Ignore time passed and just yield.
        return old_sleep(0.1)

    def fast_joinall(*args, **kwargs):
        if 'timeout' in kwargs:
            kwargs['timeout'] = 0.1
        return old_joinall(*args, **kwargs)

    def fast_killall(*args, **kwargs):
        if 'timeout' in kwargs:
            kwargs['timeout'] = 0.1
        return old_killall(*args, **kwargs)

    monkeypatch.setattr(gevent, 'sleep', fast_wait)
    monkeypatch.setattr(gevent, 'joinall', fast_joinall)
    monkeypatch.setattr(gevent, 'killall', fast_killall)
