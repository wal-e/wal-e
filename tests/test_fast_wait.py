import gevent

from fast_wait import fast_wait

assert fast_wait


def nonterm_greenlet():
    while True:
        gevent.sleep(300)


def test_fast_wait():
    """Annoy someone who causes fast-sleep test patching to regress.

    Someone could break the test-only monkey-patching of gevent.sleep
    without noticing and costing quite a bit of aggravation aggregated
    over time waiting in tests, added bit by bit.

    To avoid that, add this incredibly huge/annoying delay that can
    only be avoided by monkey-patch to catch the regression.
    """
    gevent.sleep(300)
    g = gevent.spawn(nonterm_greenlet)
    gevent.joinall([g], timeout=300)
    gevent.killall([g], timeout=300)
