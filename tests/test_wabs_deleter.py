import gevent
import pytest
from collections import namedtuple

try:
    # New module location sometime after Azure SDK v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.txt
    from azure.storage.blob import BlobService
except ImportError:
    from azure.storage import BlobService

from fast_wait import fast_wait
from gevent import coros

from wal_e import exception
from wal_e.worker.wabs import wabs_deleter

assert fast_wait

B = namedtuple('Blob', ['name'])


class ContainerDeleteKeysCollector(object):
    """A callable to stand-in for bucket.delete_keys

    Used to test that given keys are bulk-deleted.

    Also can inject an exception.
    """
    def __init__(self):
        self.deleted_keys = []
        self.aborted_keys = []
        self.exc = None

        # Protect exc, since some paths test it and then use it, which
        # can run afoul race conditions.
        self._exc_protect = coros.RLock()

    def inject(self, exc):
        self._exc_protect.acquire()
        self.exc = exc
        self._exc_protect.release()

    def __call__(self, container, key):
        self._exc_protect.acquire()

        try:
            if self.exc:
                self.aborted_keys.append(key)

                # Prevent starvation/livelock with a polling process
                # by yielding.
                gevent.sleep(0.1)

                raise self.exc
        finally:
            self._exc_protect.release()

        self.deleted_keys.append(key)


@pytest.fixture
def collect(monkeypatch):
    """Instead of performing bulk delete, collect key names deleted.

    This is to test invariants, as to ensure deleted keys are passed
    to boto properly.
    """

    collect = ContainerDeleteKeysCollector()
    monkeypatch.setattr(BlobService, 'delete_blob', collect)

    return collect


def test_fast_wait():
    """Annoy someone who causes fast-sleep test patching to regress.

    Someone could break the test-only monkey-patching of gevent.sleep
    without noticing and costing quite a bit of aggravation aggregated
    over time waiting in tests, added bit by bit.

    To avoid that, add this incredibly huge/annoying delay that can
    only be avoided by monkey-patch to catch the regression.
    """
    gevent.sleep(300)


def test_construction():
    """The constructor basically works."""
    wabs_deleter.Deleter('test', 'ing')


def test_close_error():
    """Ensure that attempts to use a closed Deleter results in an error."""

    d = wabs_deleter.Deleter(BlobService('test', 'ing'), 'test-container')
    d.close()

    with pytest.raises(exception.UserCritical):
        d.delete('no value should work')


def test_processes_one_deletion(collect):
    key_name = 'test-key-name'
    b = B(name=key_name)

    d = wabs_deleter.Deleter(BlobService('test', 'ing'), 'test-container')
    d.delete(b)
    d.close()

    assert collect.deleted_keys == [key_name]


def test_processes_many_deletions(collect):
    # Generate a target list of keys in a stable order
    target = sorted(['test-key-' + str(x) for x in range(20001)])

    # Construct boto S3 Keys from the generated names and delete them
    # all.
    blobs = [B(name=key_name) for key_name in target]
    d = wabs_deleter.Deleter(BlobService('test', 'ing'), 'test-container')

    for b in blobs:
        d.delete(b)

    d.close()

    # Sort the deleted key names to obtain another stable order and
    # then ensure that everything was passed for deletion
    # successfully.
    assert sorted(collect.deleted_keys) == target


def test_retry_on_normal_error(collect):
    """Ensure retries are processed for most errors."""
    key_name = 'test-key-name'
    b = B(name=key_name)

    collect.inject(Exception('Normal error'))
    d = wabs_deleter.Deleter(BlobService('test', 'ing'), 'test-container')
    d.delete(b)

    # Since delete_keys will fail over and over again, aborted_keys
    # should grow quickly.
    while len(collect.aborted_keys) < 2:
        gevent.sleep(0.1)

    # Since delete_keys has been failing repeatedly, no keys should be
    # successfully deleted.
    assert not collect.deleted_keys

    # Turn off fault injection and flush/synchronize with close().
    collect.inject(None)
    d.close()

    # The one enqueued job should have been processed.n
    assert collect.deleted_keys == [key_name]


def test_no_retry_on_keyboadinterrupt(collect):
    """Ensure that KeyboardInterrupts are forwarded."""
    key_name = 'test-key-name'
    b = B(name=key_name)

    # If vanilla KeyboardInterrupt is used, then sending SIGINT to the
    # test can cause it to pass improperly, so use a subtype instead.
    class MarkedKeyboardInterrupt(KeyboardInterrupt):
        pass

    collect.inject(MarkedKeyboardInterrupt('SIGINT, probably'))
    d = wabs_deleter.Deleter(BlobService('test', 'ing'), 'test-container')

    with pytest.raises(MarkedKeyboardInterrupt):
        d.delete(b)

        # Exactly when coroutines are scheduled is non-deterministic,
        # so spin while yielding to provoke the
        # MarkedKeyboardInterrupt being processed within the
        # pytest.raises context manager.
        while True:
            gevent.sleep(0.1)

    # Only one key should have been aborted, since the purpose is to
    # *not* retry when processing KeyboardInterrupt.
    assert collect.aborted_keys == [key_name]

    # Turn off fault injection and flush/synchronize with close().
    collect.inject(None)
    d.close()

    # Since there is no retrying, no keys should be deleted.
    assert not collect.deleted_keys
