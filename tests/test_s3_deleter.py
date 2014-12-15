import gevent
import pytest

from gevent import coros

from boto.s3 import bucket
from boto.s3 import key
from fast_wait import fast_wait
from wal_e import exception
from wal_e.worker.s3 import s3_deleter

assert fast_wait


class BucketDeleteKeysCollector(object):
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

    def __call__(self, keys):
        self._exc_protect.acquire()

        try:
            if self.exc:
                self.aborted_keys.extend(keys)

                # Prevent starvation/livelock with a polling process
                # by yielding.
                gevent.sleep(0.1)

                raise self.exc
        finally:
            self._exc_protect.release()

        self.deleted_keys.extend(keys)


@pytest.fixture
def collect(monkeypatch):
    """Instead of performing bulk delete, collect key names deleted.

    This is to test invariants, as to ensure deleted keys are passed
    to boto properly.
    """

    collect = BucketDeleteKeysCollector()
    monkeypatch.setattr(bucket.Bucket, 'delete_keys', collect)

    return collect


@pytest.fixture
def b():
    return bucket.Bucket(name='test-bucket-name')


@pytest.fixture(autouse=True)
def never_use_single_delete(monkeypatch):
    """Detect any mistaken uses of single-key deletion.

    Older wal-e versions used one-at-a-time deletions.  This is just
    to help ensure that use of this API (through the nominal boto
    symbol) is detected.
    """
    def die():
        assert False

    monkeypatch.setattr(key.Key, 'delete', die)
    monkeypatch.setattr(bucket.Bucket, 'delete_key', die)


def test_construction():
    """The constructor basically works."""
    s3_deleter.Deleter()


def test_close_error():
    """Ensure that attempts to use a closed Deleter results in an error."""

    d = s3_deleter.Deleter()
    d.close()

    with pytest.raises(exception.UserCritical):
        d.delete('no value should work')


def test_processes_one_deletion(b, collect):
    # Mock up a key and bucket
    key_name = 'test-key-name'
    k = key.Key(bucket=b, name=key_name)

    d = s3_deleter.Deleter()
    d.delete(k)
    d.close()

    assert collect.deleted_keys == [key_name]


def test_processes_many_deletions(b, collect):
    # Generate a target list of keys in a stable order
    target = sorted(['test-key-' + str(x) for x in range(20001)])

    # Construct boto S3 Keys from the generated names and delete them
    # all.
    keys = [key.Key(bucket=b, name=key_name) for key_name in target]
    d = s3_deleter.Deleter()

    for k in keys:
        d.delete(k)

    d.close()

    # Sort the deleted key names to obtain another stable order and
    # then ensure that everything was passed for deletion
    # successfully.
    assert sorted(collect.deleted_keys) == target


def test_retry_on_normal_error(b, collect):
    """Ensure retries are processed for most errors."""
    key_name = 'test-key-name'
    k = key.Key(bucket=b, name=key_name)

    collect.inject(Exception('Normal error'))
    d = s3_deleter.Deleter()
    d.delete(k)

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


def test_no_retry_on_keyboadinterrupt(b, collect):
    """Ensure that KeyboardInterrupts are forwarded."""
    key_name = 'test-key-name'
    k = key.Key(bucket=b, name=key_name)

    # If vanilla KeyboardInterrupt is used, then sending SIGINT to the
    # test can cause it to pass improperly, so use a subtype instead.
    class MarkedKeyboardInterrupt(KeyboardInterrupt):
        pass

    collect.inject(MarkedKeyboardInterrupt('SIGINT, probably'))
    d = s3_deleter.Deleter()

    with pytest.raises(MarkedKeyboardInterrupt):
        d.delete(k)

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
