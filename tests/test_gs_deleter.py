import gevent
import pytest

from gevent import coros

from fast_wait import fast_wait
from gcloud import storage
from wal_e import exception
from wal_e.worker.gs import gs_deleter

assert fast_wait


class BucketDeleteBlobsCollector(object):
    """A callable to stand-in for bucket.delete_blobs

    Used to test that given blobs are bulk-deleted.

    Also can inject an exception.
    """
    def __init__(self):
        self.deleted_blobs = []
        self.aborted_blobs = []
        self.exc = None

        # Protect exc, since some paths test it and then use it, which
        # can run afoul race conditions.
        self._exc_protect = coros.RLock()

    def inject(self, exc):
        self._exc_protect.acquire()
        self.exc = exc
        self._exc_protect.release()

    def __call__(self, blobs, on_error=None):
        self._exc_protect.acquire()

        # Make sure the on_call function is correctly passed.
        assert on_error is gs_deleter._on_error

        try:
            if self.exc:
                self.aborted_blobs.extend(blob.name for blob in blobs)

                # Prevent starvation/livelock with a polling process
                # by yielding.
                gevent.sleep(0.1)

                raise self.exc
        finally:
            self._exc_protect.release()

        self.deleted_blobs.extend(blob.name for blob in blobs)


@pytest.fixture
def collect(monkeypatch):
    """Instead of performing bulk delete, collect blob names deleted.

    This is to test invariants, as to ensure deleted blobs are passed
    to gcloud properly.
    """

    collect = BucketDeleteBlobsCollector()
    monkeypatch.setattr(storage.Bucket, 'delete_blobs', collect)

    return collect


@pytest.fixture
def b():
    return storage.Bucket('test-bucket-name')


@pytest.fixture(autouse=True)
def never_use_single_delete(monkeypatch):
    """Detect any mistaken uses of single-blob deletion.

    Older wal-e versions used one-at-a-time deletions.  This is just
    to help ensure that use of this API (through the nominal boto
    symbol) is detected.
    """
    def die():
        assert False

    monkeypatch.setattr(storage.Blob, 'delete', die)
    monkeypatch.setattr(storage.Bucket, 'delete_blob', die)


def test_construction():
    """The constructor basically works."""
    gs_deleter.Deleter()


def test_close_error():
    """Ensure that attempts to use a closed Deleter results in an error."""

    d = gs_deleter.Deleter()
    d.close()

    with pytest.raises(exception.UserCritical):
        d.delete('no value should work')


def test_processes_one_deletion(b, collect):
    # Mock up a blob and bucket
    blob_name = 'test-blob-name'
    k = storage.Blob(blob_name, b)

    d = gs_deleter.Deleter()
    d.delete(k)
    d.close()

    assert collect.deleted_blobs == [blob_name]


def test_processes_many_deletions(b, collect):
    # Generate a target list of blobs in a stable order
    target = sorted(['test-blob-' + str(x) for x in range(20001)])

    # Construct blobs from the generated names and delete them all.
    blobs = [storage.Blob(blob_name, b) for blob_name in target]
    d = gs_deleter.Deleter()

    for k in blobs:
        d.delete(k)

    d.close()

    # Sort the deleted blob names to obtain another stable order and
    # then ensure that everything was passed for deletion
    # successfully.
    assert sorted(collect.deleted_blobs) == target


def test_retry_on_normal_error(b, collect):
    """Ensure retries are processed for most errors."""
    blob_name = 'test-blob-name'
    k = storage.Blob(blob_name, b)

    collect.inject(Exception('Normal error'))
    d = gs_deleter.Deleter()
    d.delete(k)

    # Since delete_blob will fail over and over again, aborted_blobs
    # should grow quickly.
    while len(collect.aborted_blobs) < 2:
        gevent.sleep(0.1)

    # Since delete_blob has been failing repeatedly, no blobs should
    # be successfully deleted.
    assert not collect.deleted_blobs

    # Turn off fault injection and flush/synchronize with close().
    collect.inject(None)
    d.close()

    # The one enqueued job should have been processed.
    assert collect.deleted_blobs == [blob_name]


def test_no_retry_on_keyboadinterrupt(b, collect):
    """Ensure that KeyboardInterrupts are forwarded."""
    blob_name = 'test-blob-name'
    k = storage.Blob(blob_name, b)

    # If vanilla KeyboardInterrupt is used, then sending SIGINT to the
    # test can cause it to pass improperly, so use a subtype instead.
    class MarkedKeyboardInterrupt(KeyboardInterrupt):
        pass

    collect.inject(MarkedKeyboardInterrupt('SIGINT, probably'))
    d = gs_deleter.Deleter()

    with pytest.raises(MarkedKeyboardInterrupt):
        d.delete(k)

        # Exactly when coroutines are scheduled is non-deterministic,
        # so spin while yielding to provoke the
        # MarkedKeyboardInterrupt being processed within the
        # pytest.raises context manager.
        while True:
            gevent.sleep(0.1)

    # Only one blob should have been aborted, since the purpose is to
    # *not* retry when processing KeyboardInterrupt.
    assert collect.aborted_blobs == [blob_name]

    # Turn off fault injection and flush/synchronize with close().
    collect.inject(None)
    d.close()

    # Since there is no retrying, no blobs should be deleted.
    assert not collect.deleted_blobs
