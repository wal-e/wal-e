import pytest

from wal_e import exception
from wal_e import worker


class FakeTarPartition(object):
    """Implements enough protocol to test concurrency semantics."""
    def __init__(self, num_members, explosive=False):
        self._explosive = explosive
        self.num_members = num_members

    def __len__(self):
        return self.num_members


class FakeUploader(object):
    """A no-op uploader that makes affordance for fault injection."""

    def __call__(self, tpart):
        if tpart._explosive:
            raise tpart._explosive

        return tpart


class Explosion(Exception):
    """Marker type of injected faults."""
    pass


def make_pool(max_concurrency, max_members):
    """Set up a pool with a FakeUploader"""
    return worker.TarUploadPool(FakeUploader(),
                                max_concurrency, max_members)


def test_simple():
    """Simple case of uploading one partition."""
    pool = make_pool(1, 1)
    pool.put(FakeTarPartition(1))
    pool.join()


def test_not_enough_resources():
    """Detect if a too-large segment can never complete."""
    pool = make_pool(1, 1)

    with pytest.raises(exception.UserCritical):
        pool.put(FakeTarPartition(2))

    pool.join()


def test_simple_concurrency():
    """Try a pool that cannot execute all submitted jobs at once."""
    pool = make_pool(1, 1)

    for i in range(3):
        pool.put(FakeTarPartition(1))

    pool.join()


def test_fault_midstream():
    """Test if a previous upload fault is detected in calling .put.

    This case is seen while pipelining many uploads in excess of the
    maximum concurrency.

    NB: This test is critical as to prevent failed uploads from
    failing to notify a caller that the entire backup is incomplete.
    """
    pool = make_pool(1, 1)

    # Set up upload doomed to fail.
    tpart = FakeTarPartition(1, explosive=Explosion('Boom'))
    pool.put(tpart)

    # Try to receive the error through adding another upload.
    tpart = FakeTarPartition(1)
    with pytest.raises(Explosion):
        pool.put(tpart)


def test_fault_join():
    """Test if a fault is detected when .join is used.

    This case is seen at the end of a series of uploads.

    NB: This test is critical as to prevent failed uploads from
    failing to notify a caller that the entire backup is incomplete.
    """
    pool = make_pool(1, 1)

    # Set up upload doomed to fail.
    tpart = FakeTarPartition(1, explosive=Explosion('Boom'))
    pool.put(tpart)

    # Try to receive the error while finishing up.
    with pytest.raises(Explosion):
        pool.join()


def test_put_after_join():
    """New jobs cannot be submitted after a .join

    This is mostly a re-check to detect programming errors.
    """
    pool = make_pool(1, 1)

    pool.join()

    with pytest.raises(exception.UserCritical):
        pool.put(FakeTarPartition(1))


def test_pool_concurrent_success():
    pool = make_pool(4, 4)

    for i in range(30):
        pool.put(FakeTarPartition(1))

    pool.join()


def test_pool_concurrent_failure():
    pool = make_pool(4, 4)

    parts = [FakeTarPartition(1) for i in range(30)]

    exc = Explosion('boom')
    parts[27]._explosive = exc

    with pytest.raises(Explosion) as e:
        for part in parts:
            pool.put(part)

        pool.join()

    assert e.value is exc
