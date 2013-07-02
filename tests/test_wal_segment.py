import pytest

from wal_e import worker
from wal_e import exception


class PgXlog(object):
    """Test utility for staging a pg_xlog directory."""

    def __init__(self, cluster):
        self.cluster = cluster

        self.pg_xlog = cluster.join('pg_xlog')
        self.pg_xlog.ensure(dir=True)

        self.status = self.pg_xlog.join('archive_status')
        self.status.ensure(dir=True)

    def touch(self, name, status):
        assert status in ('.ready', '.done')

        self.pg_xlog.join(name).ensure(file=True)
        self.status.join(name + status).ensure(file=True)

    def assert_exists(self, name, status):
        assert status in ('.ready', '.done')

        assert self.pg_xlog.join(name).check(exists=1)
        assert self.status.join(name + status).check(exists=1)


@pytest.fixture()
def pg_xlog(tmpdir, monkeypatch):
    """Set up xlog utility functions and change directories."""
    monkeypatch.chdir(tmpdir)

    return PgXlog(tmpdir)


def make_segment(num, **kwargs):
    return worker.WalSegment('pg_xlog/' + str(num) * 8 * 3, **kwargs)


def test_simple_create():
    """Check __init__."""
    make_segment(1)


def test_mark_done_invariant():
    """Check explicit segments cannot be .mark_done'd."""
    seg = make_segment(1, explicit=True)

    with pytest.raises(exception.UserCritical):
        seg.mark_done()


def test_mark_done(pg_xlog):
    """Check non-explicit segments can be .mark_done'd."""
    seg = make_segment(1, explicit=False)

    pg_xlog.touch(seg.name, '.ready')
    seg.mark_done()


def test_mark_done_problem(pg_xlog, monkeypatch):
    """Check that mark_done fails loudly if status file is missing.

    While in normal operation, WAL-E does not expect races against
    other processes manipulating .ready files.  But, just in case that
    should occur, WAL-E is designed to crash, exercised here.
    """
    seg = make_segment(1, explicit=False)

    with pytest.raises(exception.UserCritical):
        seg.mark_done()


def test_simple_search(pg_xlog):
    """Must find a .ready file"""
    name = '1' * 8 * 3
    pg_xlog.touch(name, '.ready')

    segs = worker.WalSegment.from_ready_archive_status('pg_xlog')
    assert segs.next().path == 'pg_xlog/' + name

    with pytest.raises(StopIteration):
        segs.next()


def test_multi_search(pg_xlog):
    """Test finding a few ready files.

    Also throw in some random junk to make sure they are filtered out
    from processing correctly.
    """
    for i in xrange(3):
        ready = str(i) * 8 * 3
        pg_xlog.touch(ready, '.ready')

    # Throw in a complete segment that should be ignored.
    complete_segment_name = 'F' * 8 * 3
    pg_xlog.touch(complete_segment_name, '.done')

    # Throw in a history-file-alike that also should not be found,
    # even if it's ready.
    ready_history_file_name = ('F' * 8) + '.history'
    pg_xlog.touch(ready_history_file_name, '.ready')

    segs = worker.WalSegment.from_ready_archive_status(str(pg_xlog.pg_xlog))

    for i, seg in enumerate(segs):
        assert seg.name == str(i) * 8 * 3

    assert i == 2

    # Make sure nothing interesting happened to ignored files.
    pg_xlog.assert_exists(complete_segment_name, '.done')
    pg_xlog.assert_exists(ready_history_file_name, '.ready')
