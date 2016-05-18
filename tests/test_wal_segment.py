import pytest

from stage_pgxlog import pg_xlog
from wal_e import worker
from wal_e import exception

# Quiet pyflakes about pytest fixtures.
assert pg_xlog


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
    assert next(segs).path == 'pg_xlog/' + name

    with pytest.raises(StopIteration):
        next(segs)


def test_multi_search(pg_xlog):
    """Test finding a few ready files.

    Also throw in some random junk to make sure they are filtered out
    from processing correctly.
    """
    for i in range(3):
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
