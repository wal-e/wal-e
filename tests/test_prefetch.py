import errno
import os
import pytest

from wal_e.worker import prefetch
from wal_e import worker


@pytest.fixture
def pd(tmpdir):
    d = prefetch.Dirs(str(tmpdir))
    return d


@pytest.fixture
def seg():
    return worker.WalSegment('0' * 8 * 3)


@pytest.fixture
def raise_eperm():
    def raiser(*args, **kwargs):
        e = OSError('bogus EPERM')
        e.errno = errno.EPERM
        raise e

    return raiser


def test_double_create(pd, seg):
    pd.create(seg)
    pd.create(seg)


def test_atomic_download(pd, seg, tmpdir):
    assert not pd.is_running(seg)

    pd.create(seg)
    assert pd.is_running(seg)

    with pd.download(seg) as ad:
        s = b'hello'
        ad.tf.write(s)
        ad.tf.flush()
        assert pd.running_size(seg) == len(s)

    assert pd.contains(seg)
    assert not pd.is_running(seg)

    promote_target = tmpdir.join('another-spot')
    pd.promote(seg, str(promote_target))

    pd.clear()
    assert not pd.contains(seg)


def test_atomic_download_failure(pd, seg):
    "Ensure a raised exception doesn't move WAL into place"
    pd.create(seg)
    e = Exception('Anything')

    with pytest.raises(Exception) as err:
        with pd.download(seg):
            raise e

    assert err.value is e
    assert not pd.is_running(seg)
    assert not pd.contains(seg)


def test_cleanup_running(pd, seg):
    pd.create(seg)
    assert pd.is_running(seg)

    nxt = next(seg.future_segment_stream())
    pd.clear_except([nxt])
    assert not pd.is_running(seg)


def test_cleanup_promoted(pd, seg):
    pd.create(seg)
    assert pd.is_running(seg)

    with pd.download(seg):
        pass

    assert not pd.is_running(seg)
    assert pd.contains(seg)

    nxt = next(seg.future_segment_stream())
    pd.clear_except([nxt])
    assert not pd.contains(seg)


def test_running_size_error(pd, seg, monkeypatch, raise_eperm):
    pd.create(seg)
    monkeypatch.setattr(os, 'listdir', raise_eperm)

    with pytest.raises(EnvironmentError):
        pd.running_size(seg)


def test_create_error(pd, seg, monkeypatch, raise_eperm):
    monkeypatch.setattr(os, 'makedirs', raise_eperm)
    assert not pd.create(seg)


def test_clear_error(pd, seg, monkeypatch, raise_eperm):
    pd.create(seg)
    monkeypatch.setattr(os, 'rmdir', raise_eperm)
    pd.clear()
