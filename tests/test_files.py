import errno
import os
import pytest

from wal_e import files


def test_no_error(tmpdir):
    p = str(tmpdir.join('somefile'))
    with files.DeleteOnError(p) as doe:
        doe.f.write(b'hello')

    with open(p, 'rb') as f:
        assert f.read() == b'hello'


def test_clear_on_error(tmpdir):
    p = str(tmpdir.join('somefile'))

    boom = Exception('Boom')
    with pytest.raises(Exception) as e:
        with files.DeleteOnError(p) as doe:
            doe.f.write(b'hello')
            raise boom
    assert e.value == boom

    with pytest.raises(IOError) as e:
        open(p)

    assert e.value.errno == errno.ENOENT


def test_no_error_if_already_deleted(tmpdir):
    p = str(tmpdir.join('somefile'))

    with files.DeleteOnError(p) as doe:
        doe.f.write(b'hello')
        os.unlink(p)


def test_explicit_deletion_without_exception(tmpdir):
    p = str(tmpdir.join('somefile'))

    with files.DeleteOnError(p) as doe:
        doe.f.write(b'hello')
        doe.remove_regardless = True

    with pytest.raises(IOError) as e:
        open(p)

    assert e.value.errno == errno.ENOENT
