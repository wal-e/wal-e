import errno
import os
import pytest

from wal_e import files


def test_no_error(tmpdir):
    p = unicode(tmpdir.join('somefile'))
    with files.DeleteOnError(p) as doe:
        doe.f.write('hello')

    with open(p) as f:
        assert f.read() == 'hello'


def test_clear_on_error(tmpdir):
    p = unicode(tmpdir.join('somefile'))

    boom = StandardError('Boom')
    with pytest.raises(StandardError) as e:
        with files.DeleteOnError(p) as doe:
            doe.f.write('hello')
            raise boom
    assert e.value == boom

    with pytest.raises(IOError) as e:
        open(p)

    assert e.value.errno == errno.ENOENT


def test_no_error_if_already_deleted(tmpdir):
    p = unicode(tmpdir.join('somefile'))

    with files.DeleteOnError(p) as doe:
        doe.f.write('hello')
        os.unlink(p)


def test_explicit_deletion_without_exception(tmpdir):
    p = unicode(tmpdir.join('somefile'))

    with files.DeleteOnError(p) as doe:
        doe.f.write('hello')
        doe.remove_regardless = True

    with pytest.raises(IOError) as e:
        open(p)

    assert e.value.errno == errno.ENOENT
