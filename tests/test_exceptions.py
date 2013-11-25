# Test exception construction
#
# Flush out simple typo crashes and problems with occasional changes
# in the exception base classes.
from wal_e.tar_partition import TarMemberTooBigError


def test_tar_member_too_big_error():
    # Must not raise an exception.
    a = TarMemberTooBigError("file.dat", 100, 150)
    assert a.msg == 'Attempted to archive a file that is too large.'
    hint = ('There is a file in the postgres database directory that '
            'is larger than 100 bytes. If no such file exists, please '
            'report this as a bug. In particular, check file.dat, '
            'which appears to be 150 bytes.')
    assert a.hint == hint
