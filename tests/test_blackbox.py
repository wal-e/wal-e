import blackbox
import pytest

from blackbox import config
from s3_integration_help import default_test_bucket
from stage_pgxlog import pg_xlog

# Quiet pyflakes about pytest fixtures.
assert config
assert default_test_bucket
assert pg_xlog


def test_wal_push_fetch(pg_xlog, tmpdir, config):
    contents = 'abcdefghijlmnopqrstuvwxyz\n' * 10000
    seg_name = '00000001' * 3
    pg_xlog.touch(seg_name, '.ready')
    pg_xlog.seg(seg_name).write(contents)
    config.main('wal-push', 'pg_xlog/' + seg_name)

    # Recall file and check for equality.
    download_file = tmpdir.join('TEST-DOWNLOADED')
    config.main('wal-fetch', seg_name, unicode(download_file))
    assert download_file.read() == contents


def test_wal_fetch_non_existent(tmpdir, config):
    # Recall file and check for equality.
    download_file = tmpdir.join('TEST-DOWNLOADED')

    with pytest.raises(SystemExit) as e:
        config.main('wal-fetch', 'irrelevant', unicode(download_file))

    assert e.value.code == 1


def test_backup_push_fetch(tmpdir, monkeypatch, config):
    import wal_e.tar_partition
    import wal_e.operator.backup
    fsynced_files = []

    monkeypatch.setattr(wal_e.operator.backup, 'PgBackupStatements',
                        blackbox.NoopPgBackupStatements)

    # psql binary test will fail if local pg env isn't set up
    monkeypatch.setattr(wal_e.cmd, 'external_program_check',
                        lambda *args, **kwargs: None)

    # check that _fsync_files() is called with the right
    # arguments. There's a separate unit test in test_tar_hacks.py
    # that it actually fsyncs the right files.
    monkeypatch.setattr(wal_e.tar_partition, '_fsync_files',
                        lambda filenames: fsynced_files.extend(filenames))

    contents = 'abcdefghijlmnopqrstuvwxyz\n' * 10000
    push_dir = tmpdir.join('push-from').ensure(dir=True)
    push_dir.join('arbitrary-file').write(contents)

    # Construct a symlink a non-existent path.  This provoked a crash
    # at one time.
    push_dir.join('pg_xlog').mksymlinkto('/tmp/wal-e-test-must-not-exist')

    # Holy crap, the tar segmentation code relies on the directory
    # containing files without a common prefix...the first character
    # of two files must be distinct!
    push_dir.join('holy-smokes').ensure()

    config.main('backup-push', unicode(push_dir))

    fetch_dir = tmpdir.join('fetch-to').ensure(dir=True)
    config.main('backup-fetch', unicode(fetch_dir), 'LATEST')

    assert fetch_dir.join('arbitrary-file').read() == contents

    for filename in fetch_dir.listdir():
        if filename.check(link=0):
            assert unicode(filename) in fsynced_files
        elif filename.check(link=1):
            assert unicode(filename) not in fsynced_files
