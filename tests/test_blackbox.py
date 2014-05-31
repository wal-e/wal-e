import pytest

from blackbox import config
from blackbox import noop_pg_backup_statements
from blackbox import small_push_dir
from os import path
from s3_integration_help import default_test_bucket
from stage_pgxlog import pg_xlog

# Quiet pyflakes about pytest fixtures.
assert config
assert noop_pg_backup_statements
assert small_push_dir
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

    config.main('wal-prefetch', path.dirname(unicode(download_file)), seg_name)
    assert tmpdir.join('.wal-e', 'prefetch', seg_name).check(file=1)


def test_wal_fetch_non_existent(tmpdir, config):
    # Recall file and check for equality.
    download_file = tmpdir.join('TEST-DOWNLOADED')

    with pytest.raises(SystemExit) as e:
        config.main('wal-fetch', 'irrelevant', unicode(download_file))

    assert e.value.code == 1


def test_backup_push_fetch(tmpdir, small_push_dir, monkeypatch, config,
                           noop_pg_backup_statements):
    import wal_e.tar_partition

    # check that _fsync_files() is called with the right
    # arguments. There's a separate unit test in test_tar_hacks.py
    # that it actually fsyncs the right files.
    fsynced_files = []
    monkeypatch.setattr(wal_e.tar_partition, '_fsync_files',
                        lambda filenames: fsynced_files.extend(filenames))

    config.main('backup-push', unicode(small_push_dir))

    fetch_dir = tmpdir.join('fetch-to').ensure(dir=True)
    config.main('backup-fetch', unicode(fetch_dir), 'LATEST')

    assert fetch_dir.join('arbitrary-file').read() == \
        small_push_dir.join('arbitrary-file').read()

    for filename in fetch_dir.listdir():
        if filename.check(link=0):
            assert unicode(filename) in fsynced_files
        elif filename.check(link=1):
            assert unicode(filename) not in fsynced_files


def test_delete_everything(config, small_push_dir, noop_pg_backup_statements):
    config.main('backup-push', unicode(small_push_dir))
    config.main('delete', '--confirm', 'everything')
