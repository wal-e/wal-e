import datetime
import pytest

from blackbox import config
from blackbox import noop_pg_backup_statements
from blackbox import small_push_dir
from gs_integration_help import default_test_gs_bucket
from os import path
from s3_integration_help import default_test_bucket
from stage_pgxlog import pg_xlog

# Quiet pyflakes about pytest fixtures.
assert config
assert default_test_bucket
assert default_test_gs_bucket
assert noop_pg_backup_statements
assert pg_xlog
assert small_push_dir


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


def test_wal_push_parallel(pg_xlog, config, monkeypatch):
    from wal_e.worker import upload

    old_info = upload.logger.info

    class GatherActions(object):
        def __init__(self):
            self.actions = set()

        def __call__(self, *args, **kwargs):
            s = kwargs['structured']
            self.actions.add((s['action'], s['state']))
            return old_info(*args, **kwargs)

    ga = GatherActions()
    monkeypatch.setattr(upload.logger, 'info', ga)

    def seg_name(*parts):
        return ''.join(str(p).zfill(8) for p in parts)

    segments = [seg_name(1, 1, x) for x in xrange(1, 4)]

    for s in segments:
        pg_xlog.touch(s, '.ready')

    # Prepare the second segment with *only* a ready file, to make
    # sure parallel-push doesn't crash when pg_xlog's file is missing.
    pg_xlog.seg(segments[1]).remove()

    # This push has enough parallelism that it should attempt all the
    # wal segments staged.
    config.main('wal-push', '-p8', 'pg_xlog/' + segments[0])

    # Ensure all three action types, particularly the "skip" state,
    # are encountered.
    assert ga.actions == set([('push-wal', 'begin'),
                              ('push-wal', 'skip'),
                              ('push-wal', 'complete')])

    # An explicit request to upload a segment that doesn't exist must
    # yield a failure.
    #
    # NB: Normally one would use pytest.raises, but in this case,
    # e.value was *sometimes* giving an integer value, and sometimes
    # the SystemExit value, whereas the builtin try/except constructs
    # appear reliable by comparison.
    try:
        config.main('wal-push', '-p8', 'pg_xlog/' + segments[1])
    except SystemExit as e:
        assert e.code == 1
    else:
        assert False


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

    # Spin around backup-fetch LATEST for a while to paper over race
    # conditions whereby a backup may not be visible to backup-fetch
    # immediately.
    from boto import exception
    start = datetime.datetime.now()
    deadline = start + datetime.timedelta(seconds=15)
    while True:
        try:
            config.main('backup-fetch', unicode(fetch_dir), 'LATEST')
        except exception.S3ResponseError:
            if datetime.datetime.now() > deadline:
                raise
            else:
                continue
        else:
            break

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
