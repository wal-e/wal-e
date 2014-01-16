import blackbox
import pytest

from blackbox import apply_blackbox_config, main
from s3_integration_help import default_test_bucket
from stage_pgxlog import pg_xlog
from wal_e import cmd
from wal_e.operator import s3_operator

# Quiet pyflakes about pytest fixtures.
assert apply_blackbox_config
assert default_test_bucket
assert main
assert pg_xlog


def test_wal_push_fetch(pg_xlog, tmpdir, main):
    contents = 'abcdefghijlmnopqrstuvwxyz\n' * 10000
    seg_name = '00000001' * 3
    pg_xlog.touch(seg_name, '.ready')
    pg_xlog.seg(seg_name).write(contents)
    main('wal-push', 'pg_xlog/' + seg_name)

    # Recall file and check for equality.
    download_file = tmpdir.join('TEST-DOWNLOADED')
    main('wal-fetch', seg_name, unicode(download_file))
    assert download_file.read() == contents


def test_wal_fetch_non_existent(tmpdir, main):
    # Recall file and check for equality.
    download_file = tmpdir.join('TEST-DOWNLOADED')

    with pytest.raises(SystemExit) as e:
        main('wal-fetch', 'irrelevant', unicode(download_file))

    assert e.value.code == 1


def test_backup_push(tmpdir, monkeypatch, main):
    monkeypatch.setattr(s3_operator, 'PgBackupStatements',
                        blackbox.NoopPgBackupStatements)
    monkeypatch.setattr(cmd, 'external_program_check',
                        lambda *args, **kwargs: None)

    contents = 'abcdefghijlmnopqrstuvwxyz\n' * 10000
    push_dir = tmpdir.join('push-from').ensure(dir=True)
    push_dir.join('arbitrary-file').write(contents)

    # Holy crap, the tar segmentation code relies on the directory
    # containing files without a common prefix...the first character
    # of two files must be distinct!
    push_dir.join('holy-smokes').ensure()

    main('backup-push', unicode(push_dir))

    fetch_dir = tmpdir.join('fetch-to').ensure(dir=True)
    main('backup-fetch', unicode(fetch_dir), 'LATEST')
    assert fetch_dir.join('arbitrary-file').read() == contents
