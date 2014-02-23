import os
import tarfile

from wal_e import tar_partition


def test_fsync_tar_members(monkeypatch, tmpdir):
    """Test that _fsync_files() syncs all files and directories

    Syncing directories is a platform specific feature, so it is
    optional.

    There is a separate test in test_blackbox that tar_file_extract()
    actually calls _fsync_files and passes it the expected list of
    files.

    """
    dira = tmpdir.join('dira').ensure(dir=True)
    dirb = tmpdir.join('dirb').ensure(dir=True)
    foo = dira.join('foo').ensure()
    bar = dirb.join('bar').ensure()
    baz = dirb.join('baz').ensure()

    # Monkeypatch around open, close, and fsync to capture which
    # filenames the file descriptors being fsynced actually correspond
    # to. Only bother to remember filenames in the tmpdir.

    # fd => filename for the tmpdir files currently open.
    open_descriptors = {}
    # Set of filenames that fsyncs have been called on.
    synced_filenames = set()
    # Filter on prefix of this path.
    tmproot = unicode(tmpdir)

    real_open = os.open
    real_close = os.close
    real_fsync = os.fsync

    def fake_open(filename, flags, mode=0777):
        fd = real_open(filename, flags, mode)
        if filename.startswith(tmproot):
            open_descriptors[fd] = filename
        return fd

    def fake_close(fd):
        if fd in open_descriptors:
            del open_descriptors[fd]
        real_close(fd)
        return

    def fake_fsync(fd):
        if fd in open_descriptors:
            synced_filenames.add(open_descriptors[fd])
        real_fsync(fd)
        return

    monkeypatch.setattr(os, 'open', fake_open)
    monkeypatch.setattr(os, 'close', fake_close)
    monkeypatch.setattr(os, 'fsync', fake_fsync)

    filenames = [unicode(filename) for filename in [foo, bar, baz]]
    tar_partition._fsync_files(filenames)

    for filename in filenames:
        assert filename in synced_filenames

    # Not every OS allows you to open directories, if not, don't try
    # to open it to fsync.
    if hasattr(os, 'O_DIRECTORY'):
        assert unicode(dira) in synced_filenames
        assert unicode(dirb) in synced_filenames


def test_creation_upper_dir(tmpdir, monkeypatch):
    """Check for upper-directory creation in untarring

    This affected the special "cat" based extraction works when no
    upper level directory is present.  Using that path depends on
    PIPE_BUF_BYTES, so test that integration via monkey-patching it to
    a small value.

    """
    from wal_e import pipebuf

    # Set up a directory with a file inside.
    adir = tmpdir.join('adir').ensure(dir=True)
    some_file = adir.join('afile')
    some_file.write('1234567890')

    tar_path = unicode(tmpdir.join('foo.tar'))

    # Add the file to a test tar, *but not the directory*.
    tar = tarfile.open(name=tar_path, mode='w')
    tar.add(unicode(some_file))
    tar.close()

    # Replace cat_extract with a version that does the same, but
    # ensures that it is called by the test.
    original_cat_extract = tar_partition.cat_extract

    class CheckCatExtract(object):
        def __init__(self):
            self.called = False

        def __call__(self, *args, **kwargs):
            self.called = True
            return original_cat_extract(*args, **kwargs)

    check = CheckCatExtract()
    monkeypatch.setattr(tar_partition, 'cat_extract', check)
    monkeypatch.setattr(pipebuf, 'PIPE_BUF_BYTES', 1)

    dest_dir = tmpdir.join('dest')
    dest_dir.ensure(dir=True)
    with open(tar_path) as f:
        tar_partition.TarPartition.tarfile_extract(f, unicode(dest_dir))

    # Make sure the test exercised cat_extraction.
    assert check.called
