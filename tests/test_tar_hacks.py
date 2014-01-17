from wal_e import tar_partition
import os


# Test that _fsync_files() syncs all files and also, if possible, all
# directories passed to it. There is a separate test in test_blackbox
# that tar_file_extract() actually calls _fsync_files and passes it
# the expected list of files.

def test_fsync_tar_members(monkeypatch, tmpdir):
    dira = tmpdir.join('dira').ensure(dir=True)
    dirb = tmpdir.join('dirb').ensure(dir=True)
    foo = dira.join('foo').ensure()
    bar = dirb.join('bar').ensure()
    baz = dirb.join('baz').ensure()


    # we need to monkeypatch around open, close, and fsync to capture
    # which filenames the file descriptors being fsynced actually
    # correspond to. Only bother to remember filenames in the tmpdir.

    # fd => filename for the tmpdir files we currently have open
    open_descriptors = {}
    # set of filenames that we've seen fsyncs for
    synced_filenames = set()
    # filter on prefix of this path
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

    # Not every OS allows you to open directories, if not we can't and
    # presumably needn't fsync the parent directory
    if hasattr(os, 'O_DIRECTORY'):
        assert unicode(dira) in synced_filenames
        assert unicode(dirb) in synced_filenames
