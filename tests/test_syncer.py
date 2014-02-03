from wal_e.worker import syncer
import os


def test_recursive_fsync(monkeypatch, tmpdir):
    dira = tmpdir.join('dira').ensure(dir=True)
    dirb = tmpdir.join('dirb').ensure(dir=True)
    foo = dira.join('foo').ensure()
    bar = dirb.join('bar').ensure()
    baz = dirb.join('baz').ensure()

    dangling_symlink = dirb.join('dangle').mksymlinkto(
        '/tmp/wal-e-test-must-not-exist')

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
    syncer.recursive_fsync(unicode(tmpdir))

    for filename in filenames:
        assert filename in synced_filenames

    # Ensure link was not attempted to be followed and fsynced.
    assert unicode(dangling_symlink) not in synced_filenames

    # Not every OS allows you to open directories, if not, don't try
    # to open it to fsync.
    if hasattr(os, 'O_DIRECTORY'):
        assert unicode(dira) in synced_filenames
        assert unicode(dirb) in synced_filenames
