import pytest
import os
import errno

from subprocess import call

from wal_e.storage import StorageLayout
from wal_e import exception
from wal_e.operator.file_operator import FileBackup

from wal_e.blobstore.file import uri_put_file
from wal_e.blobstore.file import uri_get_file
from wal_e.blobstore.file import do_lzop_get
from wal_e.blobstore.file import write_and_return_error


def create_files(files):
    """Helper function to create a test directory structure.
    File path is used as file contents"""
    for f in files:
        dir_path = os.path.dirname(f)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        fp = open(f, "wb")
        fp.write(f.encode("utf-8"))
        fp.close()


def test_valid_prefix():
    store = StorageLayout("file://localhost/tmp")
    assert store.is_file is True


def test_invalid_prefix():
    with pytest.raises(exception.UserException):
        StorageLayout("notfile://localhost/tmp")


def test_uri_put_file_writes_key_file(tmpdir):
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)
    with open(base + "/src.txt", "rb") as f:
        uri_put_file("", "file://localhost/" + base + "/dst.txt", f)

    with open(base + "/dst.txt", "rb") as dst_file:
        assert dst_file.read() == file_list[0].encode('utf-8')


def test_uri_put_file_creates_key_dir(tmpdir):
    """Verify file upload"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)
    with open(file_list[0], "rb") as f:
        uri_put_file("", "file://localhost/" + base + "/subdir/dst.txt", f)

    with open(base + "/subdir//dst.txt", "rb") as dst_file:
        assert dst_file.read() == file_list[0].encode('utf-8')


def test_uri_get_file(tmpdir):
    """Verify file download"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)
    file_contents = uri_get_file("", "file://localhost/" + base + "/src.txt")
    assert file_contents == file_list[0].encode('utf-8')


def test_bucket_list(tmpdir):
    """Verify bucket keys can be listed"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/subdirfile.txt",
                 base + "/subdir/file.txt"]
    create_files(file_list)
    store = StorageLayout("file://localhost/" + base)
    backup = FileBackup(store, "", "")
    conn = backup.cinfo.connect("")
    bucket = conn.get_bucket("")
    result = bucket.list(base)
    assert len(result) == len(file_list)
    for f in file_list:
        matches = [x for x in result if x.path == f]
        assert len(matches) == 1
        assert hasattr(matches[0], 'size') is True
        assert hasattr(matches[0], 'last_modified') is True


def test_delete_keys(tmpdir):
    """Verify keys are deleted and bucket is trimmed"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/subdir1/file.txt",
                 base + "/subdir2/file.txt"]
    create_files(file_list)
    store = StorageLayout("file://localhost/" + base)
    backup = FileBackup(store, "", "")
    conn = backup.cinfo.connect("")
    bucket = conn.get_bucket("")
    bucket.delete_keys(file_list)
    assert len(os.listdir(base)) == 0


def test_do_lzop_get(tmpdir):
    """Create a dummy lzo file and confirm it is download/decompression"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)
    call(["lzop", base + "/src.txt"])
    do_lzop_get("", "file://localhost/" + base + "/src.txt.lzo",
                base + "/dst.txt", False, do_retry=True)

    with open(base + "/dst.txt", "rb") as dst_file:
        assert dst_file.read() == file_list[0].encode('utf-8')


def test_do_lzop_get_missing_key(tmpdir):
    """Verify exception is raised for missing key"""
    base = str(tmpdir.mkdir("base"))
    with pytest.raises(IOError) as e:
        do_lzop_get("", "file://localhost/" + base + "/src.txt.lzo",
                    base + "/dst.txt", False, do_retry=True)

    assert e.value.errno == errno.ENOENT


def test_write_and_return_error(tmpdir):
    """Verify None as result in normal operation"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)

    store = StorageLayout("file://localhost/" + base)
    backup = FileBackup(store, "", "")
    conn = backup.cinfo.connect("")
    bucket = conn.get_bucket("")
    f = open(base + "/dst.txt", "wb")
    key = bucket.get_key(base + "/src.txt")

    result = write_and_return_error(key, f)
    assert result is None

    with open(base + "/dst.txt", "rb") as dst_file:
        assert dst_file.read() == file_list[0].encode('utf-8')


def test_write_and_return_error_with_error(tmpdir):
    """Verify exception as result in error operation"""
    base = str(tmpdir.mkdir("base"))
    file_list = [base + "/src.txt"]
    create_files(file_list)

    store = StorageLayout("file://localhost/" + base)
    backup = FileBackup(store, "", "")
    conn = backup.cinfo.connect("")
    bucket = conn.get_bucket("")
    f = open(base + "/dst.txt", "wb")
    key = bucket.get_key(base + "/missing.txt")

    with pytest.raises(IOError) as e:
        result = write_and_return_error(key, f)
        raise result

    assert e.value.errno == errno.ENOENT
