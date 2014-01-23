import tempfile
import time

from wal_e import storage
from wal_e.pipeline import get_upload_pipeline
from wal_e.blobstore import get_blobstore


def uri_put_file(creds, uri, fp, content_encoding=None):
    blobstore = get_blobstore(storage.StorageLayout(uri))
    return blobstore.uri_put_file(creds, uri, fp,
                                  content_encoding=content_encoding)


def do_lzop_put(creds, url, local_path, gpg_key):
    """
    Compress and upload a given local path.

    :type url: string
    :param url: A (s3|wabs)://bucket/key style URL that is the destination

    :type local_path: string
    :param local_path: a path to a file to be compressed

    """
    assert url.endswith('.lzo')
    blobstore = get_blobstore(storage.StorageLayout(url))

    with tempfile.NamedTemporaryFile(mode='r+b') as tf:
        pipeline = get_upload_pipeline(
            open(local_path, 'r'), tf, gpg_key=gpg_key)
        pipeline.finish()

        tf.flush()

        clock_start = time.time()
        tf.seek(0)
        k = blobstore.uri_put_file(creds, url, tf)
        clock_finish = time.time()

        kib_per_second = format_kib_per_second(
            clock_start, clock_finish, k.size)

        return kib_per_second


def do_lzop_get(creds, url, path, decrypt):
    """
    Get and decompress an S3 or WABS URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    blobstore = get_blobstore(storage.StorageLayout(url))
    return blobstore.do_lzop_get(creds, url, path, decrypt)


def format_kib_per_second(start, finish, amount_in_bytes):
    try:
        return '{0:02g}'.format((amount_in_bytes / 1024) / (finish - start))
    except ZeroDivisionError:
        return 'NaN'
