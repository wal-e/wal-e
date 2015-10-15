from urllib.parse import urlparse
import gevent

from . import calling_format
from wal_e import files
from wal_e import log_help
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE

logger = log_help.WalELogger(__name__)


def _uri_to_key(creds, uri, conn=None):
    assert uri.startswith('file://')
    url_tup = urlparse(uri)
    bucket_name = url_tup.netloc
    if conn is None:
        conn = calling_format.connect(creds)
    return conn.get_bucket(bucket_name).get_key(url_tup.path)


def uri_put_file(creds, uri, fp, content_type=None, conn=None):
    assert fp.tell() == 0

    k = _uri_to_key(creds, uri, conn=conn)

    k.set_contents_from_file(fp)
    return k


def uri_get_file(creds, uri, conn=None):
    k = _uri_to_key(creds, uri, conn=conn)
    return k.get_contents_as_string()


def do_lzop_get(creds, url, path, decrypt, do_retry):
    """
    Get and decompress a URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'

    with files.DeleteOnError(path) as decomp_out:
        key = _uri_to_key(creds, url)
        with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
            g = gevent.spawn(write_and_return_error, key, pl.stdin)
            exc = g.get()
            if exc is not None:
                raise exc

        logger.info(
            msg='completed download and decompression',
            detail='Downloaded and decompressed "{url}" to "{path}"'
            .format(url=url, path=path))

    return True


def write_and_return_error(key, stream):
    try:
        key.get_contents_to_file(stream)
        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
