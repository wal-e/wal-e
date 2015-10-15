from urlparse import urlparse
import socket
import traceback
import gevent

from . import calling_format
from wal_e import files
from wal_e import log_help
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count

logger = log_help.WalELogger(__name__)

def _uri_to_key(creds, uri, conn=None):
    assert uri.startswith('file://')
    url_tup = urlparse(uri)
    bucket_name = url_tup.netloc
    if conn is None:
        conn = calling_format.connect(creds)
    return conn.get_bucket(bucket_name).get_key(url_tup.path)


def uri_put_file(creds, uri, fp, content_encoding=None, conn=None):
    assert fp.tell() == 0

    k = _uri_to_key(creds, uri, conn=conn)

    if content_encoding is not None:
        k.content_type = content_encoding

    k.set_contents_from_file(fp)
    return k


def uri_get_file(creds, uri, conn=None):
    k = _uri_to_key(creds, uri, conn=conn)
    return k.get_contents_as_string()


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'

    def log_wal_fetch_failures_on_error(exc_tup, exc_processor_cxt):
        def standard_detail_message(prefix=''):
            return (prefix + '  There have been {n} attempts to fetch wal '
                    'file {url} so far.'.format(n=exc_processor_cxt, url=url))
        typ, value, tb = exc_tup
        del exc_tup

        # TODO: better error handling ?
        logger.warning(
            msg='retrying WAL file fetch from unexpected exception',
            detail=standard_detail_message(
                'The exception type is {etype} and its value is '
                '{evalue} and its traceback is {etraceback}'
                .format(etype=typ, evalue=value, etraceback=''.join(traceback.format_tb(tb)))))

        # Help Python GC by resolving possible cycles
        del tb

    def download():
        with files.DeleteOnError(path) as decomp_out:
            key = _uri_to_key(creds, url)
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, key, pl.stdin)

                try:
                    # Raise any exceptions from write_and_return_error
                    exc = g.get()
                    if exc is not None:
                        raise exc
                finally:
                    pass

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def write_and_return_error(key, stream):
    try:
        key.get_contents_to_file(stream)
        stream.flush()
    except Exception, e:
        return e
    finally:
        stream.close()
