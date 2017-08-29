import gevent
import socket
import traceback

from . import calling_format
from google.cloud import storage
from urllib.parse import urlparse
from wal_e import files
from wal_e import log_help
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count
from google.cloud.exceptions import NotFound

logger = log_help.WalELogger(__name__)


def _uri_to_blob(creds, uri, conn=None):
    assert uri.startswith('gs://')
    url_tup = urlparse(uri)
    bucket_name = url_tup.netloc
    if conn is None:
        conn = calling_format.connect(creds)
    b = storage.Bucket(conn, name=bucket_name)
    return storage.Blob(url_tup.path, b)


def uri_put_file(creds, uri, fp, content_type=None, conn=None):
    assert fp.tell() == 0
    blob = _uri_to_blob(creds, uri, conn=conn)

    fp.seek(0, 2)
    size = fp.tell()

    fp.seek(0, 0)
    # num_retries is deprecated in favor of a retry strategy that
    # includes backoffs
    blob.upload_from_file(fp, size=size, content_type=content_type)
    return blob


def uri_get_file(creds, uri, conn=None):
    blob = _uri_to_blob(creds, uri, conn=conn)
    return blob.download_as_string()


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a GCS URL

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

        # Screen for certain kinds of known-errors to retry from
        if issubclass(typ, socket.error):
            socketmsg = value[1] if isinstance(value, tuple) else value

            logger.info(
                msg='Retrying fetch because of a socket error',
                detail=standard_detail_message(
                    "The socket error's message is '{0}'."
                    .format(socketmsg)))
        else:
            # For all otherwise untreated exceptions, report them as a
            # warning and retry anyway -- all exceptions that can be
            # justified should be treated and have error messages
            # listed.
            logger.warning(
                msg='retrying WAL file fetch from unexpected exception',
                detail=standard_detail_message(
                    'The exception type is {etype} and its value is '
                    '{evalue} and its traceback is {etraceback}'
                    .format(etype=typ, evalue=value,
                            etraceback=''.join(traceback.format_tb(tb)))))

        # Help Python GC by resolving possible cycles
        del tb

    def download():
        with files.DeleteOnError(path) as decomp_out:
            blob = _uri_to_blob(creds, url)
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, blob, pl.stdin)

                try:
                    # Raise any exceptions from write_and_return_error
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except NotFound as e:
                    # Do not retry if the blob not present, this
                    # can happen under normal situations.
                    pl.abort()
                    logger.warning(
                        msg=('could no longer locate object while '
                             'performing wal restore'),
                        detail=('The absolute URI that could not be '
                                'located is {url}.'.format(url=url)),
                        hint=('This can be normal when Postgres is trying '
                              'to detect what timelines are available '
                              'during restoration.'))
                    decomp_out.remove_regardless = True
                    return False

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def write_and_return_error(blob, stream):
    try:
        blob.download_to_file(stream)
        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
