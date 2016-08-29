import socket
import traceback
from urllib.parse import urlparse

import gevent

from swiftclient.exceptions import ClientException

from wal_e import log_help
from wal_e import files
from wal_e.blobstore.swift import calling_format
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count


logger = log_help.WalELogger(__name__)


class SwiftKey(object):
    def __init__(self, name, size, last_modified=None):
        self.name = name
        self.size = size
        self.last_modified = last_modified


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('swift://')

    url_tup = urlparse(uri)

    container_name = url_tup.netloc
    conn = calling_format.connect(creds)

    conn.put_object(
        container_name, url_tup.path, fp, content_type=content_type
    )
    # Swiftclient doesn't return us the total file size, we see how much of the
    # file swiftclient read in order to determine the file size.
    return SwiftKey(url_tup.path, size=fp.tell())


def do_lzop_get(creds, uri, path, decrypt, do_retry=True):
    """
    Get and decompress a Swift URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert uri.endswith('.lzo'), 'Expect an lzop-compressed file'

    def log_wal_fetch_failures_on_error(exc_tup, exc_processor_cxt):
        def standard_detail_message(prefix=''):
            return (prefix + '  There have been {n} attempts to fetch wal '
                    'file {uri} so far.'.format(n=exc_processor_cxt, uri=uri))
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
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:

                conn = calling_format.connect(creds)

                g = gevent.spawn(write_and_return_error, uri, conn, pl.stdin)

                # Raise any exceptions from write_and_return_error
                try:
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except ClientException as e:
                    if e.http_status == 404:
                        # Do not retry if the key not present, this
                        # can happen under normal situations.
                        pl.abort()
                        logger.warning(
                            msg=('could no longer locate object while '
                                 'performing wal restore'),
                            detail=('The absolute URI that could not be '
                                    'located is {uri}.'.format(uri=uri)),
                            hint=('This can be normal when Postgres is trying '
                                  'to detect what timelines are available '
                                  'during restoration.'))
                        decomp_out.remove_regardless = True
                        return False
                    else:
                        raise

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{uri}" to "{path}"'
                .format(uri=uri, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def uri_get_file(creds, uri, conn=None, resp_chunk_size=None):
    assert uri.startswith('swift://')
    url_tup = urlparse(uri)
    container_name = url_tup.netloc
    object_name = url_tup.path

    if conn is None:
        conn = calling_format.connect(creds)
    _, content = conn.get_object(
        container_name, object_name, resp_chunk_size=resp_chunk_size
    )
    return content


def write_and_return_error(uri, conn, stream):
    try:
        response = uri_get_file(None, uri, conn, resp_chunk_size=8192)
        for chunk in response:
            stream.write(chunk)
        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
