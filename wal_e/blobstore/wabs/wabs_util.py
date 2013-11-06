import gevent
import socket
from urlparse import urlparse
import traceback
from collections import namedtuple as _namedtuple
from hashlib import md5
import base64

from azure import WindowsAzureMissingResourceError
from azure.storage import BlobService

import wal_e.log_help as log_help
from wal_e.retries import retry, retry_with_count
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE

from . import calling_format
assert calling_format

logger = log_help.WalELogger(__name__)

_Key = _namedtuple('_Key', ['size'])


def uri_put_file(access_key, secret_key, uri, fp, content_encoding=None):
    assert fp.tell() == 0
    data = fp.read()

    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)
    check_sum = base64.encodestring(md5(data).digest())
    kwargs = dict(x_ms_blob_type='BlockBlob',
                  content_md5=check_sum.strip('\n'))
    if content_encoding is not None:
        kwargs['x_ms_blob_content_encoding'] = content_encoding

    conn = BlobService(access_key, secret_key, protocol='https')
    conn.put_blob(url_tup.netloc, url_tup.path, data, **kwargs)
    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return _Key(size=len(data))


def uri_get_file(access_key, secret_key, uri, conn=None):
    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)

    if conn is None:
        conn = BlobService(access_key, secret_key, protocol='https')
    return conn.get_blob(url_tup.netloc, url_tup.path)


def do_lzop_get(access_key, secret_key, url, path, decrypt):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('wabs://')

    conn = BlobService(access_key, secret_key)

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

    @retry(retry_with_count(log_wal_fetch_failures_on_error))
    def download():
        with open(path, 'wb') as decomp_out:
            pipeline = get_download_pipeline(PIPE, decomp_out, decrypt)
            g = gevent.spawn(write_and_return_error, url, conn, pipeline.stdin)

            try:
                # Raise any exceptions from _write_and_close
                g.get()
            except WindowsAzureMissingResourceError:
                # Short circuit any re-try attempts under certain race
                # conditions.
                logger.warn(
                    msg=('could no longer locate object while performing '
                         'wal restore'),
                    detail=('The URI  at {url} no longer exists.'
                            .format(url=url)),
                    hint=('This can be normal when Postgres is trying to '
                          'detect what timelines are available during '
                          'restoration.'))
                return False

            pipeline.finish()

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    return download()


def write_and_return_error(url, conn, stream):
    url_tup = urlparse(url)
    try:
        data = conn.get_blob(url_tup.netloc, url_tup.path)
        stream.write(data)
        stream.flush()
    except Exception, e:
        return e
    finally:
        stream.close()
