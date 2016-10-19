import base64
import collections
import errno
import gevent
import io
import os
import socket
import sys
import traceback

try:
    # New class name in the Azure SDK sometime after v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.txt
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    # Backwards compatbility for older Azure drivers.
    from azure import WindowsAzureMissingResourceError \
        as AzureMissingResourceHttpError

try:
    # New module location sometime after Azure SDK v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.txt
    from azure.storage.blob import BlobService
except ImportError:
    from azure.storage import BlobService

from . import calling_format
from hashlib import md5
from urllib.parse import urlparse
from wal_e import log_help
from wal_e import files
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count

assert calling_format

logger = log_help.WalELogger(__name__)

_Key = collections.namedtuple('_Key', ['size'])
WABS_CHUNK_SIZE = 4 * 1024 * 1024


def uri_put_file(creds, uri, fp, content_type=None):
    assert fp.tell() == 0
    assert uri.startswith('wabs://')

    def log_upload_failures_on_error(exc_tup, exc_processor_cxt):
        def standard_detail_message(prefix=''):
            return (prefix + '  There have been {n} attempts to upload  '
                    'file {url} so far.'.format(n=exc_processor_cxt, url=uri))
        typ, value, tb = exc_tup
        del exc_tup

        # Screen for certain kinds of known-errors to retry from
        if issubclass(typ, socket.error):
            socketmsg = value[1] if isinstance(value, tuple) else value

            logger.info(
                msg='Retrying upload because of a socket error',
                detail=standard_detail_message(
                    "The socket error's message is '{0}'."
                    .format(socketmsg)))
        else:
            # For all otherwise untreated exceptions, report them as a
            # warning and retry anyway -- all exceptions that can be
            # justified should be treated and have error messages
            # listed.
            logger.warning(
                msg='retrying file upload from unexpected exception',
                detail=standard_detail_message(
                    'The exception type is {etype} and its value is '
                    '{evalue} and its traceback is {etraceback}'
                    .format(etype=typ, evalue=value,
                            etraceback=''.join(traceback.format_tb(tb)))))

        # Help Python GC by resolving possible cycles
        del tb

    # Because we're uploading in chunks, catch rate limiting and
    # connection errors which occur for each individual chunk instead of
    # failing the whole file and restarting.
    @retry(retry_with_count(log_upload_failures_on_error))
    def upload_chunk(chunk, block_id):
        if isinstance(chunk, str):
            chunk = chunk.encode('utf-8')
        check_sum = base64.b64encode(md5(chunk).digest()).decode('utf-8')
        conn.put_block(url_tup.netloc, url_tup.path.lstrip('/'), chunk,
                       block_id, content_md5=check_sum)

    url_tup = urlparse(uri)
    kwargs = dict(x_ms_blob_type='BlockBlob')
    if content_type is not None:
        kwargs['x_ms_blob_content_type'] = content_type

    conn = BlobService(
        creds.account_name, creds.account_key,
        sas_token=creds.access_token, protocol='https')
    conn.put_blob(url_tup.netloc, url_tup.path.lstrip('/'), b'', **kwargs)

    # WABS requires large files to be uploaded in 4MB chunks
    block_ids = []
    length, index = 0, 0
    pool_size = os.getenv('WABS_UPLOAD_POOL_SIZE', 5)
    p = gevent.pool.Pool(size=pool_size)
    while True:
        data = fp.read(WABS_CHUNK_SIZE)
        if data:
            length += len(data)
            block_id = base64.b64encode(
                str(index).encode('utf-8')).decode('utf-8')
            p.wait_available()
            p.spawn(upload_chunk, data, block_id)
            block_ids.append(block_id)
            index += 1
        else:
            p.join()
            break

    conn.put_block_list(url_tup.netloc, url_tup.path.lstrip('/'), block_ids)

    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return _Key(size=len(data))


def uri_get_file(creds, uri, conn=None):
    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)

    if conn is None:
        conn = BlobService(creds.account_name, creds.account_key,
                           sas_token=creds.access_token, protocol='https')

    # Determin the size of the target blob
    props = conn.get_blob_properties(url_tup.netloc, url_tup.path.lstrip('/'))
    blob_size = int(props['content-length'])

    ret_size = 0
    data = io.BytesIO()
    # WABS requires large files to be downloaded in 4MB chunks
    while ret_size < blob_size:
        ms_range = 'bytes={0}-{1}'.format(ret_size,
                                          ret_size + WABS_CHUNK_SIZE - 1)
        while True:
            # Because we're downloading in chunks, catch rate limiting and
            # connection errors here instead of letting them bubble up to the
            # @retry decorator so that we don't have to start downloading the
            # whole file over again.
            try:
                part = conn.get_blob(url_tup.netloc,
                                     url_tup.path.lstrip('/'),
                                     x_ms_range=ms_range)
            except EnvironmentError as e:
                if e.errno in (errno.EBUSY, errno.ECONNRESET):
                    logger.warning(
                        msg="retrying after encountering exception",
                        detail=("Exception traceback:\n{0}".format(
                            traceback.format_exception(*sys.exc_info()))),
                        hint="")
                    gevent.sleep(30)
                else:
                    raise
            else:
                break
        length = len(part)
        ret_size += length
        data.write(part)
        if length > 0 and length < WABS_CHUNK_SIZE:
            break
        elif length == 0:
            break

    return data.getvalue()


def do_lzop_get(creds, url, path, decrypt, do_retry=True):
    """
    Get and decompress a WABS URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('wabs://')

    conn = BlobService(
        creds.account_name, creds.account_key,
        sas_token=creds.access_token, protocol='https')

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
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, url, conn, pl.stdin)

                try:
                    # Raise any exceptions guarded by
                    # write_and_return_error.
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except AzureMissingResourceHttpError:
                    # Short circuit any re-try attempts under certain race
                    # conditions.
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


def write_and_return_error(url, conn, stream):
    try:
        data = uri_get_file(None, url, conn=conn)
        stream.write(data)
        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
