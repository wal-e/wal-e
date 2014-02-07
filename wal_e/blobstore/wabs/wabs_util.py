import os
import sys
import gevent
import errno
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
WABS_CHUNK_SIZE = 4 * 1024 * 1024


def uri_put_file(creds, uri, fp, content_encoding=None):
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
        check_sum = base64.encodestring(md5(chunk).digest()).strip('\n')
        conn.put_block(url_tup.netloc, url_tup.path, chunk,
                       block_id, content_md5=check_sum)

    url_tup = urlparse(uri)
    kwargs = dict(x_ms_blob_type='BlockBlob')
    if content_encoding is not None:
        kwargs['x_ms_blob_content_encoding'] = content_encoding

    conn = BlobService(creds.account_name, creds.account_key, protocol='https')
    conn.put_blob(url_tup.netloc, url_tup.path, '', **kwargs)

    # WABS requires large files to be uploaded in 4MB chunks
    block_ids = []
    length, index = 0, 0
    pool_size = os.getenv('WABS_UPLOAD_POOL_SIZE', 5)
    p = gevent.pool.Pool(size=pool_size)
    while True:
        data = fp.read(WABS_CHUNK_SIZE)
        if data:
            length += len(data)
            block_id = base64.b64encode(str(index))
            p.wait_available()
            p.spawn(upload_chunk, data, block_id)
            block_ids.append(block_id)
            index += 1
        else:
            p.join()
            break

    conn.put_block_list(url_tup.netloc, url_tup.path, block_ids)

    # To maintain consistency with the S3 version of this function we must
    # return an object with a certain set of attributes.  Currently, that set
    # of attributes consists of only 'size'
    return _Key(size=len(data))


def uri_get_file(creds, uri, conn=None):
    assert uri.startswith('wabs://')
    url_tup = urlparse(uri)

    if conn is None:
        conn = BlobService(creds.account_name, creds.account_key,
                           protocol='https')

    # Determin the size of the target blob
    props = conn.get_blob_properties(url_tup.netloc, url_tup.path)
    blob_size = int(props['content-length'])

    ret_size = 0
    data = ''
    # WABS requires large files to be downloaded in 4MB chunks
    while ret_size < blob_size:
        ms_range = 'bytes={}-{}'.format(ret_size,
                                        ret_size + WABS_CHUNK_SIZE - 1)
        while True:
            # Because we're downloading in chunks, catch rate limiting and
            # connection errors here instead of letting them bubble up to the
            # @retry decorator so that we don't have to start downloading the
            # whole file over again.
            try:
                part = conn.get_blob(url_tup.netloc,
                                     url_tup.path,
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
        data += part
        if length > 0 and length < WABS_CHUNK_SIZE:
            break
        elif length == 0:
            break

    return data


def do_lzop_get(creds, url, path, decrypt):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert url.endswith('.lzo'), 'Expect an lzop-compressed file'
    assert url.startswith('wabs://')

    conn = BlobService(creds.account_name, creds.account_key, protocol='https')

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
                # Raise any exceptions guarded by
                # write_and_return_error.
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
    try:
        data = uri_get_file(None, url, conn=conn)
        stream.write(data)
        stream.flush()
    except Exception, e:
        return e
    finally:
        stream.close()
