from urllib.parse import urlparse
import gevent
import os
import socket
import traceback

import boto

from . import calling_format
from wal_e import files
from wal_e import log_help
from wal_e.exception import UserException
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count

logger = log_help.WalELogger(__name__)

# Set a timeout for boto HTTP operations should no timeout be set.
# Yes, in the case the user *wanted* no timeouts, this would set one.
# If that becomes a problem, someone should post a bug, although I am
# having a hard time imagining why that behavior could ever be useful.
if not boto.config.has_option('Boto', 'http_socket_timeout'):
    if not boto.config.has_section('Boto'):
        boto.config.add_section('Boto')

    boto.config.set('Boto', 'http_socket_timeout', '5')


def _uri_to_key(creds, uri, conn=None):
    assert uri.startswith('s3://')
    url_tup = urlparse(uri)
    bucket_name = url_tup.netloc
    cinfo = calling_format.from_store_name(bucket_name)
    if conn is None:
        conn = cinfo.connect(creds)
    bucket = boto.s3.bucket.Bucket(connection=conn, name=bucket_name)
    return boto.s3.key.Key(bucket=bucket, name=url_tup.path)


def uri_put_file(creds, uri, fp, content_type=None, conn=None):
    # Per Boto 2.2.2, which will only read from the current file
    # position to the end.  This manifests as successfully uploaded
    # *empty* keys in S3 instead of the intended data because of how
    # tempfiles are used (create, fill, submit to boto).
    #
    # It is presumed it is the caller's responsibility to rewind the
    # file position, and since the whole program was written with this
    # in mind, assert it as a precondition for using this procedure.
    assert fp.tell() == 0

    k = _uri_to_key(creds, uri, conn=conn)

    if content_type is not None:
        k.content_type = content_type

    k.set_contents_from_file(fp, encrypt_key=True)
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

        # Screen for certain kinds of known-errors to retry from
        if issubclass(typ, socket.error):
            socketmsg = value[1] if isinstance(value, tuple) else value

            logger.info(
                msg='Retrying fetch because of a socket error',
                detail=standard_detail_message(
                    "The socket error's message is '{0}'."
                    .format(socketmsg)))
        elif (issubclass(typ, boto.exception.S3ResponseError) and
              value.error_code == 'RequestTimeTooSkewed'):
            logger.info(msg='Retrying fetch because of a Request Skew time',
                        detail=standard_detail_message())
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
            key = _uri_to_key(creds, url)
            with get_download_pipeline(PIPE, decomp_out.f, decrypt) as pl:
                g = gevent.spawn(write_and_return_error, key, pl.stdin)

                try:
                    # Raise any exceptions from write_and_return_error
                    exc = g.get()
                    if exc is not None:
                        raise exc
                except boto.exception.S3ResponseError as e:
                    if e.status == 404:
                        # Do not retry if the key not present, this
                        # can happen under normal situations.
                        pl.abort()
                        logger.info(
                            msg=('could no longer locate object while '
                                 'performing wal restore'),
                            detail=('The absolute URI that could not be '
                                    'located is {url}.'.format(url=url)),
                            hint=('This can be normal when Postgres is trying '
                                  'to detect what timelines are available '
                                  'during restoration.'))
                        decomp_out.remove_regardless = True
                        return False
                    else:
                        raise

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{url}" to "{path}"'
                .format(url=url, path=path))
        return True

    if do_retry:
        download = retry(
            retry_with_count(log_wal_fetch_failures_on_error))(download)

    return download()


def sigv4_check_apply():
    # Insist that one of AWS_REGION or WALE_S3_ENDPOINT is defined.
    # The former is for authenticating correctly with AWS SigV4.
    #
    # The latter is for alternate implementations that are
    # S3-interface compatible.  Many, or most, of these do not support
    # AWS SigV4 at all and none are known to require SigV4 (and
    # instead use the non-region-demanding SigV2), so simplify by
    # relaxing the AWS_REGION requirement in that case.
    region = os.getenv('AWS_REGION')
    endpoint = os.getenv('WALE_S3_ENDPOINT')

    if region and endpoint:
        logger.warning(msg='WALE_S3_ENDPOINT defined, ignoring AWS_REGION',
                       hint='AWS_REGION is only intended for use with AWS S3, '
                       'and not interface-compatible use cases supported by '
                       'WALE_S3_ENDPOINT')
    elif region and not endpoint:
        # Normal case for an AWS user: Set up SigV4, which can only be
        # enacted globally.
        if not boto.config.has_option('s3', 'use-sigv4'):
            if not boto.config.has_section('s3'):
                boto.config.add_section('s3')

            boto.config.set('s3', 'use-sigv4', 'True')
    elif not region and endpoint:
        # Normal case for a S3-interface-compatible user, e.g. RADOS
        # or Deis users.  SigV4 doesn't have the same level of uptake
        # on those implementations.
        pass
    elif not region and not endpoint:
        raise UserException(
            msg='must define one of AWS_REGION or WALE_S3_ENDPOINT',
            hint=(
                'AWS users will want to set AWS_REGION, those using '
                'alternative S3-compatible systems will want to use '
                'WALE_S3_ENDPOINT.'
            )
        )
    else:
        # Entire Cartesian product should be handled.
        assert False


def write_and_return_error(key, stream):
    try:
        key.get_contents_to_file(stream)
        stream.flush()
    except Exception as e:
        return e
    finally:
        stream.close()
