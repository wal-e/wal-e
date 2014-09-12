import boto
import os
import re
import urlparse

from boto import s3
from boto.s3 import connection
from wal_e import log_help
from wal_e.exception import UserException

logger = log_help.WalELogger(__name__)

_S3_REGIONS = {
    # A map like this is actually defined in boto.s3 in newer versions of boto
    # but we reproduce it here for the folks (notably, Ubuntu 12.04) on older
    # versions.
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-southeast-2': 's3-ap-southeast-2.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
    'sa-east-1': 's3-sa-east-1.amazonaws.com',
    'us-standard': 's3.amazonaws.com',
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
}

try:
    # Override the hard-coded region map with boto's mappings if
    # available.
    from boto.s3 import regions
    _S3_REGIONS.update(dict((r.name, r.endpoint) for r in regions()))
except ImportError:
    pass


def _is_ipv4_like(s):
    """Find if a string superficially looks like an IPv4 address.

    AWS documentation plays it fast and loose with this; in other
    regions, it seems like even non-valid IPv4 addresses (in
    particular, ones that possess decimal numbers out of range for
    IPv4) are rejected.
    """
    parts = s.split('.')

    if len(parts) != 4:
        return False

    for part in parts:
        try:
            int(part)
        except ValueError:
            return False

    return True


def _is_mostly_subdomain_compatible(bucket_name):
    """Returns True if SubdomainCallingFormat can be used...mostly

    This checks to make sure that putting aside certificate validation
    issues that a bucket_name is able to use the
    SubdomainCallingFormat.
    """
    return (bucket_name.lower() == bucket_name and
            len(bucket_name) >= 3 and
            len(bucket_name) <= 63 and
            '_' not in bucket_name and
            '..' not in bucket_name and
            '-.' not in bucket_name and
            '.-' not in bucket_name and
            not bucket_name.startswith('-') and
            not bucket_name.endswith('-') and
            not bucket_name.startswith('.') and
            not bucket_name.endswith('.') and
            not _is_ipv4_like(bucket_name))


def _connect_secureish(*args, **kwargs):
    """Connect using the safest available options.

    This turns on encryption (works in all supported boto versions)
    and certificate validation (in the subset of supported boto
    versions that can handle certificate validation, namely, those
    after 2.6.0).

    Versions below 2.6 don't support the validate_certs option to
    S3Connection, and enable it via configuration option just seems to
    cause an error.
    """
    if tuple(int(x) for x in boto.__version__.split('.')) >= (2, 6, 0):
        kwargs['validate_certs'] = True

    kwargs['is_secure'] = True

    return connection.S3Connection(*args, **kwargs)


def _s3connection_opts_from_uri(impl):
    # 'impl' should look like:
    #
    #    <protocol>+<calling_format>://[user:pass]@<host>[:port]
    #
    # A concrete example:
    #
    #     https+virtualhost://user:pass@localhost:1235
    o = urlparse.urlparse(impl, allow_fragments=False)

    if o.scheme is not None:
        proto_match = re.match(
            r'(?P<protocol>http|https)\+'
            r'(?P<format>virtualhost|path|subdomain)', o.scheme)
        if proto_match is None:
            raise UserException(
                msg='WALE_S3_ENDPOINT URI scheme is invalid',
                detail='The scheme defined is ' + repr(o.scheme),
                hint='An example of a valid scheme is https+virtualhost.')

    opts = {}

    if proto_match.group('protocol') == 'http':
        opts['is_secure'] = False
    else:
        # Constrained by prior regexp.
        proto_match.group('protocol') == 'https'
        opts['is_secure'] = True

    f = proto_match.group('format')
    if f == 'virtualhost':
        opts['calling_format'] = connection.VHostCallingFormat()
    elif f == 'path':
        opts['calling_format'] = connection.OrdinaryCallingFormat()
    elif f == 'subdomain':
        opts['calling_format'] = connection.SubdomainCallingFormat()
    else:
        # Constrained by prior regexp.
        assert False

    if o.username is not None or o.password is not None:
        raise UserException(
            msg='WALE_S3_ENDPOINT does not support username or password')

    if o.hostname is not None:
        opts['host'] = o.hostname

    if o.port is not None:
        opts['port'] = o.port

    if o.path:
        raise UserException(
            msg='WALE_S3_ENDPOINT does not support a URI path',
            detail='Path is {0!r}'.format(o.path))

    if o.query:
        raise UserException(
            msg='WALE_S3_ENDPOINT does not support query parameters')

    return opts


class CallingInfo(object):
    """Encapsulate information used to produce a S3Connection."""

    def __init__(self, bucket_name=None, calling_format=None, region=None,
                 ordinary_endpoint=None):
        self.bucket_name = bucket_name
        self.calling_format = calling_format
        self.region = region
        self.ordinary_endpoint = ordinary_endpoint

    def __repr__(self):
        return ('CallingInfo({bucket_name}, {calling_format!r}, {region!r}, '
                '{ordinary_endpoint!r})'.format(**self.__dict__))

    def __str__(self):
        return repr(self)

    def connect(self, creds):
        """Return a boto S3Connection set up with great care.

        This includes TLS settings, calling format selection, and
        region detection.

        The credentials are applied by the caller because in many
        cases (instance-profile IAM) it is possible for those
        credentials to fluctuate rapidly.  By comparison, region
        fluctuations of a bucket name are not nearly so likely versus
        the gains of not looking up a bucket's region over and over.
        """
        def _conn_help(*args, **kwargs):
            return _connect_secureish(
                *args,
                provider=creds,
                calling_format=self.calling_format(),
                **kwargs)

        # If WALE_S3_ENDPOINT is set, do not attempt to guess
        # the right calling conventions and instead honor the explicit
        # settings within WALE_S3_ENDPOINT.
        impl = os.getenv('WALE_S3_ENDPOINT')
        if impl:
            return connection.S3Connection(**_s3connection_opts_from_uri(impl))

        # Check if subdomain format compatible; no need to go through
        # any region detection mumbo-jumbo of any kind.
        if self.calling_format is connection.SubdomainCallingFormat:
            return _conn_help()

        # Check if OrdinaryCallingFormat compatible, but also see if
        # the endpoint has already been set, in which case only
        # setting the host= flag is necessary.
        assert self.calling_format is connection.OrdinaryCallingFormat
        if self.ordinary_endpoint is not None:
            return _conn_help(host=self.ordinary_endpoint)

        # By this point, this is an OrdinaryCallingFormat bucket that
        # has never had its region detected in this CallingInfo
        # instance.  So, detect its region (this can happen without
        # knowing the right regional endpoint) and store it to speed
        # future calls.
        assert self.calling_format is connection.OrdinaryCallingFormat
        assert self.region is None
        assert self.ordinary_endpoint is None

        conn = _conn_help()

        bucket = s3.bucket.Bucket(connection=conn,
                                  name=self.bucket_name)

        try:
            loc = bucket.get_location()
        except boto.exception.S3ResponseError, e:
            if e.status == 403:
                # A 403 can be caused by IAM keys that do not permit
                # GetBucketLocation.  To not change behavior for
                # environments that do not have GetBucketLocation
                # allowed, fall back to the default endpoint,
                # preserving behavior for those using us-standard.
                logger.warning(msg='cannot detect location of bucket',
                               detail=('The specified bucket name was: ' +
                                       repr(self.bucket_name)),
                               hint=('Permit the GetLocation permission for '
                                     'the provided AWS credentials.  '
                                     'Or, use a bucket name that follows the '
                                     'preferred bucket naming guidelines '
                                     'and has no dots in it.'))

                self.region = 'us-standard'
                self.ordinary_endpoint = _S3_REGIONS[self.region]
            else:
                raise
        else:
            # An empty, successful get location returns an empty
            # string to mean S3-Classic/US-Standard.
            if loc == '':
                loc = 'us-standard'

            self.region = loc
            self.ordinary_endpoint = _S3_REGIONS[loc]

        # Region/endpoint information completed: connect.
        assert self.ordinary_endpoint is not None
        return _conn_help(host=self.ordinary_endpoint)


def from_store_name(bucket_name):
    """Construct a CallingInfo value from a bucket name.

    This is useful to encapsulate the ugliness of setting up S3
    connections, especially with regions and TLS certificates are
    involved.
    """
    mostly_ok = _is_mostly_subdomain_compatible(bucket_name)

    if not mostly_ok:
        return CallingInfo(
            bucket_name=bucket_name,
            region='us-standard',
            calling_format=connection.OrdinaryCallingFormat,
            ordinary_endpoint=_S3_REGIONS['us-standard'])
    else:
        if '.' in bucket_name:
            # The bucket_name might have been DNS compatible, but once
            # dots are involved TLS certificate validations will
            # certainly fail even if that's the case.
            return CallingInfo(
                bucket_name=bucket_name,
                calling_format=connection.OrdinaryCallingFormat,
                region=None,
                ordinary_endpoint=None)
        else:
            # If the bucket follows naming rules and has no dots in
            # the name, SubdomainCallingFormat can be used, with TLS,
            # world-wide, and WAL-E can be region-oblivious.
            return CallingInfo(
                bucket_name=bucket_name,
                calling_format=connection.SubdomainCallingFormat,
                region=None,
                ordinary_endpoint=None)

    assert False
