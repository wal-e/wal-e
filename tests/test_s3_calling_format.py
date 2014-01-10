import boto
import inspect
import os
import pytest

from boto.s3 import connection
from s3_integration_help import (
    FreshBucket,
    no_real_s3_credentials,
)
from wal_e.blobstore.s3 import Credentials
from wal_e.blobstore.s3 import calling_format
from wal_e.blobstore.s3.calling_format import (
    _is_mostly_subdomain_compatible,
    _is_ipv4_like,
)

SUBDOMAIN_BOGUS = [
    '1.2.3.4',
    'myawsbucket.',
    'myawsbucket-.',
    'my.-awsbucket',
    '.myawsbucket',
    'myawsbucket-',
    '-myawsbucket',
    'my_awsbucket',
    'my..examplebucket',

    # Too short.
    'sh',

    # Too long.
    'long' * 30,
]

SUBDOMAIN_OK = [
    'myawsbucket',
    'my-aws-bucket',
    'myawsbucket.1',
    'my.aws.bucket'
]

# Contrivance to quiet down pyflakes, since pytest does some
# string-evaluation magic in test collection.
no_real_s3_credentials = no_real_s3_credentials


def test_subdomain_detect():
    """Exercise subdomain compatible/incompatible bucket names."""
    for bn in SUBDOMAIN_OK:
        assert _is_mostly_subdomain_compatible(bn) is True

    for bn in SUBDOMAIN_BOGUS:
        assert _is_mostly_subdomain_compatible(bn) is False


def test_us_standard_default_for_bogus():
    """Test degradation to us-standard for all weird bucket names.

    Such bucket names are not supported outside of us-standard by
    WAL-E.
    """
    for bn in SUBDOMAIN_BOGUS:
        cinfo = calling_format.from_store_name(bn)
        assert cinfo.region == 'us-standard'


def test_cert_validation_sensitivity():
    """Test degradation of dotted bucket names to OrdinaryCallingFormat

    Although legal bucket names with SubdomainCallingFormat, these
    kinds of bucket names run afoul certification validation, and so
    they are forced to fall back to OrdinaryCallingFormat.
    """
    for bn in SUBDOMAIN_OK:
        if '.' not in bn:
            cinfo = calling_format.from_store_name(bn)
            assert (cinfo.calling_format ==
                    boto.s3.connection.SubdomainCallingFormat)
        else:
            assert '.' in bn

            cinfo = calling_format.from_store_name(bn)
            assert (cinfo.calling_format == connection.OrdinaryCallingFormat)
            assert cinfo.region is None
            assert cinfo.ordinary_endpoint is None


@pytest.mark.skipif("no_real_s3_credentials()")
def test_real_get_location():
    """Exercise a case where a get location call is needed.

    In cases where a bucket has offensive characters -- like dots --
    that would otherwise break TLS, test sniffing the right endpoint
    so it can be used to address the bucket.
    """
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    bucket_name = 'wal-e-test-us-west-1.get.location'

    cinfo = calling_format.from_store_name(bucket_name)

    with FreshBucket(bucket_name,
                     host='s3-us-west-1.amazonaws.com',
                     calling_format=connection.OrdinaryCallingFormat()) as fb:
        fb.create(location='us-west-1')
        conn = cinfo.connect(creds)

        assert cinfo.region == 'us-west-1'
        assert cinfo.calling_format is connection.OrdinaryCallingFormat
        assert conn.host == 's3-us-west-1.amazonaws.com'


@pytest.mark.skipif("no_real_s3_credentials()")
def test_classic_get_location():
    """Exercise get location on a s3-classic bucket."""
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    bucket_name = 'wal-e-test.classic.get.location'

    cinfo = calling_format.from_store_name(bucket_name)

    with FreshBucket(bucket_name,
                     host='s3.amazonaws.com',
                     calling_format=connection.OrdinaryCallingFormat()) as fb:
        fb.create()
        conn = cinfo.connect(creds)

        assert cinfo.region == 'us-standard'
        assert cinfo.calling_format is connection.OrdinaryCallingFormat
        assert conn.host == 's3.amazonaws.com'


@pytest.mark.skipif("no_real_s3_credentials()")
def test_subdomain_compatible():
    """Exercise a case where connecting is region-oblivious."""
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    bucket_name = 'wal-e-test-us-west-1-no-dots'

    cinfo = calling_format.from_store_name(bucket_name)

    with FreshBucket(bucket_name,
                     host='s3-us-west-1.amazonaws.com',
                     calling_format=connection.OrdinaryCallingFormat()) as fb:
        fb.create(location='us-west-1')
        conn = cinfo.connect(creds)

        assert cinfo.region is None
        assert cinfo.calling_format is connection.SubdomainCallingFormat
        assert isinstance(conn.calling_format,
                          connection.SubdomainCallingFormat)


def test_ipv4_detect():
    """IPv4 lookalikes are not valid SubdomainCallingFormat names

    Even though they otherwise follow the bucket naming rules,
    IPv4-alike names are called out as specifically banned.
    """
    assert _is_ipv4_like('1.1.1.1') is True

    # Out-of IPv4 numerical range is irrelevant to the rules.
    assert _is_ipv4_like('1.1.1.256') is True
    assert _is_ipv4_like('-1.1.1.1') is True

    assert _is_ipv4_like('1.1.1.hello') is False
    assert _is_ipv4_like('hello') is False
    assert _is_ipv4_like('-1.1.1') is False
    assert _is_ipv4_like('-1.1.1.') is False


@pytest.mark.skipif("no_real_s3_credentials()")
def test_get_location_errors(monkeypatch):
    """Simulate situations where get_location fails

    Exercise both the case where IAM refuses the privilege to get the
    bucket location and where some other S3ResponseError is raised
    instead.
    """
    bucket_name = 'wal-e.test.403.get.location'

    def just_403(self):
        raise boto.exception.S3ResponseError(status=403,
                                             reason=None, body=None)

    def unhandled_404(self):
        raise boto.exception.S3ResponseError(status=404,
                                             reason=None, body=None)

    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    with FreshBucket(bucket_name,
                     calling_format=connection.OrdinaryCallingFormat()):
        cinfo = calling_format.from_store_name(bucket_name)

        # Provoke a 403 when trying to get the bucket location.
        monkeypatch.setattr(boto.s3.bucket.Bucket, 'get_location', just_403)
        cinfo.connect(creds)

        assert cinfo.region == 'us-standard'
        assert cinfo.calling_format is connection.OrdinaryCallingFormat

        cinfo = calling_format.from_store_name(bucket_name)

        # Provoke an unhandled S3ResponseError, in this case 404 not
        # found.
        monkeypatch.setattr(boto.s3.bucket.Bucket, 'get_location',
                            unhandled_404)

        with pytest.raises(boto.exception.S3ResponseError) as e:
            cinfo.connect(creds)

        assert e.value.status == 404


def test_str_repr_call_info():
    """Ensure CallingInfo renders sensibly.

    Try a few cases sensitive to the bucket name.
    """
    if boto.__version__ <= '2.2.0':
        pytest.skip('Class name output is unstable on older boto versions')

    cinfo = calling_format.from_store_name('hello-world')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(hello-world, "
        "<class 'boto.s3.connection.SubdomainCallingFormat'>, "
        "None, None)"
    )

    cinfo = calling_format.from_store_name('hello.world')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(hello.world, "
        "<class 'boto.s3.connection.OrdinaryCallingFormat'>, "
        "None, None)"
    )

    cinfo = calling_format.from_store_name('Hello-World')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(Hello-World, "
        "<class 'boto.s3.connection.OrdinaryCallingFormat'>, "
        "'us-standard', 's3.amazonaws.com')"
    )


@pytest.mark.skipif("no_real_s3_credentials()")
@pytest.mark.skipif("sys.version_info < (2, 7)")
def test_cipher_suites():
    # Imported for its side effects of setting up ssl cipher suites
    # and gevent.
    from wal_e import cmd

    # Quiet pyflakes.
    assert cmd

    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))
    cinfo = calling_format.from_store_name('irrelevant')
    conn = cinfo.connect(creds)

    # Warm up the pool and the connection in it; new_http_connection
    # seems to be a more natural choice, but leaves the '.sock'
    # attribute null.
    conn.get_all_buckets()

    # Set up 'port' keyword argument for newer Botos that require it.
    spec = inspect.getargspec(conn._pool.get_http_connection)
    kw = {'host': 's3.amazonaws.com',
          'is_secure': True}
    if 'port' in spec.args:
        kw['port'] = 443

    htcon = conn._pool.get_http_connection(**kw)

    chosen_cipher_suite = htcon.sock.cipher()[0].split('-')

    # Test for the expected cipher suite.
    #
    # This can change or vary on different platforms somewhat
    # harmlessly, but do the simple thing and insist on an exact match
    # for now.
    assert chosen_cipher_suite == ['AES256', 'SHA']
