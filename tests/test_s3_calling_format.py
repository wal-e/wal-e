import boto
import inspect
import os
import pytest
import wal_e.exception

from boto.s3 import connection
from s3_integration_help import (
    FreshBucket,
    bucket_name_mangle,
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


def test_bogus_region(monkeypatch):
    # Raises an error when it is necessary to resolve a hostname for a
    # bucket but no such region is found in the AWS endpoint
    # dictionary.
    monkeypatch.setenv('AWS_REGION', 'not-a-valid-region-name')
    with pytest.raises(wal_e.exception.UserException) as e:
        calling_format.from_store_name('forces.OrdinaryCallingFormat')

    assert e.value.msg == 'Could not resolve host for AWS_REGION'
    assert e.value.detail == 'AWS_REGION is set to "not-a-valid-region-name".'

    # Doesn't raise an error when it is unnecessary to resolve a
    # hostname for given bucket name.
    monkeypatch.setenv('AWS_REGION', 'not-a-valid-region-name')
    calling_format.from_store_name('subdomain-format-acceptable')


def test_cert_validation_sensitivity(monkeypatch):
    """Test degradation of dotted bucket names to OrdinaryCallingFormat

    Although legal bucket names with SubdomainCallingFormat, these
    kinds of bucket names run afoul certification validation, and so
    they are forced to fall back to OrdinaryCallingFormat.
    """
    monkeypatch.setenv('AWS_REGION', 'us-east-1')
    for bn in SUBDOMAIN_OK:
        if '.' not in bn:
            cinfo = calling_format.from_store_name(bn)
            assert (cinfo.calling_format ==
                    boto.s3.connection.SubdomainCallingFormat)
        else:
            assert '.' in bn

            cinfo = calling_format.from_store_name(bn)
            assert (cinfo.calling_format == connection.OrdinaryCallingFormat)
            assert cinfo.region == 'us-east-1'
            assert cinfo.ordinary_endpoint == 's3.amazonaws.com'


@pytest.mark.skipif("no_real_s3_credentials()")
def test_subdomain_compatible():
    """Exercise a case where connecting is region-oblivious."""
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    bucket_name = bucket_name_mangle('wal-e-test-us-west-1-no-dots')

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


def test_str_repr_call_info(monkeypatch):
    """Ensure CallingInfo renders sensibly.

    Try a few cases sensitive to the bucket name.
    """
    monkeypatch.setenv('AWS_REGION', 'us-east-1')
    cinfo = calling_format.from_store_name('hello-world')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(hello-world, "
        "<class 'boto.s3.connection.SubdomainCallingFormat'>, "
        "'us-east-1', None)"
    )

    cinfo = calling_format.from_store_name('hello.world')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(hello.world, "
        "<class 'boto.s3.connection.OrdinaryCallingFormat'>, "
        "'us-east-1', 's3.amazonaws.com')"
    )

    cinfo = calling_format.from_store_name('Hello-World')
    assert repr(cinfo) == str(cinfo)
    assert repr(cinfo) == (
        "CallingInfo(Hello-World, "
        "<class 'boto.s3.connection.OrdinaryCallingFormat'>, "
        "'us-east-1', 's3.amazonaws.com')"
    )


@pytest.mark.skipif("no_real_s3_credentials()")
def test_cipher_suites():
    # Imported for its side effects of setting up ssl cipher suites
    # and gevent.
    from wal_e import cmd

    # Quiet pyflakes.
    assert cmd

    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))
    cinfo = calling_format.from_store_name('irrelevant', region='us-east-1')
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

    htcon = conn.new_http_connection(**kw)
    htcon.connect()
    chosen_cipher_suite = htcon.sock.cipher()[0].split('-')

    # Test for the expected cipher suite.
    #
    # This can change or vary on different platforms and over time as
    # new suites are phased in harmlessly, but do the simple thing and
    # insist on an exact match for now.
    acceptable = [
        ['AES256', 'SHA'],
        ['AES128', 'SHA'],
        ['ECDHE', 'RSA', 'AES128', 'SHA'],
        ['ECDHE', 'RSA', 'AES128', 'GCM', 'SHA256']
    ]

    assert chosen_cipher_suite in acceptable
