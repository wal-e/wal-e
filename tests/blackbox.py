import gs_integration_help
import os
import pytest
import s3_integration_help
import sys

from wal_e import cmd

_PREFIX_VARS = ['WALE_S3_PREFIX', 'WALE_WABS_PREFIX', 'WALE_SWIFT_PREFIX']

_AWS_CRED_ENV_VARS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                      'AWS_SECURITY_TOKEN', 'AWS_REGION']
_GS_CRED_ENV_VARS = ['GOOGLE_APPLICATION_CREDENTIALS']


class AwsTestConfig(object):
    name = 'aws'

    def __init__(self, request):
        self.env_vars = {}
        self.monkeypatch = request.getfuncargvalue('monkeypatch')
        self.default_test_bucket = request.getfuncargvalue(
            'default_test_bucket')
        for name in _AWS_CRED_ENV_VARS:
            maybe_value = os.getenv(name)
            self.env_vars[name] = maybe_value

    def patch(self, test_name):
        # Scrub WAL-E prefixes left around in the user's environment to
        # prevent unexpected results.
        for name in _PREFIX_VARS:
            self.monkeypatch.delenv(name, raising=False)

        # Set other credentials.
        for name, value in self.env_vars.items():
            if value is None:
                self.monkeypatch.delenv(name, raising=False)
            else:
                self.monkeypatch.setenv(name, value)

        self.monkeypatch.setenv('WALE_S3_PREFIX', 's3://{0}/{1}'
                                .format(self.default_test_bucket, test_name))
        self.monkeypatch.setenv('AWS_REGION', 'us-west-1')

    def main(self, *args):
        self.monkeypatch.setattr(sys, 'argv', ['wal-e'] + list(args))
        return cmd.main()


class AwsTestConfigSetImpl(AwsTestConfig):
    name = 'aws+impl'

    # The same as AwsTestConfig but with a WALE_S3_ENDPOINT override.
    def patch(self, test_name):
        self.monkeypatch.setenv(
            'WALE_S3_ENDPOINT',
            'http+path://s3-us-west-1.amazonaws.com:80')
        return AwsTestConfig.patch(self, test_name)


class AwsInstanceProfileTestConfig(object):
    name = 'aws+instance-profile'

    def __init__(self, request):
        self.request = request
        self.monkeypatch = request.getfuncargvalue('monkeypatch')
        self.default_test_bucket = request.getfuncargvalue(
            'default_test_bucket')

    def patch(self, test_name):
        # Get STS-vended credentials to stand in for Instance Profile
        # credentials before scrubbing the environment of AWS
        # environment variables.
        c = s3_integration_help.sts_conn()
        policy = s3_integration_help.make_policy(self.default_test_bucket,
                                                 test_name)
        fed = c.get_federation_token(self.default_test_bucket, policy=policy)

        # Scrub AWS environment-variable based cred to make sure the
        # instance profile path is used.
        for name in _AWS_CRED_ENV_VARS:
            self.monkeypatch.delenv(name, raising=False)

        self.monkeypatch.setenv('WALE_S3_PREFIX', 's3://{0}/{1}'
                                .format(self.default_test_bucket, test_name))
        self.monkeypatch.setenv('AWS_REGION', 'us-west-1')

        # Patch boto.utils.get_instance_metadata to return a ginned up
        # credential.
        m = {
            "Code": "Success",
            "LastUpdated": "3014-01-11T02:13:53Z",
            "Type": "AWS-HMAC",
            "AccessKeyId": fed.credentials.access_key,
            "SecretAccessKey": fed.credentials.secret_key,
            "Token": fed.credentials.session_token,
            "Expiration": "3014-01-11T08:16:59Z"
        }

        from boto import provider
        self.monkeypatch.setattr(provider.Provider,
                            '_credentials_need_refresh',
                            lambda self: False)

        # Different versions of boto require slightly different return
        # formats.
        import test_aws_instance_profiles
        if test_aws_instance_profiles.boto_flat_metadata():
            m = {'irrelevant': m}
        else:
            m = {'iam': {'security-credentials': {'irrelevant': m}}}
        from boto import utils

        self.monkeypatch.setattr(utils, 'get_instance_metadata',
                            lambda *args, **kwargs: m)

    def main(self, *args):
        self.monkeypatch.setattr(
            sys, 'argv', ['wal-e', '--aws-instance-profile'] + list(args))
        return cmd.main()


class GsTestConfig(object):
    name = 'gs'

    def __init__(self, request):
        self.env_vars = {}
        self.monkeypatch = request.getfuncargvalue('monkeypatch')
        self.bucket = request.getfuncargvalue('default_test_gs_bucket')

        for name in _GS_CRED_ENV_VARS:
            maybe_value = os.getenv(name)
            self.env_vars[name] = maybe_value

    def patch(self, test_name):
        # Scrub WAL-E prefixes left around in the user's environment to
        # prevent unexpected results.
        for name in _PREFIX_VARS:
            self.monkeypatch.delenv(name, raising=False)

        # Set other credentials.
        for name, value in self.env_vars.items():
            if value is None:
                self.monkeypatch.delenv(name, raising=False)
            else:
                self.monkeypatch.setenv(name, value)

        self.monkeypatch.setenv('WALE_GS_PREFIX', 'gs://{0}/{1}'
                                .format(self.bucket, test_name))

    def main(self, *args):
        self.monkeypatch.setattr(sys, 'argv', ['wal-e'] + list(args))
        return cmd.main()


def _make_fixture_param_and_ids():
    ret = {
        'params': [],
        'ids': [],
    }

    def _add_config(c):
        ret['params'].append(c)
        ret['ids'].append(c.name)

    if not s3_integration_help.no_real_s3_credentials():
        _add_config(AwsTestConfig)
        _add_config(AwsTestConfigSetImpl)
        _add_config(AwsInstanceProfileTestConfig)

    if not gs_integration_help.no_real_gs_credentials():
        _add_config(GsTestConfig)

    return ret


@pytest.fixture(**_make_fixture_param_and_ids())
def config(request, monkeypatch):
    config = request.param(request)
    config.patch(request.node.name)
    return config


class NoopPgBackupStatements(object):
    @classmethod
    def run_start_backup(cls):
        name = '0' * 8 * 3
        offset = '0' * 8
        return {'file_name': name,
                'file_offset': offset,
                'pg_xlogfile_name_offset':
                'START-BACKUP-FAKE-XLOGPOS'}

    @classmethod
    def run_stop_backup(cls):
        name = '1' * 8 * 3
        offset = '1' * 8
        return {'file_name': name,
                'file_offset': offset,
                'pg_xlogfile_name_offset':
                'STOP-BACKUP-FAKE-XLOGPOS'}

    @classmethod
    def pg_version(cls):
        return {'version': 'FAKE-PG-VERSION'}


@pytest.fixture
def noop_pg_backup_statements(monkeypatch):
    import wal_e.operator.backup

    monkeypatch.setattr(wal_e.operator.backup, 'PgBackupStatements',
                        NoopPgBackupStatements)

    # psql binary test will fail if local pg env isn't set up
    monkeypatch.setattr(wal_e.cmd, 'external_program_check',
                        lambda *args, **kwargs: None)


@pytest.fixture
def small_push_dir(tmpdir):
    """Create a small pg data directory-alike"""
    contents = 'abcdefghijlmnopqrstuvwxyz\n' * 10000
    push_dir = tmpdir.join('push-from').ensure(dir=True)
    push_dir.join('arbitrary-file').write(contents)

    # Construct a symlink a non-existent path.  This provoked a crash
    # at one time.
    push_dir.join('pg_xlog').mksymlinkto('/tmp/wal-e-test-must-not-exist')

    # Holy crap, the tar segmentation code relies on the directory
    # containing files without a common prefix...the first character
    # of two files must be distinct!
    push_dir.join('holy-smokes').ensure()

    return push_dir
