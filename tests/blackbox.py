import os
import pytest
import s3_integration_help
import sys

from wal_e import cmd

_PREFIX_VARS = ['WALE_S3_PREFIX', 'WALE_WABS_PREFIX', 'WALE_SWIFT_PREFIX']

_AWS_CRED_ENV_VARS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                      'AWS_SECURITY_TOKEN']


class AwsTestConfig(object):
    name = 'aws'

    def __init__(self, request):
        self.env_vars = {}
        self.request = request
        self.monkeypatch = request.getfuncargvalue('monkeypatch')

        for name in _AWS_CRED_ENV_VARS:
            maybe_value = os.getenv(name)
            self.env_vars[name] = maybe_value

    def patch(self, test_name, default_test_bucket):
        # Scrub WAL-E prefixes left around in the user's environment to
        # prevent unexpected results.
        for name in _PREFIX_VARS:
            self.monkeypatch.delenv(name, raising=False)

        # Set other credentials.
        for name, value in self.env_vars.iteritems():
            if value is None:
                self.monkeypatch.delenv(name, raising=False)
            else:
                self.monkeypatch.setenv(name, value)

        self.monkeypatch.setenv('WALE_S3_PREFIX', 's3://{0}/{1}'
                                .format(default_test_bucket, test_name))

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

    return ret


@pytest.fixture(**_make_fixture_param_and_ids())
def config(request, monkeypatch, default_test_bucket):
    config = request.param(request)
    config.patch(request.node.name, default_test_bucket)
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
