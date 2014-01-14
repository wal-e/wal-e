import os
import pytest
import s3_integration_help
import sys

from wal_e import cmd

_PREFIX_VARS = ['WALE_S3_PREFIX', 'WALE_WABS_PREFIX', 'WALE_SWIFT_PREFIX']


class AwsTestConfig(object):
    def __init__(self):
        self.env_vars = {}
        relevant_env_vars = ['AWS_ACCESS_KEY_ID',
                             'AWS_SECRET_ACCESS_KEY',
                             'AWS_SECURITY_TOKEN']

        for name in relevant_env_vars:
            maybe_value = os.getenv(name)
            self.env_vars[name] = maybe_value

    def patch(self, test_name, default_test_bucket, monkeypatch):
        # Scrub WAL-E prefixes left around in the user's environment to
        # prevent unexpected results.
        for name in _PREFIX_VARS:
            monkeypatch.delenv(name, raising=False)

        # Set other credentials.
        for name, value in self.env_vars.iteritems():
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)

        monkeypatch.setenv('WALE_S3_PREFIX', 's3://{0}/{1}'
                           .format(default_test_bucket, test_name))

    @property
    def name(self):
        return 'aws'


def _make_fixture_param_and_ids():
    ret = {
        'params': [],
        'ids': [],
    }

    def _add_config(c):
        ret['params'].append(c)
        ret['ids'].append(c.name)

    if not s3_integration_help.no_real_s3_credentials():
        _add_config(AwsTestConfig())

    return ret


@pytest.fixture(autouse=True,
                **_make_fixture_param_and_ids())
def apply_blackbox_config(request, monkeypatch, default_test_bucket):
    request.param.patch(request.node.name, default_test_bucket, monkeypatch)


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


class MainWithArgs(object):
    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def __call__(self, *argv):
        self.monkeypatch.setattr(sys, 'argv', ['wal-e'] + list(argv))
        cmd.main()


@pytest.fixture
def main(monkeypatch):
    return MainWithArgs(monkeypatch)
