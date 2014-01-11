from boto.provider import Provider as DefaultS3Provider

from wal_e.exception import UserException


class InstanceProfileProvider(DefaultS3Provider):
    """Provides the S3Connection class with credentials discovered
    through the aws metadata store.

    """

    def get_credentials(self, access_key=None, secret_key=None,
                        security_token=None):
        if self.MetadataServiceSupport[self.name]:
            self._populate_keys_from_metadata_server()

        if not self._secret_key:
            raise UserException('Could not retrieve secret key from instance '
                                'profile.',
                                hint='Check that your instance has an IAM '
                                'profile or set --aws-acces-key-id')


class Credentials(object):
    def __init__(self, access_key_id, secret_access_key, security_token=None):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.security_token = security_token

    def get_provider(self):
        return DefaultS3Provider('aws', self.access_key_id,
                                 self.secret_access_key, self.security_token)


class InstanceProfileCredentials(Credentials):
    def __init__(self):
        super(InstanceProfileCredentials, self).__init__(
            None, None, None)

    def get_provider(self):
        return InstanceProfileProvider('aws')
