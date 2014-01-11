from functools import partial

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
                                'profile or set --aws-access-key-id')


Credentials = partial(DefaultS3Provider, "aws")


class InstanceProfileCredentials(DefaultS3Provider):
    def __init__(self):
        super(InstanceProfileCredentials, self).__init__(
            None, None, None)

    def get_provider(self):
        return InstanceProfileProvider('aws')
