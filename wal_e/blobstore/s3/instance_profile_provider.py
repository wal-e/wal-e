from boto import provider

from wal_e.exception import UserException


class InstanceProfileProvider(provider.Provider):
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
                                'profile or set --
