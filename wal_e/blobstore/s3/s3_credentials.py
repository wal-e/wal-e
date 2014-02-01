from boto import provider
from functools import partial
from wal_e.exception import UserException


class InstanceProfileProvider(provider.Provider):
    """Override boto Provider to control use of the AWS metadata store

    In particular, prevent boto from looking in a series of places for
    keys outside off WAL-E's control (e.g. boto.cfg, environment
    variables, and so on).  As-is that precedence and detection code
    is in one big ream, and so a method override and some internal
    symbols are used to excise most of that cleverness.

    Also take this opportunity to inject a WAL-E-friendly exception to
    help the user with missing keys.

    """

    def get_credentials(self, access_key=None, secret_key=None,
                        security_token=None, profile_name=None):
        if self.MetadataServiceSupport[self.name]:
            self._populate_keys_from_metadata_server()

        if not self._secret_key:
            raise UserException('Could not retrieve secret key from instance '
                                'profile.',
                                hint='Check that your instance has an IAM '
                                'profile or set --aws-access-key-id')


Credentials = partial(provider.Provider, "aws")
InstanceProfileCredentials = partial(InstanceProfileProvider, 'aws')
