from wal_e.blobstore.s3.instance_profile_provider import InstanceProfileProvider
from boto.provider import Provider as DefaultS3Provider


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
