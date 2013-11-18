class Credentials(object):
    def __init__(self, access_key_id, secret_access_key, security_token=None):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.security_token = security_token
