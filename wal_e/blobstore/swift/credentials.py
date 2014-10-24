class Credentials(object):
    def __init__(self, authurl, user, password, tenant_name, region,
            endpoint_type, auth_version):
        self.authurl = authurl
        self.user = user
        self.password = password
        self.tenant_name = tenant_name
        self.region = region
        self.endpoint_type = endpoint_type
        self.auth_version = auth_version
