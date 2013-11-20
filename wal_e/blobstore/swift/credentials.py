class Credentials(object):
    def __init__(self, authurl, user, password, tenant_name, region):
        self.authurl = authurl
        self.user = user
        self.password = password
        self.tenant_name = tenant_name
        self.region = region
