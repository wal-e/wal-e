class Credentials(object):
    def __init__(self, authurl, user, password, tenant_name, region,
            endpoint_type, auth_version, domain_id, tenant_id, user_id,
            user_domain_id):
        self.authurl = authurl
        self.user = user
        self.password = password
        self.tenant_name = tenant_name
        self.region = region
        self.endpoint_type = endpoint_type
        self.auth_version = auth_version
        self.domain_id = domain_id
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.user_domain_id = user_domain_id
