class Credentials(object):
    def __init__(self, authurl, user, password, tenant_name, region,
            endpoint_type, auth_version, domain_id, domain_name, tenant_id,
            user_id, user_domain_id, user_domain_name, project_id,
            project_name, project_domain_id, project_domain_name):
        self.authurl = authurl
        self.user = user
        self.password = password
        self.tenant_name = tenant_name
        self.region = region
        self.endpoint_type = endpoint_type
        self.auth_version = auth_version
        self.domain_id = domain_id
        self.domain_name = domain_name
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.user_domain_id = user_domain_id
        self.user_domain_name = user_domain_name
        self.project_id = project_id
        self.project_name = project_name
        self.project_domain_id = project_domain_id
        self.project_domain_name = project_domain_name
