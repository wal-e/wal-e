import swiftclient


def connect(creds):
    """
    Construct a connection value from a container
    """
    return swiftclient.Connection(
        authurl=creds.authurl,
        user=creds.user,
        key=creds.password,
        auth_version=creds.auth_version,
        tenant_name=creds.tenant_name,
        os_options={
            "region_name": creds.region,
            "endpoint_type": creds.endpoint_type,
            "domain_id": creds.domain_id,
            "domain_name": creds.domain_name,
            "tenant_id": creds.tenant_id,
            "user_id": creds.user_id,
            "user_domain_id": creds.user_domain_id,
            "user_domain_name": creds.user_domain_name,
            "project_id": creds.project_id,
            "project_name": creds.project_name,
            "project_domain_id": creds.project_domain_id,
            "project_domain_name": creds.project_domain_name,
        }
    )
