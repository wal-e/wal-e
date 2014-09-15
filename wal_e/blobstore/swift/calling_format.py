import swiftclient


def connect(creds):
    """
    Construct a connection value from a container
    """
    return swiftclient.Connection(
        authurl=creds.authurl,
        user=creds.user,
        key=creds.password,
        auth_version="2",
        tenant_name=creds.tenant_name,
        os_options={
            "region_name": creds.region,
            "endpoint_type": creds.endpoint_type
        }
    )
