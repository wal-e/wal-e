import boto


def connect(creds):
    """
    Construct a connection value to Google Storage API
    """

    return boto.connect_gs(creds.access_key, creds.secret_key)
