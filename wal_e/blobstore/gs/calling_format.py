from gcloud import storage


def connect(creds):
    """
    Construct a connection value to Google Storage API
    """
    return storage.Client()
