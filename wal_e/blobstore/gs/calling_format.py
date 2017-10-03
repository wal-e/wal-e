from google.cloud import storage

def connect(creds):
    """Construct a connection value to Google Storage API

    The credentials are retrieved using the default method which inspects the
    GOOGLE_APPLICATION_CREDENTIALS environment variable.

    """
    return storage.Client()
