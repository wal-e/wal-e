import google.auth
from google.cloud import storage
from requests.adapters import HTTPAdapter


def connect(creds, max_retries=100):
    """Construct a connection value to Google Storage API

    The credentials are retrieved using get_credentials that checks
    the environment for the correct values.

    """
    credentials, project = google.auth.default()
    return RetryClient(max_retries=max_retries, project=project,
                       credentials=credentials)


# No longer have to worry about thread safety because urllib3 and requests
# is used instead of httplib2, however, we still need to handle retries.
#
# While the new google-cloud library has a Retries strategy, it only
# handles status codes and not exceptions (like SSL errors).  urllib3
# does handle these, but requires a parameter to be passed into the adapter.
# Here we just hook setattr and when the http object is being set, we
# overmount the handlers.


class RetryClient(storage.Client):
    _max_retries = 100

    def __init__(self, max_retries=100, **kwargs):
        self._max_retries = max_retries
        super(RetryClient, self).__init__(**kwargs)

    def __setattr__(self, name, value):
        if name == '_http_internal' and value is not None:
            # Lets set the maxretries on the sesion as well, which
            #  passes to urllib3 and can handle SSL errors.
            value.mount('http://', HTTPAdapter(max_retries=self._max_retries))
            value.mount('https://', HTTPAdapter(max_retries=self._max_retries))
        super(RetryClient, self).__setattr__(name, value)
