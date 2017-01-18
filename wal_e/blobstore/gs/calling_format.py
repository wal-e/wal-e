from google.cloud.storage._http import Connection
from google.cloud.credentials import get_credentials
from google.cloud import storage
from google.auth.credentials import with_scopes_if_required
import google_auth_httplib2

from gevent.local import local


def connect(creds):
    """Construct a connection value to Google Storage API

    The credentials are retrieved using get_credentials that checks
    the environment for the correct values.

    """
    credentials = get_credentials()
    return storage.Client(credentials=credentials,
                          http=ThreadSafeHttp(credentials))


class ThreadSafeHttp(object):

    __scoped_credentials = None
    __local = local()

    def __init__(self, creds):
        self.__scoped_credentials = with_scopes_if_required(
            creds, Connection.SCOPE)

    def __getattr__(self, name):
        if not hasattr(self.__local, 'http'):
            self.__local.http = google_auth_httplib2.AuthorizedHttp(
                self.__scoped_credentials)

        return getattr(self.__local.http, name)
