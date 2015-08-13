from gcloud.storage.connection import Connection
from gcloud.credentials import get_credentials
from gcloud import storage

from gevent.local import local
from httplib2 import Http


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
        self.__scoped_credentials = Connection._create_scoped_credentials(
            creds, Connection.SCOPE)

    def __getattr__(self, name):
        if not hasattr(self.__local, 'http'):
            self.__local.http = self.__scoped_credentials.authorize(Http())

        return getattr(self.__local.http, name)
