from boto import provider
from functools import partial

Credentials = partial(provider.Provider, "aws")
