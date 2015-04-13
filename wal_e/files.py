import os
import errno


class DeleteOnError(object):
    def __init__(self, where):
        self.where = where
        self.f = None
        self.remove_regardless = False

    def __enter__(self):
        self.f = open(self.where, 'wb')
        return self

    def __exit__(self, typ, value, traceback):
        try:
            if typ is not None or self.remove_regardless:
                os.unlink(self.where)
        except EnvironmentError as e:
            if e.errno != errno.ENOENT:
                raise
        finally:
            if self.f:
                self.f.close()
