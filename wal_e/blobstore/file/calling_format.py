import os
import datetime


def remove_empty_dirs(path):
    """ removes empty dirs under a given path """
    for root, dirs, files in os.walk(path):
        for d in dirs:
            dir_path = os.path.join(root, d)
            if not os.listdir(dir_path):
                os.removedirs(dir_path)


def ensure_dir_exists(path):
    """ create a directory if required """
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def common_dir_path(args, sep='/'):
    """ return the highest common directory given a list of files """
    return os.path.commonprefix(args).rpartition(sep)[0]


class FileKey:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.path = os.path.normpath("/" + name)
        if os.path.isfile(self.path):
            stat = os.stat(self.path)
            self.last_modified = datetime.datetime.utcfromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%S")
            self.size = stat.st_size

    def get_contents_as_string(self):
        fp = open(self.path, 'r')
        contents = fp.read()
        fp.close()
        return contents

    def set_contents_from_file(self, fp):
        ensure_dir_exists(self.path)
        f = open(self.path, 'w')
        while True:
            piece = fp.read(1024)
            if not piece:
                break
            f.write(piece)
        f.close()
        setattr(self, 'size', os.path.getsize(self.path))

    def get_contents_to_file(self, fp):
        f = open(self.path, 'rb')
        while True:
            piece = f.read(1024)
            if not piece:
                break
            fp.write(piece)
        f.close()


class Bucket(object):
    def __init__(self, name):
        self.name = name

    def get_key(self, name):
        return FileKey(bucket=self, name=name)

    def delete_keys(self, keys):
        for k in keys:
            os.remove(k)
        # deleting files can leave empty dirs => trim them
        remove_empty_dirs(common_dir_path(keys))

    def list(self, prefix):
        # TODO: handle errors from missing path
        path = "/" + prefix
        file_paths = [os.path.join(root, f) for root, dirs, files in os.walk(path) for f in files]
        # convert to an array of Keys
        return [FileKey(bucket=self, name=f) for f in file_paths]


class Connection(object):

    def get_bucket(self, name, validate=False):
        return Bucket(name)


def connect(creds):
    return Connection()
