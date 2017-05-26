import os
import shutil
from datetime import datetime


def remove_empty_dirs(path):
    """ removes empty dirs under a given path """
    for root, dirs, files in os.walk(path):
        for d in dirs:
            dir_path = os.path.join(root, d)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)


def ensure_dir_exists(path):
    """ create a directory if required """
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def common_dir_path(args, sep='/'):
    """ return the highest common directory given a list of files """
    return os.path.commonprefix(args).rpartition(sep)[0]


def epoch_to_iso8601(timestamp):
    return datetime.utcfromtimestamp(timestamp).isoformat()


class FileKey(object):
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.path = os.path.join("/", name.strip("/"))
        if os.path.isfile(self.path):
            stat = os.stat(self.path)
            self.last_modified = epoch_to_iso8601(stat.st_mtime)
            self.size = stat.st_size

    def get_contents_as_string(self):
        with open(self.path, 'rb') as fp:
            contents = fp.read()
        return contents

    def set_contents_from_file(self, fp):
        ensure_dir_exists(self.path)
        with open(self.path, 'wb') as f:
            shutil.copyfileobj(fp, f)
        setattr(self, 'size', os.path.getsize(self.path))

    def get_contents_to_file(self, fp):
        with open(self.path, 'rb') as f:
            shutil.copyfileobj(f, fp)


class Bucket(object):
    def __init__(self, name):
        self.name = name

    def get_key(self, name):
        return FileKey(bucket=self, name=name)

    def delete_keys(self, keys):
        for k in keys:
            key_path = os.path.join("/", k.strip("/"))
            os.remove(key_path)
        # deleting files can leave empty dirs => trim them
        common_path = os.path.join("/", common_dir_path(keys).strip("/"))
        remove_empty_dirs(common_path)

    def list(self, prefix):
        path = "/" + prefix
        file_paths = [os.path.join(root, f)
                      for root, dirs, files in os.walk(path) for f in files]
        # convert to an array of Keys
        return [FileKey(bucket=self, name=f) for f in file_paths]


class Connection(object):

    def get_bucket(self, name, validate=False):
        return Bucket(name)


def connect(creds):
    return Connection()
