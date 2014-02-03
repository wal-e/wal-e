import os

from os import path


def _sync_file(thing, is_directory):
    mode = os.O_RDONLY

    if is_directory and hasattr(os, 'O_DIRECTORY'):
        mode |= os.O_DIRECTORY

    fd = os.open(thing, mode)
    os.fsync(fd)
    os.close(fd)


def recursive_fsync(root):
    def raise_walk_error(e):
        raise e

    walker = os.walk(root, onerror=raise_walk_error)

    for root, dirs, files in walker:
        for name in files:
            p = path.join(root, name)

            if path.islink(p):
                # Flushing the symlink's text appears unsupported by
                # any known platform.
                pass
            else:
                _sync_file(p, False)

        for name in dirs:
            _sync_file(path.join(root, name), True)
