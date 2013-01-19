#!/usr/bin/env python
import sys
from distutils.spawn import find_executable

EXTERNAL_DEPENDENCIES = ['lzop', 'psql', 'pv']


def runtests(args=None):
    import pytest

    for dep in EXTERNAL_DEPENDENCIES:
        if not find_executable(dep):
            print >>sys.stderr, 'FATAL: Missing binary for dependency "%s".' % dep
            sys.exit(1)

    if args is None:
        args = []

    sys.exit(pytest.main(args))


if __name__ == '__main__':
    runtests(sys.argv)
