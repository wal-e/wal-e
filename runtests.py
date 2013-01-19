#!/usr/bin/env python
import sys

from wal_e.cmd import external_program_check
from wal_e.pipeline import PV_BIN


def runtests(args=None):
    import pytest

    external_program_check([PV_BIN])

    if args is None:
        args = []

    sys.exit(pytest.main(args))


if __name__ == '__main__':
    runtests(sys.argv)
