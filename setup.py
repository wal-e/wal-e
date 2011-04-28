#!/usr/bin/env python

import os.path

# Version file managment scheme and graceful degredation for
# setuptools borrowed and adapted from GitPython.
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

v = open(os.path.join(os.path.dirname(__file__), 'VERSION'))
VERSION = v.readline().strip()
v.close()

import sys

if sys.version_info < (2, 7):
    install_requires = ['argparse>=0.8']
else:
    install_requires = []

setup(
    name = "WAL-E",
    version = VERSION,
    packages = find_packages(),

    install_requires = install_requires,

    # metadata for upload to PyPI
    author = "Daniel Farina",
    author_email = "daniel@heroku.com",
    description = "PostgreSQL WAL-shipping for S3",
    license = "BSD",
    keywords = "postgresql database backup",
    url = "https://github.com/heroku/wal-e",

    # install 
    entry_points = {'console_scripts': ['wal-e = wal_e.cmd:main']}
)
