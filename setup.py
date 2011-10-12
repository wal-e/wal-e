#!/usr/bin/env python
import os.path
import sys

# Version file managment scheme and graceful degredation for
# setuptools borrowed and adapted from GitPython.
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

if sys.version_info < (2, 6):
    raise RuntimeError('Python versions < 2.6 are not supported.')

v = open(os.path.join(os.path.dirname(__file__), 'VERSION'))
VERSION = v.readline().strip()
v.close()

install_requires = ['gevent>=0.13.0', 'boto>=2.0']

if sys.version_info < (2, 7):
    install_requires.append('argparse>=0.8')

setup(
    name="WAL-E",
    version=VERSION,
    packages=find_packages(),

    install_requires=install_requires,

    # metadata for upload to PyPI
    author="Daniel Farina",
    author_email="daniel@heroku.com",
    description="PostgreSQL WAL-shipping for S3",
    license="BSD",
    keywords="postgresql database backup",
    url="https://github.com/heroku/wal-e",

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
