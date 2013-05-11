#!/usr/bin/env python
import os.path
import sys

# Version file managment scheme and graceful degredation for
# setuptools borrowed and adapted from GitPython.
try:
    from setuptools import setup, find_packages

    # Silence pyflakes
    assert setup
    assert find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

if sys.version_info < (2, 6):
    raise RuntimeError('Python versions < 2.6 are not supported.')

v = open(os.path.join(os.path.dirname(__file__), 'wal_e', 'VERSION'))
VERSION = v.readline().strip()
v.close()

install_requires = ['gevent>=0.13.0', 'boto>=2.0']
tests_require = [
    "pytest>=2.2.1",
    "pytest-capturelog>=0.7",
    "unittest2>=0.5",
]

if sys.version_info < (2, 7):
    install_requires.append('argparse>=0.8')

setup(
    name="WAL-E",
    version=VERSION,
    packages=find_packages(),

    install_requires=install_requires,
    tests_require=tests_require,

    # metadata for upload to PyPI
    author="Daniel Farina",
    author_email="daniel@heroku.com",
    description="PostgreSQL WAL-shipping for S3",
    license="BSD",
    keywords="postgresql database backup",
    url="https://github.com/wal-e/wal-e",

    # Include the VERSION file
    package_data={'wal_e': ['VERSION']},

    # run tests
    test_suite='runtests.runtests',

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
