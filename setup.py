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


# Utility function to read the contents of short files.
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

VERSION = read(os.path.join('wal_e', 'VERSION')).strip()

install_requires = [
    l for l in read('requirements.txt').split('\n')
    if l and not l.startswith('#')]

tests_require = [
    "pytest-capturelog>=0.7",
    "pytest>=2.2.1",
    "tox>=1.4.2",
]

if sys.version_info < (2, 7):
    install_requires.append('argparse>=0.8')

setup(
    name="wal-e",
    version=VERSION,
    packages=find_packages(),

    install_requires=install_requires,
    tests_require=tests_require,

    # metadata for upload to PyPI
    author="Daniel Farina",
    author_email="daniel@heroku.com",
    description="PostgreSQL WAL-shipping for S3",
    long_description=read('README.rst'),
    classifiers=['Topic :: Database',
                 'Topic :: System :: Archiving',
                 'Topic :: System :: Recovery Tools'],
    platforms=['any'],
    license="BSD",
    keywords=("postgres postgresql database backup archive "
              "archiving s3 aws wal shipping"),
    url="https://github.com/wal-e/wal-e",

    # Include the VERSION file
    package_data={'wal_e': ['VERSION']},

    # run tests
    test_suite='runtests.runtests',

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
