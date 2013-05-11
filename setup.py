#!/usr/bin/env python
import errno
import os.path
import sys

from distutils.cmd import Command

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


# Utility function to read the contents of short files.
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

VERSION = read(os.path.join('wal_e', 'VERSION')).strip()

install_requires = ['gevent>=0.13.0', 'boto>=2.0']
extras_require = {
    'test':  ["pytest>=2.2.1", "pytest-xdist>=1.8", "pytest-capturelog>=0.7"]
    }

if sys.version_info < (2, 7):
    install_requires.append('argparse>=0.8')


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import subprocess

        try:
            retcode = subprocess.call('py.test')
        except EnvironmentError, e:
            if e.errno == errno.ENOENT:
                print >>sys.stderr, ('Could not find test runner, consider '
                                     '"pip install pytest pytest-xdist".')
        else:
            raise SystemExit(retcode)

setup(
    name="wal-e",
    version=VERSION,
    packages=find_packages(),

    install_requires=install_requires,
    extras_require=extras_require,

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
    cmdclass={'test': PyTest},

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
