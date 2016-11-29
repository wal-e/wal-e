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

if sys.version_info < (3, 4):
    raise RuntimeError('Python versions < 3.4 are not supported.')


# Utility function to read the contents of short files.
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


VERSION = read(os.path.join('wal_e', 'VERSION')).strip()

setup(
    name="wal-e",
    version=VERSION,
    packages=find_packages(),

    install_requires=['gevent>=1.1.1'],
    extras_require={
        'aws': ['boto>=2.40.0'],
        'azure': ['azure>=1.0.3'],
        'google': ['gcloud>=0.17.0'],
        'swift': ['python-swiftclient>=3.0.0',
                  'python-keystoneclient>=3.0.0']
    },

    # metadata for upload to PyPI
    author="The WAL-E Contributors",
    author_email="wal-e@googlegroups.com",
    maintainer="Daniel Farina",
    maintainer_email="daniel@fdr.io",
    description="Continuous Archiving for Postgres",
    long_description=read('README.rst'),
    classifiers=[
        'Topic :: Database',
        'Topic :: System :: Archiving',
        'Topic :: System :: Recovery Tools',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    platforms=['any'],
    license="BSD",
    keywords=("postgres postgresql database backup archive archiving s3 aws "
              "openstack swift wabs azure google gce gcs wal shipping"),
    url="https://github.com/wal-e/wal-e",

    # Include the VERSION file
    package_data={'wal_e': ['VERSION']},

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
