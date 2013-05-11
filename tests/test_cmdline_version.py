import subprocess

from os import path


def test_version_print():
    # Load up the contents of the VERSION file out-of-band
    from wal_e import cmd
    place = path.join(path.dirname(cmd.__file__), 'VERSION')
    with open(place) as f:
        expected = f.read()

    # Try loading it via command line invocation
    proc = subprocess.Popen(['wal-e', 'version'], stdout=subprocess.PIPE)
    result = proc.communicate()[0]

    # Make sure the two versions match and the command exits
    # successfully.
    assert proc.returncode == 0
    assert result == expected
