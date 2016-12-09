from subprocess import PIPE
import os
from wal_e.piper import popen_sp

CONTROLDATA_BIN = 'pg_controldata'
CONFIG_BIN = 'pg_config'


class PgControlDataParser(object):
    """
    When we're backing up a PG cluster that is not
    running, we can't query it for information like
    the current restartpoint's WAL index,
    the current PG version, etc.

    Fortunately, we can use pg_controldata, which
    provides this information and doesn't require
    a running PG process
    """

    def __init__(self, data_directory):
        self.data_directory = data_directory
        pg_config_proc = popen_sp([CONFIG_BIN],
                             stdout=PIPE)
        output = pg_config_proc.communicate()[0].decode('utf-8')
        for line in output.split('\n'):
            parts = line.split('=')
            if len(parts) != 2:
                continue
            key, val = [x.strip() for x in parts]
            if key == 'BINDIR':
                self._controldata_bin = os.path.join(val, CONTROLDATA_BIN)
            elif key == 'VERSION':
                self._pg_version = val

    def _read_controldata(self):
        controldata_proc = popen_sp(
            [self._controldata_bin, self.data_directory], stdout=PIPE)
        stdout = controldata_proc.communicate()[0].decode('utf-8')
        controldata = {}
        for line in stdout.split('\n'):
            split_values = line.split(':')
            if len(split_values) == 2:
                key, val = split_values
                controldata[key.strip()] = val.strip()
        return controldata

    def controldata_bin(self):
        return self._controldata_bin

    def pg_version(self):
        return self._pg_version

    def last_xlog_file_name_and_offset(self):
        controldata = self._read_controldata()
        last_checkpoint_offset = \
            controldata["Latest checkpoint's REDO location"]
        current_timeline = controldata["Latest checkpoint's TimeLineID"]
        x, offset = last_checkpoint_offset.split('/')
        timeline = current_timeline.zfill(8)
        wal = x.zfill(8)
        offset = offset[0:2].zfill(8)
        return {
            'file_name': ''.join([timeline, wal, offset]),
            'file_offset': offset.zfill(8)}
