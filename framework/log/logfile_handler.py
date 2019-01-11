'''Concurrent file handler for log files.'''

import os
import logging
from filelock import FileLock, Timeout


class LogFileHandler(logging.FileHandler):
    '''Concurrent file handler for log files.'''

    def __init__(self, tag, filepath, mode='a', encoding=None, delay=False):  # pylint: disable=R0913
        super().__init__(filepath, mode, encoding, delay)

        self.tag = tag
        self.filepath = os.path.normpath(os.path.abspath(filepath))
        self.filelock = None

    def __del__(self):

        if self.filelock is not None:
            lockpath = self.filelock.lock_file
            self.filelock.release(force=True)
            self.filelock = None
            # os.remove(lockpath)

    def setup(self):
        '''Setup for custom log module.

        Args: None
        Returns: None
        '''

        filename = os.path.basename(self.filepath)
        dirpath = os.path.dirname(self.filepath)

        self.filelock = FileLock(os.path.join(dirpath, filename + '.lock'))

        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s ' + self.tag + ' %(message)s')
        super().setFormatter(formatter)

    def emit(self, record):
        '''Outputs the record to the file.

        Args:
            record: Log message.

        Returns: None
        '''

        try:
            self.filelock.acquire(timeout=1)
            super().emit(record)
            self.filelock.release(force=True)

        except Timeout:
            pass  # Nothing we can do..
