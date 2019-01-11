'''Log wrapper for all services.'''

import os
import logging
from framework.log.logfile_handler import LogFileHandler


class LogLevel:  # pylint: disable=R0903
    '''LogLevel class.'''

    LV_DEBUG = 0
    LV_INFO = 1
    LV_WARN = 2
    LV_ERROR = 3
    LV_CRITICAL = 4


class Logger:
    '''Log wrapper for all services.'''

    DEFAULT_NAME = 'alameda-ai'

    def __init__(self, name='alameda-ai', logfile='/var/log/alameda-ai.log',
                 level=LogLevel.LV_INFO):

        self.logger = logging.getLogger(name)
        self.logger.propagate = False
        self.logger.setLevel(Logger.__get_logging_level(level))

        # Check permission of the given logfile:
        try:
            open(logfile, 'a').close()
        except OSError:
            logfile = os.path.join(os.environ['PYTHONPATH'],
                                   os.path.basename(logfile))

        # Set file and stream handlers
        self.logger.handlers = []
        self.file_handler = LogFileHandler(name, logfile)
        self.file_handler.setup()
        self.logger.addHandler(self.file_handler)

        stream_handler = logging.StreamHandler()
        self.logger.addHandler(stream_handler)

        self.file_handler.flush()

    def __del__(self):

        self.file_handler.flush()
        self.logger.handlers[0].close()
        self.logger.handlers = []

    @staticmethod
    def __get_logging_level(level):
        '''Get log level defined in logging package.

        Args:
            level(LogLevel): Log level defined in this module.

        Returns:
            int: logging levels
        '''

        if level == LogLevel.LV_DEBUG:
            return logging.DEBUG
        if level == LogLevel.LV_INFO:
            return logging.INFO
        if level == LogLevel.LV_WARN:
            return logging.WARNING
        if level == LogLevel.LV_ERROR:
            return logging.ERROR
        if level == LogLevel.LV_CRITICAL:
            return logging.CRITICAL

        return logging.INFO

    def set_level(self, level=LogLevel.LV_INFO):
        '''Set log level on the fly.

        Args:
            level(LogLevel): Log level defined in this module.

        Returns: None
        '''

        self.logger.setLevel(Logger.__get_logging_level(level))

    def debug(self, msg, *args, **kwargs):
        '''Write debug log.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns: None
        '''

        self.logger.debug(msg, *args, **kwargs)
        self.file_handler.flush()

    def info(self, msg, *args, **kwargs):
        '''Write info log.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns: None
        '''

        self.logger.info(msg, *args, **kwargs)
        self.file_handler.flush()

    def warning(self, msg, *args, **kwargs):
        '''Write warning log.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns: None
        '''

        self.logger.warning(msg, *args, **kwargs)
        self.file_handler.flush()

    def error(self, msg, *args, **kwargs):
        '''Write error log.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns: None
        '''

        self.logger.error(msg, *args, **kwargs)
        self.file_handler.flush()

    def critical(self, msg, *args, **kwargs):
        '''Write critical log.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns: None
        '''

        self.logger.critical(msg, *args, **kwargs)
        self.file_handler.flush()
