'''Unit test for Logger class.'''

import os
import unittest
from framework.log.logger import Logger, LogLevel


class LoggerTestCase(unittest.TestCase):
    '''Unit test for Logger class.'''

    UNITTEST_LOGPATH = "./unittest.log"
    TEST_MSG = "This is unittest for logger module."

    def setUp(self):
        '''Setup unittest environment.'''

        self.testitem = Logger('unittest', self.UNITTEST_LOGPATH)

    def tearDown(self):
        '''Clean unittest environment.'''

        os.remove(self.UNITTEST_LOGPATH)

    def __clear_logfile(self):
        '''Clear log file.'''

        open(self.UNITTEST_LOGPATH, 'w').close()

    def __has_log(self, logmsg, loglevel=None):
        '''Check if log message is in the logfile.

        Args:
            loglevel(str): Log level string, ex: 'DEBUG', 'INFO', etc.
        '''

        with open(self.UNITTEST_LOGPATH, 'r') as f_test:
            content = f_test.read()

        if loglevel is None:
            return logmsg in content

        return logmsg in content and loglevel in content

    def test_set_level(self):
        '''Test on set_level() function.

        Test target:
            Test if log level is set.
        '''

        # Log level is used to filter out low-level logs, so here we test on
        # calling log function with the same log level.
        visible_log = "Visible log"
        invisible_log = "Invisible log"

        self.testitem.set_level(LogLevel.LV_DEBUG)
        self.testitem.debug(visible_log)
        self.testitem.set_level(LogLevel.LV_INFO)
        self.testitem.debug(invisible_log)

        self.assertTrue(self.__has_log(visible_log, 'DEBUG'))
        self.assertFalse(self.__has_log(invisible_log, 'DEBUG'))

    def test_debug(self):
        '''Test on debug() function.

        Test target:
            Test if log is written to assigned log file.
        '''

        self.testitem.set_level(LogLevel.LV_DEBUG)
        self.testitem.debug(self.TEST_MSG)
        self.assertTrue(self.__has_log(self.TEST_MSG, 'DEBUG'))

if __name__ == '__main__':
    unittest.main(verbosity=2)
