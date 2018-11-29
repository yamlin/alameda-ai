try:
    import unittest
    from unittest.mock import patch, Mock
except ImportError:
    import unittest
    from mock import patch, Mock
import logging
import pickle
import tempfile
import sys
import os
import shutil
from framework.filesystem.fsal import FileSystem
from framework.datastore.file_dao import FileDataStore

class FileDAOTestCase(unittest.TestCase):
    def setUp(self):
        # logging.disable(logging.CRITICAL)  # suppress log
        self.file = "rand_test"
        self.fs = FileSystem()
        self.temp_dir = tempfile.mkdtemp()
        self.fs.impl.remote_root_dirpath = self.temp_dir

    def tearDown(self):
        logging.disable(logging.NOTSET)
        shutil.rmtree(self.temp_dir)

    def _get_pod_data(self):
        return [
            {"namespace": "ns2", "uid": "id1", 
             "name": "pod_name", "type": "POD"},
        ]

    def _get_non_existence_pod(self):
        return {"namespace": "empty", "uid": "id1", 
             "name": "pod_name", "type": "POD"}
    
    def test_create_pods(self):
        
        dao = FileDataStore(file=self.file, file_system=self.fs)
        
        pods = self._get_pod_data()
        res = dao.create_pod(pods)

        # # generate the pod keys
        expected_keys = map(
            lambda p: (p["namespace"], p["uid"], p["name"]), pods)
        result = dao.read_data()
        for key in expected_keys:
            self.assertIn(key, result)

    def test_delete_pods(self):
        # setup the pod data
        self.test_create_pods()

        dao = FileDataStore(file=self.file, file_system=self.fs)
        delete_pods = self._get_pod_data()
        dao.delete_pod(delete_pods)

        pods = dao.read_data()
        # expected empty dict
        self.assertTrue(not pods)
    
    def test_delete_not_exsistence_pods(self):
        # setup the pod data
        self.test_create_pods()
        dao = FileDataStore(file=self.file, file_system=self.fs)
        
        dao.delete_pod([self._get_non_existence_pod()])

        pods = dao.read_data()
        # expected some pod
        self.assertTrue(pods)

        # check the keys in our data
        expected_keys = map(
            lambda p: (p["namespace"], p["uid"], p["name"]), 
            self._get_pod_data()
        )
        for key in expected_keys:
            self.assertIn(key, pods)


if __name__ == "__main__":
    TEST_SUITE = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(TEST_SUITE)
