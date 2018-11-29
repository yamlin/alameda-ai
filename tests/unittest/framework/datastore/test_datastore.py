import logging
import os
import pickle
import shutil
import sys
import tempfile

from framework.datastore.file_dao import FileDataStore
from framework.datastore.influx_dao import InfluxDataStore

try:
    import unittest
    from unittest.mock import patch, Mock
except ImportError:
    import unittest
    from mock import patch, Mock


class FileDAOTestCase(unittest.TestCase):
    
    def setUp(self):
        logging.disable(logging.CRITICAL)  # suppress log
        self.fs = Mock()
        self.file = tempfile.NamedTemporaryFile()
        self.default_data = {("ns1", "id", "name"): "POD"}
        self.decoder = Mock()

        self.dao = FileDataStore(file=self.file.name, file_system=self.fs, decoder=self.decoder)
        # write the default data
        with open(self.file.name, "wb") as f:
            pickle.dump(self.default_data, f)


    def tearDown(self):
        logging.disable(logging.NOTSET)
        pass

    def _setup_read_data(self):
        self.fs.is_file.return_value = True
        self.fs.get_file.return_value = self.file.name, None

    def _get_pod_data(self):
        return [
            {"namespace": "ns2", "uid": "id1", 
             "name": "pod_name", "type": "POD"},
        ]

    def test_read_data_creation(self):
        self.decoder.load.return_value = self.default_data
        self._setup_read_data()
        data = self.dao.read_data()
        self.assertEqual(data, self.default_data, "The result is not as expectation")
        self.fs.get_file.assert_called_once()
        self.decoder.load.assert_called_once()

    def test_read_data_exception(self):
        self.decoder.load.return_value = self.default_data
        self.fs.is_file.return_value = True
        self.fs.get_file.side_effect = Exception("test")
        data = self.dao.read_data()    
        self.assertEqual(data, {}, "The result is not as expectation")
        self.fs.get_file.assert_called_once()
    
    def test_creat_pods(self):
        self._setup_read_data()
        self.decoder.load.return_value = self.default_data
        self.decoder.dump.return_value = None
        self.fs.update_file.return_value = True, ""
        res = self.dao.create_pod(self._get_pod_data())
        self.fs.update_file.assert_called_once()
        self.assertTrue(res[0])

    def test_delete_pods(self):
        self._setup_read_data() 
        self.fs.update_file.return_value = True, ""
        self.decoder.load.return_value = self.default_data
        self.decoder.dump.return_value = None
        res = self.dao.delete_pod(self._get_pod_data())
        self.fs.update_file.assert_called_once()
        self.assertTrue(res[0])


class InfluxDAOTestCase(unittest.TestCase):
    
    def setUp(self):
        logging.disable(logging.CRITICAL)  # suppress log
        self.client = Mock()

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def generate_disk_data(self):
        return {
            "id": 123,
            "name": "disk_name"
        }

    def generate_pod_data(self):
        return {
            "id": 123,
            "name": "pod_name"
        }

    def test_disk_creation(self):
        data = self.generate_disk_data()

        self.client.write_points.return_value = None
        dao = InfluxDataStore(self.client)
        result = dao.create_disk(data)

        self.assertIsNone(result, "Result is not None")
        self.client.write_points.assert_called_once()

    def test_disk_creation_exception(self):
        data = self.generate_disk_data()
        self.client.write_points.side_effect = Exception()
        dao = InfluxDataStore(self.client)
        with self.assertRaises(Exception):
            dao.create_disk(data)
        self.client.write_points.assert_called_once()

    def test_pod_creation(self):
        data = self.generate_pod_data()
        self.client.write_points.return_value = None
        dao = InfluxDataStore(self.client)
        result = dao.create_pod(data)

        self.assertIsNone(result, "Result is not None")
        self.client.write_points.assert_called_once()

    def test_pod_creation_exception(self):
        data = self.generate_disk_data()
        self.client.write_points.side_effect = Exception()
        dao = InfluxDataStore(self.client)
        with self.assertRaises(Exception):
            dao.create_pod(data)
        self.client.write_points.assert_called_once()

    def test_disk_deletion(self):
        disk_id = 1
        self.client.query.return_value = disk_id
        dao = InfluxDataStore(self.client)
        result = dao.delete_pod(disk_id)

        self.assertEqual(result, disk_id, "Result is not equal")
        self.client.query.assert_called_once()

    def test_pod_deletion(self):
        pod_id = 1
        self.client.query.return_value = pod_id
        dao = InfluxDataStore(self.client)
        result = dao.delete_pod(pod_id)

        self.assertEqual(result, pod_id, "Result is not equal")
        self.client.query.assert_called_once()


if __name__ == "__main__":
    TEST_SUITE = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(TEST_SUITE)
