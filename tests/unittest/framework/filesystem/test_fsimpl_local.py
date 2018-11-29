'''Unit test for FileSystemImplLocal class.'''

import os
import shutil
import unittest
from framework.filesystem.fsal import FileSystem, FileSystemType


class FileSystemImplLocalTestCase(unittest.TestCase):
    '''Unit test for FileSystemImplLocal class.'''

    TEST_MSG = "Unittest for FileSystemImplLocal"

    def setUp(self):
        '''Setup unittest environment.'''

        self.create_dir = False
        self.test_remote_root_dirpath = "./test_ori_fs"
        self.test_filename = "test.txt"
        self.test_filepath = os.path.join(self.test_remote_root_dirpath,
                                          self.test_filename)

        self.testitem = FileSystem(fstype=FileSystemType.FSTYPE_LOCAL)
        self.testitem.impl.remote_root_dirpath = self.test_remote_root_dirpath

        if os.path.exists(self.test_remote_root_dirpath):
            raise Exception("Directory for unittest already exists.")

        os.makedirs(self.test_remote_root_dirpath)

        with open(self.test_filepath, "w") as f_test:
            f_test.write(self.TEST_MSG)

    def tearDown(self):
        '''Clean unittest environment.'''

        shutil.rmtree(self.test_remote_root_dirpath)

    def test_is_file(self):
        '''Test is_file() function.

        Test target:
            Check on an existing file and a non-existing file.
        '''

        existing_file = self.test_filename
        existing_filepath = self.test_filepath
        nonexisting_file = self.test_filename + ".abc"
        nonexisting_filepath = self.test_filepath + ".abc"

        self.assertTrue(os.path.isfile(existing_filepath))
        self.assertTrue(self.testitem.is_file(existing_file))
        self.assertFalse(os.path.isfile(nonexisting_filepath))
        self.assertFalse(self.testitem.is_file(nonexisting_file))

    def test_get_file(self):
        '''Test get_file() function.

        Test target:
            Get file and read content correctly.
        '''

        local_filepath = './test_get_file.txt'
        self.testitem.get_file(self.test_filename, local_filepath)

        realmsg = ''
        with open(local_filepath, "r") as f_test:
            realmsg = f_test.read()

        self.assertEqual(self.TEST_MSG, realmsg)
        os.remove(local_filepath)

    def test_save_file_1(self):
        '''Test save_file() function.

        Test target:
            Missing local file.
        '''

        missing_file = "/test_save_file_1.txt"
        self.assertFalse(self.testitem.save_file(
            self.test_filename, missing_file)[0])

    def test_save_file_2(self):
        '''Test save_file() function.

        Test target:
            Remote file exists.
        '''

        existing_filepath = "./test_save_file_2.txt"
        open(existing_filepath, 'a').close()
        self.assertFalse(self.testitem.save_file(
            self.test_filename, existing_filepath)[0])
        os.remove(existing_filepath)

    def test_save_file_3(self):
        '''Test save_file() function.

        Test target:
            Local file is saved with the same content.
        '''

        # Save file as normal workflow
        local_filepath = "./test_save_file_3.txt"
        with open(local_filepath, 'w') as f_test:
            f_test.write(self.TEST_MSG)

        remote_filename = "test_save_file_3.txt"
        self.assertTrue(self.testitem.save_file(
            remote_filename, local_filepath)[0])

        # Check status
        remote_filepath = os.path.join(self.test_remote_root_dirpath,
                                       remote_filename)
        self.assertTrue(os.path.isfile(remote_filepath))

        with open(remote_filepath, 'r') as f_test:
            content = f_test.read()
        self.assertEqual(self.TEST_MSG, content)

        # Remove testing file
        os.remove(remote_filepath)
        os.remove(local_filepath)

    def test_update_file_1(self):
        '''Test update_file() function.

        Test target:
            Missing local file.
        '''

        missing_file = "/test_update_file_1.txt"
        self.assertFalse(self.testitem.update_file(
            self.test_filename, missing_file)[0])

    def test_update_file_2(self):
        '''Test update_file() function.

        Test target:
            Missing remote file.
        '''

        # Existing local file
        existing_local_filepath = "./test_update_file_2.txt"
        open(existing_local_filepath, 'a').close()

        # Non-existing remote file
        missing_remote_filename = "test_update_file_2.txt"
        missing_remote_filepath = os.path.join(
            self.test_remote_root_dirpath, missing_remote_filename)
        self.assertFalse(os.path.isfile(missing_remote_filepath))

        # Test
        self.assertFalse(self.testitem.update_file(
            missing_remote_filename, existing_local_filepath)[0])

        # Clear testing files
        os.remove(existing_local_filepath)

    def test_update_file_3(self):
        '''Test update_file() function.

        Test target:
            Local file is the remote file.
        '''

        # Check file exists.
        self.assertTrue(os.path.isfile(self.test_filepath))

        # Write new_msg.
        new_msg = "TEST_UPDATE_FILE_3"
        with open(self.test_filepath, 'a') as f_test:
            f_test.write(new_msg)

        # Check new_msg is written into the target file.
        with open(self.test_filepath, 'r') as f_test:
            content = f_test.read()
            self.assertTrue(new_msg in content)

        # Test
        self.assertFalse(self.testitem.update_file(
            self.test_filename, self.test_filepath)[0])

    def test_update_file_4(self):
        '''Test update_file() function.

        Test target:
            Local file is not the remote file.
        '''

        msg = "TEST_UPDATE_FILE_4"

        # Create new local file
        new_local_filepath = "./test_update_file_4.txt"
        self.assertFalse(os.path.isfile(new_local_filepath))

        with open(new_local_filepath, 'w') as f_test:
            f_test.write(msg)
        self.assertTrue(os.path.isfile(new_local_filepath))

        # Check remote file exists
        self.assertTrue(os.path.isfile(self.test_filepath))

        # Update new local file to existing remote file
        self.assertTrue(self.testitem.update_file(
            self.test_filename, new_local_filepath)[0])

        # Check if content of local and remote files are the same
        with open(self.test_filepath, 'r') as f_test:
            remote_content = f_test.read()

        self.assertEqual(remote_content, msg)

        # Clear testing files
        os.remove(new_local_filepath)


if __name__ == '__main__':
    unittest.main(verbosity=2)
