''' A simple data file store. '''

import logging
import os
import pickle
import tempfile

from framework.filesystem.fsal import FileSystem, FileSystemType

class DefaultFileDecoder(object):
    # pylint: disable=no-self-use, invalid-name, broad-except
    ''' The default file decode '''
    def load(self, file):
        ''' load the data file '''
        with open(file, "rb") as f:
            return pickle.load(f)

    def dump(self, data, file):
        ''' Dump the data to file '''
        try:
            with open(file, "wb") as f:
                pickle.dump(data, f)
                return True, None
        except Exception as e:
            logging.error("Fail to dump data to %s", file)
            return False, e.with_traceback


class FileDataStore(object):
    # pylint: disable=no-self-use, invalid-name, broad-except
    ''' A simple data file store '''

    __DEFALUT_DIR = "/alameda-ai/.fs/"
    __DEFALUT_FILE = "pods_file"

    def __init__(self, file=None, file_system=None, decoder=None):
        ''' Initial method
        Args:
            file: the file name to store pod info.
            file_system: file system

        Return:
            None
        '''
        if not file_system:
            file_system = FileSystem(fstype=FileSystemType.FSTYPE_LOCAL)
            file_system.impl.remote_root_dirpath = self.__DEFALUT_DIR

        self.file_system = file_system
        self.file = file or self.__DEFALUT_FILE
        self.decoder = decoder or DefaultFileDecoder()

        # initialize the file
        if not self.file_system.is_file(self.file):
            local_file = self.__write_random_file({})
            res = self.file_system.save_file(self.file, local_file)
            if not res[0]:
                logging.error("Error: %s", res[1])
                raise Exception("Error to initial file: ", res[1])
            # remove the tmp file
            os.remove(local_file)

    def __write_random_file(self, data):
        ''' Write the data to a tmporary file.
        Args:
            data: the content to be written

        Returns:
            str: the file name.
        '''
        file = tempfile.NamedTemporaryFile(delete=False)
        self.decoder.dump(data, file.name)
        return file.name

    def __get_random_file(self):
        return tempfile.NamedTemporaryFile().name

    def read_data(self):
        ''' Read the pickle file.
        Args:
            None

        Returns:
            dict: the pod data.
        '''
        try:
            local_file = self.__get_random_file()
            res = self.file_system.get_file(self.file, local_file)
            if not res[0]:
                logging.error("Could not get the data file: %s", self.file)

            data = self.decoder.load(local_file)
            return data
        except Exception as e:
            logging.error("Load data file error: %s", str(e))
            return {}

    def create_pod(self, pods):
        '''Add the pods.

        Args:
            pods: The list of pods.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        data = self.read_data()
        for p in pods:
            t = (p['namespace'], p['uid'], p['name'])
            data[t] = {"type": p['type']}

        local_file = self.__write_random_file(data)
        res = self.file_system.update_file(self.file, local_file)
        # remove the tmp file
        os.remove(local_file)
        return res

    def delete_pod(self, pods):
        '''Delete the pods.

        Args:
            pods: The list of pods.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        data = self.read_data()
        for p in pods:
            t = (p['namespace'], p['uid'], p['name'])
            if t in data:
                del data[t]

        local_file = self.__write_random_file(data)
        res = self.file_system.update_file(self.file, local_file)
        # remove the tmp file
        os.remove(local_file)
        return res
