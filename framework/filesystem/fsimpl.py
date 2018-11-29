'''Abstract class for all filesystem implementation.'''

import abc


class FileSystemImpl:
    '''Abstract class for all filesystem implementation.'''

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def is_file(self, filename):
        '''Check if file exists or not.

        Args:
            filename(str): Target file name.

        Returns:
            bool: Returns True if the target file exists. Otherwise, return
                  False.
        '''

        pass

    @abc.abstractmethod
    def get_file(self, filename, local_filepath, overwrite=False, **kwargs):
        '''Get file using filename.

        Args:
            filename(str): Target file name.
            local_filepath(str): Local filepath for caller.
            overwrite(bool): If local file exists, overwrite it.
            kwargs(dict): Additional arguments.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        pass

    @abc.abstractmethod
    def save_file(self, filename, local_filepath, **kwargs):
        '''Save a local file with its local path.

        If remote file exists, please use update_file() instead.

        Args:
            filename(str): File name to be saved.
            local_filepath(str): Local filepath.
            kwargs(dict): Additional arguments.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        pass

    @abc.abstractmethod
    def update_file(self, filename, local_filepath, **kwargs):
        '''Update existing file use local file.

        Args:
            filename(str): File name to be updated.
            local_filepath(str): Local filepath.
            kwargs(dict): Additional arguments.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        pass
