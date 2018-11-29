'''File system class to abstract file operations.

FSAL stands for File System Abstraction Layer.
'''

from framework.filesystem.fsimpl_local import FileSystemImplLocal


class FileSystemType:  # pylint: disable=too-few-public-methods
    '''File system types for FileSystem class.'''

    FSTYPE_LOCAL = 0


class FileSystem:
    '''File system class to abstract file operations.

    Here we use bridge design pattern to isolate implementations.
    '''

    def __init__(self, fstype=FileSystemType.FSTYPE_LOCAL):  # pylint: disable=W0613
        '''Setup file system by its type.

        According to its filesystem type, corresponding implementation will be
        linked to this class.

        Args:
            fstype(FileSystemType): File system type.
        '''

        self.impl = FileSystemImplLocal()

    def is_file(self, filename):
        '''Check if file exists or not.

        Args:
            filename(str): Target file name.

        Returns:
            bool: Returns True if the target file exists. Otherwise, return
                  False.
        '''

        return self.impl.is_file(filename)

    def get_file(self, filename, local_filepath, overwrite=False, **kwargs):
        '''Get a file using its filename.

        File will be copied to given local filepath. So be sure that your
        'local_filepath' can be overwrite.

        Args:
            filename(str): Target file name.
            local_filepath(str): Local filepath for caller.
            overwrite(bool): If local file exists, overwrite it.
            kwargs(dict): Additional arguments.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        return self.impl.get_file(filename, local_filepath, overwrite,
                                  **kwargs)

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

        return self.impl.save_file(filename, local_filepath, **kwargs)

    def update_file(self, filename, local_filepath, **kwargs):
        '''Update an existing file use local file.

        If remote file is missing, this operation is failed.

        Args:
            filename(str): File name to be updated.
            local_filepath(str): Local filepath.
            kwargs(dict): Additional arguments.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        return self.impl.update_file(filename, local_filepath, **kwargs)
