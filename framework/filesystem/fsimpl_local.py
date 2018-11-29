'''Implementation of local file system.'''

import os
import shutil
from filelock import FileLock, Timeout
from framework.filesystem.fsimpl import FileSystemImpl


class FileSystemImplLocal(FileSystemImpl):
    '''Implementation of local file system.'''

    def __init__(self, remote="/alameda-ai/.fs/"):

        self.remote_root_dirpath = os.path.realpath(remote)

    def __copyfile_locked(self, src, dst):
        '''Copy file from src to dst.

        Args:
            src(str): Source filepath.
            dst(str): Destination filepath.

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        # Create remote folder if it's missing.
        if not os.path.isdir(self.remote_root_dirpath):
            os.makedirs(self.remote_root_dirpath)

        # Find out which file is originals
        _src = os.path.realpath(src)
        _dst = os.path.realpath(dst)
        src_lock = None
        dst_lock = None

        if _src.startswith(os.path.realpath(self.remote_root_dirpath)):
            src_lock = FileLock(_src + ".lock")
        elif _dst.startswith(os.path.realpath(self.remote_root_dirpath)):
            dst_lock = FileLock(_dst + ".lock")
        else:
            return False, 'Fail to copy file.'

        try:
            if src_lock is not None:
                src_lock.acquire(timeout=1)
            if dst_lock is not None:
                dst_lock.acquire(timeout=1)

            shutil.copy(src, dst)

            if src_lock is not None:
                src_lock.release(force=True)
            if dst_lock is not None:
                dst_lock.release(force=True)

        except Timeout:
            return False, 'Fail to acquire filelock.'

        else:
            return True, ""

    def is_file(self, filename):
        '''Check if file exists or not.

        Args:
            filename(str): Target file name.

        Returns:
            bool: Returns True if the target file exists. Otherwise, return
                  False.
        '''

        remote_filepath = os.path.join(self.remote_root_dirpath, filename)
        return os.path.isfile(remote_filepath)

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

        remote = os.path.join(self.remote_root_dirpath, filename)

        if not os.path.isfile(remote):
            return False, "File does not exist. " + filename

        if not overwrite and os.path.isfile(local_filepath):
            return False, "Local file already exists."

        return self.__copyfile_locked(remote, local_filepath)

    def save_file(self, filename, local_filepath, **kwargs):
        '''Save a local file with its local path.

        If remote file exists, please use update_file() instead.

        Args:
            filename(str): File name to be saved.
            local_filepath(str): Local filepath.
            kwargs(dict): Additional arguments, includes:

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.
        '''

        local = os.path.normpath(local_filepath)
        remote = os.path.normpath(
            os.path.join(self.remote_root_dirpath, filename))

        if not os.path.isfile(local):
            return False, "Local file is missing."

        has_remote = os.path.isfile(remote)
        if has_remote:
            return False, "Remote file exists."

        return self.__copyfile_locked(local, remote)

    def update_file(self, filename, local_filepath, **kwargs):
        '''Update existing file use local file.

        If remote file is missing, this operation is failed.

        Args:
            filename(str): File name to be updated.
            local_filepath(str): Local filepath.
            kwargs(dict): Additional arguments, includes:

        Returns:
            bool: True for success, False otherwise.
            str: The reason for errors.

        Raises:
            OSError: Errors for file operations.
        '''

        local = os.path.normpath(local_filepath)
        remote = os.path.normpath(os.path.join(self.remote_root_dirpath,
                                               filename))
        temp = remote + ".swp"

        if not os.path.isfile(local):
            return False, "Local file is missing."

        has_remote = os.path.isfile(remote)
        temp = remote + ".swp"

        if not has_remote:
            return False, "Remote file is missing."

        if os.path.samefile(local, remote):
            # Directly modify file is forbidden.
            return False, "Local and remote file is the same."

        success, err = self.__copyfile_locked(remote, temp)
        if not success:
            return False, "Fail to update internal file", err

        success, err = self.__copyfile_locked(local, remote)
        if not success:
            return False, "Fail to update user file", err

        os.remove(temp)

        return True, ""
