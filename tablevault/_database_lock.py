import portalocker
import time
import os
from typing import Optional
import shutil
from filelock import FileLock



class MultiLock:
    """
    A multi-lock allowing multiple readers or one writer at a time,
    with optional timeout support.
    """
    def __init__(self, lock_file):
        """
        Initializes the ReadWriteLock with the specified lock file.
        
        :param lock_file: Path to the lock file used for synchronization.
        """
        self.lock_file = lock_file
        self.read_handle = None
        self.write_handle = None
        # Ensure the lock file exists
        if not os.path.exists(self.lock_file):
            open(self.lock_file, 'a').close()

    def acquire_shared(self, timeout=None, check_interval=0.1):
        """
        Acquires a shared (read) lock.

        :param timeout: Maximum time (in seconds) to wait for the lock. None means wait indefinitely.
        :param check_interval: Time (in seconds) between lock acquisition attempts.
        :return: True if the lock was acquired, False otherwise.
        :raises TimeoutError: If the lock could not be acquired within the timeout.
        """
        start_time = time.time()
        while True:
            try:
                # Open the lock file in read mode
                self.read_handle = open(self.lock_file, 'r')
                # Attempt to acquire a shared lock without blocking
                portalocker.lock(self.read_handle, portalocker.LOCK_SH | portalocker.LOCK_NB)
                return True  # Lock acquired
            except (portalocker.exceptions.LockException, IOError):
                # Failed to acquire lock
                if self.read_handle:
                    self.read_handle.close()
                    self.read_handle = None
                if timeout is not None and (time.time() - start_time) >= timeout:
                    raise TimeoutError("Timeout while trying to acquire read lock.")
                time.sleep(check_interval)

    def release_shared(self):
        """
        Releases the shared (read) lock.
        """
        if self.read_handle:
            try:
                portalocker.unlock(self.read_handle)
            finally:
                self.read_handle.close()
                self.read_handle = None

    def acquire_exclusive(self, timeout=None, check_interval=0.1):
        """
        Acquires an exclusive (write) lock.

        :param timeout: Maximum time (in seconds) to wait for the lock. None means wait indefinitely.
        :param check_interval: Time (in seconds) between lock acquisition attempts.
        :return: True if the lock was acquired, False otherwise.
        :raises TimeoutError: If the lock could not be acquired within the timeout.
        """
        start_time = time.time()
        while True:
            try:
                # Open the lock file in append mode to ensure it exists
                self.write_handle = open(self.lock_file, 'a+')
                # Attempt to acquire an exclusive lock without blocking
                portalocker.lock(self.write_handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
                return True  # Lock acquired
            except (portalocker.exceptions.LockException, IOError):
                # Failed to acquire lock
                if self.write_handle:
                    self.write_handle.close()
                    self.write_handle = None
                if timeout is not None and (time.time() - start_time) >= timeout:
                    raise TimeoutError("Timeout while trying to acquire write lock.")
                time.sleep(check_interval)

    def release_exclusive(self):
        """
        Releases the exclusive (write) lock.
        """
        if self.write_handle:
            try:
                portalocker.unlock(self.write_handle)
            finally:
                self.write_handle.close()
                self.write_handle = None

    def __del__(self):
        """
        Destructor to ensure that any held locks are released.
        """
        self.release_shared()
        self.release_exclusive()


def clean_up_locks(db_dir: str):
    lock_path = os.path.join(db_dir, 'locks')
    for root, _, files in os.walk(db_dir):
        for file in files:
            if file.endswith('.lock'):
                file_ = os.path.join(root, file)
                lock = MultiLock(file_)
                lock.release_shared()
                lock.release_exclusive()

class DatabaseLock():
    def __init__(self, db_dir: str,  table_name:Optional[str] = None, instance_id:Optional[str] = None, restart = False):
        self.instance_id = instance_id
        self.table_name = table_name
        self.db_dir = db_dir
        self.locks = []
        self.deleted = False
        meta_lock = os.path.join(db_dir, 'metadata', 'LOG.lock') 
        self.lock = FileLock(meta_lock)
        
        if instance_id:
            lock_path = os.path.join(db_dir, 'locks', table_name, f'{instance_id}.lock')
            self.locks.append(MultiLock(lock_path))
            self.type = 'instance'

        elif table_name:
            base_lock_dir = os.path.join(db_dir, 'locks', table_name)
            os.makedirs(base_lock_dir, exist_ok=True)
            self.locks.append(MultiLock(os.path.join(base_lock_dir, f'{table_name}.lock')))
            for root, _, files in os.walk(base_lock_dir):
                for file in files:
                    if file.endswith('.lock') and file != f'{table_name}.lock':
                        lock_path = os.path.join(root, file)
                        self.locks.append(MultiLock(lock_path))
            self.type = 'table'
        else:
            base_lock_dir = os.path.join(db_dir, 'locks')
            os.makedirs(base_lock_dir, exist_ok=True)
            self.locks.append(MultiLock(os.path.join(base_lock_dir, 'DATABASE.lock')))
            for root, _, files in os.walk(base_lock_dir):
                for file in files:
                    if file.endswith('.lock') and file != 'DATABASE.lock' and file != 'RESTART.lock':
                        lock_path = os.path.join(root, file)
                        self.locks.append(MultiLock(lock_path))
            self.type = 'db'
        
        if table_name != 'RESTART':
            self.restart = restart
            restart_path = os.path.join(db_dir, 'locks', 'RESTART')
            os.makedirs(restart_path, exist_ok=True)
            restart_path = os.path.join(restart_path, 'RESTART.lock')
            self.restart_lock = MultiLock(restart_path)
        
        self.has_lock = None
    
    def acquire_shared_lock(self, timeout=None, check_interval=0.1):
        with self.lock:
            if self.deleted:
                raise ValueError('Lock Does Not Exist')
            if self.has_lock != None:
                raise ValueError('Lock has already been acquired.')
            if self.table_name != 'RESTART' and not self.restart:
                self.restart_lock.acquire_shared(timeout, check_interval)
            try:
                for lock in self.locks:
                    lock.acquire_shared(timeout, check_interval)
            except Exception as e:
                self.release_shared_lock()
                raise e
            self.has_lock = 'shared'

    def acquire_exclusive_lock(self, timeout=None, check_interval=0.1):
        with self.lock:
            if self.deleted:
                raise ValueError('Lock Does Not Exist')
            if self.has_lock != None:
                raise ValueError('Lock has already been acquired.')
            if self.table_name != 'RESTART' and not self.restart:
                self.restart_lock.acquire_shared(timeout, check_interval)
            try:
                for lock in self.locks:
                    lock.acquire_exclusive(timeout, check_interval)
            except Exception as e:
                self.release_exclusive_lock()
                raise e
            self.has_lock = 'exclusive'

    def _delete_locks(self):
        if self.type == 'db':
            raise ValueError('Cannot delete database lock.')
        elif self.type == 'table':
            lock_dir = os.path.join(self.db_dir, 'locks', self.table_name)
            shutil.rmtree(lock_dir)
        elif self.type == 'instance':
            lock_path = os.path.join(self.db_dir, 'locks', self.table_name, f'{self.instance_id}.lock')
            os.remove(lock_path)
        self.deleted = True


    def release_shared_lock(self, delete = False):
        with self.lock:
            if self.deleted:
                raise ValueError('Lock Does Not Exist')
            if self.has_lock == None:
                pass
            elif self.has_lock != 'shared':
                raise ValueError('Shared lock does not exist')
            else:
                for lock in self.locks:
                    lock.release_shared()
                if self.table_name != 'RESTART' and not self.restart:
                    self.restart_lock.release_shared()
                self.has_lock = None
            if delete:
                self._delete_locks()


    def release_exclusive_lock(self, delete = False):
        with self.lock:
            if self.deleted:
                raise ValueError('Lock Does Not Exist')
            if self.has_lock == None:
                pass
            elif self.has_lock != 'exclusive':
                raise ValueError('Shared lock does not exist')
            else:
                for lock in self.locks:
                    lock.release_exclusive()
                if self.table_name != 'RESTART' and not self.restart:
                    self.restart_lock.release_shared()
                self.has_lock = None
            if delete:
                self._delete_locks()
                


