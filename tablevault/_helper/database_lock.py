import os
import time
import shutil
from typing import Optional
from filelock import FileLock
from tablevault.defintions import constants 
from tablevault.defintions.tv_errors import TVLockError, TVArgumentError
from tablevault.defintions.utils import gen_tv_id
from tablevault._helper.metadata_store import MetadataStore

def _check_locks(process_id: str, lock_path: str, exclusive: bool) -> bool:
    for _, _, filenames in os.walk(lock_path):
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                process_name = lock_name.split("_")[0]
                if filename.endswith(".exlock"):
                    if not process_id.startswith(process_name):
                        return False
                elif filename.endswith(".shlock") and exclusive:
                    if not process_id.startswith(process_name):
                        return False
    return True


def _acquire_exclusive(process_id: str, lock_path: str) -> str:
    lid = gen_tv_id()
    legal = _check_locks(process_id, lock_path, exclusive=True)
    if not legal:
        return ""
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}_{lid}.exlock")
        with open(lock_file, "w"):
            pass
    return lid


def _acquire_shared(process_id: str, lock_path: str) -> str:
    lid = gen_tv_id()
    legal = _check_locks(process_id, lock_path, exclusive=False)
    if not legal:
        return ""
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}_{lid}.shlock")
        with open(lock_file, "w"):
            pass
    return lid


def _acquire_lock(
    process_id: str,
    lock_path: str,
    lock_type: str,
    timeout: Optional[float],
    check_interval: float,
) -> str:
    if not os.path.exists(lock_path):
        raise TVLockError(f"lockpath {lock_path} does not exist")
    start_time = time.time()
    while True:
        if lock_type == "shared":
            lid = _acquire_shared(process_id, lock_path)
        elif lock_type == "exclusive":
            lid = _acquire_exclusive(process_id, lock_path)
        if lid != "":
            return lid
        if timeout is not None and (time.time() - start_time) >= timeout:
            raise TVLockError("Timeout while trying to acquire lock.")
        time.sleep(check_interval)


def _release_lock(
    lock_path: str,
    lid: str,
) -> None:
    if not os.path.exists(lock_path):
        return
    for dirpath, _, filenames in os.walk(lock_path, topdown=False):
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_id = lock_name.split("_")[1]
                if lock_id == lid:
                    os.remove(os.path.join(dirpath, filename))

def _release_all_lock(
    process_id: str,
    lock_path: str,
) -> None:
    if not os.path.exists(lock_path):
        return
    for dirpath, _, filenames in os.walk(lock_path, topdown=False):
        # empty = True
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_name = lock_name.split("_")[0]
                if lock_name.startswith(process_id):
                    os.remove(os.path.join(dirpath, filename))
                # else:
                #     empty = False
        # if empty:
        #     empty = any(
        #         os.path.isdir(os.path.join(dirpath, entry))
        #         for entry in os.listdir(dirpath)
        #     )
        #     if empty:
        #         shutil.rmtree(dirpath)

def _make_lock_path(lock_path:str):
    parent_dir = os.path.dirname(lock_path)
    parent_locks = []
    for filename in os.listdir(parent_dir):
        if filename.endswith(".exlock") or filename.endswith(".shlock"):
            parent_locks.append(filename)
    os.makedirs(lock_path, exist_ok=True)
    for filename in parent_locks:
        lock_file = os.path.join(lock_path, filename)
        with open(lock_file, "w"):
            pass

def _delete_lock_path(lock_path:str):
    if os.path.exists(lock_path):
        for filename in os.listdir(lock_path):
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                raise TVLockError(f"Cannot delete lock_path {lock_path} with active lock: {filename}")
        shutil.rmtree(lock_path)


class DatabaseLock:
    def __init__(self, process_id: str, db_dir: str):
        self.db_dir = db_dir
        self.process_id = process_id
        self.lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        meta_lock = os.path.join(self.db_dir, constants.METADATA_FOLDER, "LOCK.lock")
        self.meta_lock = FileLock(meta_lock)
        self.db_metadata = MetadataStore(db_dir)

    def acquire_shared_lock(
        self,
        table_name: str = "",
        instance_id: str = "",
        timeout: Optional[float] = constants.TIMEOUT,
        check_interval: float = constants.CHECK_INTERVAL,
    ) -> tuple[str, str, str]:
        lock_path = self.lock_path
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)

        with self.meta_lock:
            lid = _acquire_lock(
                self.process_id,
                lock_path,
                lock_type="shared",
                timeout=timeout,
                check_interval=check_interval,
            )
            return (table_name, instance_id, lid)

    def acquire_exclusive_lock(
        self,
        table_name: str = "",
        instance_id: str = "",
        timeout: Optional[float] = constants.TIMEOUT,
        check_interval: float = constants.CHECK_INTERVAL,
    ) -> tuple[str, str, str]:
        lock_path = self.lock_path
        with self.meta_lock:
            if table_name != "":
                lock_path = os.path.join(self.lock_path, table_name)
            if instance_id != "":
                lock_path = os.path.join(lock_path, instance_id)
            lid = _acquire_lock(
                self.process_id,
                lock_path,
                lock_type="exclusive",
                timeout=timeout,
                check_interval=check_interval,
            )
            return (table_name, instance_id, lid)

    def release_lock(self, lock_id: tuple[str, str, str]) -> None:
        table_name, instance_id, lid = lock_id
        print(lock_id)
        if table_name != "":
            lock_path = os.path.join(self.lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        with self.meta_lock:
            _release_lock(lock_path, lid)

    def release_all_locks(self) -> None:
        _release_all_lock(self.process_id, self.lock_path)

    def make_lock_path(self, 
                   table_name:str = '',
                   instance_id:str = ''):
        lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)   
        with self.meta_lock:
            _make_lock_path(lock_path)
    
    def delete_lock_path(self, 
                   table_name:str = '',
                   instance_id:str = ''):
        lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)   
        with self.meta_lock:
            _delete_lock_path(lock_path) 