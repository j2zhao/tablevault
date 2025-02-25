import os
import time
import shutil
from typing import Optional
from filelock import FileLock
from tablevault._utils.constants import TIMEOUT, CHECK_INTERVAL
from tablevault._utils.errors import TVLockError
from tablevault._utils.utils import gen_process_id


def _check_locks(process_id: str, lock_path: str, exclusive: bool) -> bool:
    for _, _, filenames in os.walk(lock_path):
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_name = lock_name.split("_")
                if filename.endswith(".exlock"):
                    if not process_id.startswith(lock_name):
                        return False
                elif filename.endswith(".shlock") and exclusive:
                    if not process_id.startswith(lock_name):
                        return False
    return True


def _acquire_exclusive(process_id: str, lock_path: str) -> str:
    lid = gen_process_id()
    legal = _check_locks(process_id, lock_path, exclusive=True)
    if not legal:
        return ""
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}_{lid}.exlock")
        with open(lock_file, "w"):
            pass
    return lid


def _acquire_shared(process_id: str, lock_path: str) -> str:
    lid = gen_process_id()
    legal = _check_locks(process_id, lock_path, exclusive=False)
    if not legal:
        return ""
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}_{lid}.shlock")
        with open(lock_file, "w"):
            pass
    return True


def _acquire_lock(
    process_id: str,
    lock_path: str,
    lock_type: str,
    timeout: Optional[float],
    check_interval: float,
) -> None:
    os.makedirs(lock_path, exist_ok=True)
    start_time = time.time()
    while True:
        if lock_type == "shared":
            success = _acquire_shared(process_id, lock_path)
        elif lock_type == "exclusive":
            success = _acquire_exclusive(process_id, lock_path)
        if success:
            return
        if timeout is not None and (time.time() - start_time) >= timeout:
            raise TVLockError("Timeout while trying to acquire read lock.")
        time.sleep(check_interval)


def _release_lock(
    lock_path: str,
    lid: str,
) -> None:

    for dirpath, _, filenames in os.walk(lock_path, topdown=False):
        empty = True
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_id = lock_name.split("_")[1]
                if lock_id == lid:
                    os.remove(os.path.join(dirpath, filename))
                else:
                    empty = False
        if empty:
            empty = any(
                os.path.isdir(os.path.join(dirpath, entry))
                for entry in os.listdir(dirpath)
            )
            if empty:
                shutil.rmtree(dirpath)


def _release_all_lock(
    process_id: str,
    lock_path: str,
) -> None:

    for dirpath, _, filenames in os.walk(lock_path, topdown=False):
        empty = True
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_name = lock_name.split("_")[0]
                if lock_name.startswith(process_id):
                    os.remove(os.path.join(dirpath, filename))
                else:
                    empty = False
        if empty:
            empty = any(
                os.path.isdir(os.path.join(dirpath, entry))
                for entry in os.listdir(dirpath)
            )
            if empty:
                shutil.rmtree(dirpath)


def get_all_process_ids(db_dir):
    meta_lock = os.path.join(db_dir, "metadata", "LOCK.lock")
    meta_lock = FileLock(meta_lock)
    process_ids = set()
    with meta_lock:
        lock_path = os.path.join(db_dir, "locks")
        for _, _, filenames in os.walk(lock_path):
            for filename in filenames:
                if filename.endswith(".exlock") or filename.endswith(".shlock"):
                    lock_name = filename.split(".")[0]
                    process_ids.add(lock_name)
    return list(process_ids)


class DatabaseLock:
    def __init__(self, process_id: str, db_dir: str):
        self.db_dir = db_dir
        self.process_id = process_id
        self.lock_path = os.path.join(self.db_dir, "locks")
        meta_lock = os.path.join(self.db_dir, "metadata", "LOCK.lock")
        self.meta_lock = FileLock(meta_lock)

    def acquire_shared_lock(
        self,
        table_name: str = "",
        instance_id: str = "",
        timeout: Optional[float] = TIMEOUT,
        check_interval: float = CHECK_INTERVAL,
    ) -> tuple[str, str, str]:
        if table_name != "":
            lock_path = os.path.join(self.lock_path, table_name)
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
        timeout: Optional[float] = TIMEOUT,
        check_interval: float = CHECK_INTERVAL,
    ) -> tuple[str, str, str]:

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

    def release_lock(self, lock_id: str) -> None:
        table_name, instance_id, lid = lock_id
        if table_name != "":
            lock_path = os.path.join(self.lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        with self.meta_lock:
            _release_lock(lock_path, lid)

    def release_all_locks(self) -> None:
        _release_all_lock(self.process_id, self.lock_path)
