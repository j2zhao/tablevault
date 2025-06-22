import os
import time
import shutil
from typing import Optional
from filelock import FileLock
from tablevault._defintions import constants
from tablevault._defintions.tv_errors import TVLockError
from tablevault._helper.utils import gen_tv_id
from tablevault._helper.user_lock import set_tv_lock


def _check_locks(process_id: str, lock_path: str, exclusive: bool) -> list[str]:
    conflicting_processes = []
    for _, _, filenames in os.walk(lock_path):
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                process_name = lock_name.split("__")[0]
                if filename.endswith(".exlock"):
                    if not process_id.startswith(process_name):
                        conflicting_processes.append(process_name)
                        # return False
                elif filename.endswith(".shlock") and exclusive:
                    if not process_id.startswith(process_name):
                        conflicting_processes.append(process_name)
                        # return False
    return conflicting_processes


def _acquire_exclusive(process_id: str, lock_path: str) -> tuple[str | list[str], bool]:
    lid = gen_tv_id()
    conflicting_processes = _check_locks(process_id, lock_path, exclusive=True)
    if len(conflicting_processes) > 0:
        return conflicting_processes, False
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}__{lid}.exlock")
        with open(lock_file, "w"):
            pass
    return lid, True


def _acquire_shared(process_id: str, lock_path: str) -> tuple[str | list[str], bool]:
    lid = gen_tv_id()
    conflicting_processes = _check_locks(process_id, lock_path, exclusive=False)
    if len(conflicting_processes) > 0:
        return conflicting_processes, False
    for dirpath, _, _ in os.walk(lock_path):
        lock_file = os.path.join(dirpath, f"{process_id}__{lid}.shlock")
        with open(lock_file, "w"):
            pass
    return lid, True


def _acquire_lock(
    process_id: str,
    lock_path: str,
    lock_type: str,
    timeout: Optional[float],
    check_interval: float,
) -> tuple[str | list[str], bool]:
    if not os.path.exists(lock_path):
        raise TVLockError(f"lockpath {lock_path} does not exist")
    start_time = time.time()
    while True:
        if lock_type == "shared":
            lid, success = _acquire_shared(process_id, lock_path)
        elif lock_type == "exclusive":
            lid, success = _acquire_exclusive(process_id, lock_path)
        if success:
            return lid, success
        if timeout is not None and (time.time() - start_time) >= timeout:
            return lid, success
            # raise TVLockError("Timeout while trying to acquire lock.")
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
                lock_id = lock_name.split("__")[1]
                if lock_id == lid:
                    os.remove(os.path.join(dirpath, filename))


def _release_all_lock(
    process_id: str,
    lock_path: str,
) -> None:
    if not os.path.exists(lock_path):
        return
    locks_to_remove = []
    for dirpath, _, filenames in os.walk(lock_path, topdown=False):
        for filename in filenames:
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                lock_name = lock_name.split("__")[0]

                if lock_name == process_id:
                    lock_ = os.path.join(dirpath, filename)
                    locks_to_remove.append(lock_)
                elif lock_name.startswith(process_id):
                    raise TVLockError(
                        """Cannot remove all parent locks
                                      when children locks exist."""
                    )
    for lock_ in locks_to_remove:
        os.remove(lock_)


def _make_lock_path(lock_path: str) -> None:
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


def _delete_lock_path(process_id, lock_path: str) -> None:
    if os.path.exists(lock_path):
        for filename in os.listdir(lock_path):
            if filename.endswith(".exlock") or filename.endswith(".shlock"):
                lock_name = filename.split(".")[0]
                process_name = lock_name.split("__")[0]
                if not process_id.startswith(process_name):
                    raise TVLockError(
                        f"Cannot delete {lock_path} with active lock: {filename}"
                    )
        shutil.rmtree(lock_path)


class DatabaseLock:
    def __init__(self, process_id: str, db_dir: str) -> None:
        self.db_dir = db_dir
        self.process_id = process_id
        self.lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        meta_lock = os.path.join(self.db_dir, constants.METADATA_FOLDER, "LOCK.lock")
        self.meta_lock = FileLock(meta_lock)

    def acquire_shared_lock(
        self,
        table_name: str = "",
        instance_id: str = "",
        timeout: Optional[float] = constants.TIMEOUT,
        check_interval: float = constants.CHECK_INTERVAL,
    ) -> tuple[tuple[str, str, str] | list[str], bool]:
        lock_path = self.lock_path
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)

        with self.meta_lock:
            lid, success = _acquire_lock(
                self.process_id,
                lock_path,
                lock_type="shared",
                timeout=timeout,
                check_interval=check_interval,
            )
            if success:
                return (table_name, instance_id, lid), success
            else:
                return lid, success

    def acquire_exclusive_lock(
        self,
        table_name: str = "",
        instance_id: str = "",
        timeout: Optional[float] = constants.TIMEOUT,
        check_interval: float = constants.CHECK_INTERVAL,
    ) -> tuple[tuple[str, str, str] | list[str], bool]:
        lock_path = self.lock_path
        with self.meta_lock:
            if table_name != "":
                lock_path = os.path.join(self.lock_path, table_name)
            if instance_id != "":
                lock_path = os.path.join(lock_path, instance_id)
            lid, success = _acquire_lock(
                self.process_id,
                lock_path,
                lock_type="exclusive",
                timeout=timeout,
                check_interval=check_interval,
            )
            if success:
                set_tv_lock(instance_id, table_name, self.db_dir)
                return (table_name, instance_id, lid), success
            else:
                return lid, success

    def release_lock(self, lock_id: tuple[str, str, str]) -> None:
        table_name, instance_id, lid = lock_id
        lock_path = self.lock_path
        if table_name != "":
            lock_path = os.path.join(self.lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        with self.meta_lock:
            _release_lock(lock_path, lid)
            set_tv_lock(instance_id, table_name, self.db_dir)

    def release_all_locks(self, table_name="", instance_id="") -> None:
        lock_path = self.lock_path
        if table_name != "":
            lock_path = os.path.join(self.lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        _release_all_lock(self.process_id, lock_path)
        set_tv_lock("", "", self.db_dir)

    def make_lock_path(self, table_name: str = "", instance_id: str = ""):
        lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        with self.meta_lock:
            _make_lock_path(lock_path)

    def delete_lock_path(self, table_name: str = "", instance_id: str = "") -> None:
        lock_path = os.path.join(self.db_dir, constants.LOCK_FOLDER)
        if table_name != "":
            lock_path = os.path.join(lock_path, table_name)
        if instance_id != "":
            lock_path = os.path.join(lock_path, instance_id)
        with self.meta_lock:
            _delete_lock_path(self.process_id, lock_path)
