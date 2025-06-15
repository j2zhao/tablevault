from tablevault._defintions import constants
from tablevault._defintions.tv_errors import (
    TVArgumentError,
    TVProcessError,
)
from tablevault._defintions.types import (
    ProcessLog,
    ColumnHistoryDict,
    TableHistoryDict,
    ActiveProcessDict,
    TableTempDict,
)
from tablevault._helper.file_operations import get_description

import json
import os
from typing import Optional
import time
from filelock import FileLock

import mmap

from dataclasses import asdict
import psutil
import logging

logger = logging.getLogger(__name__)


def _serialize_active_logs(temp_logs: ActiveProcessDict) -> dict:
    serialized_logs = {key: value.to_dict() for key, value in temp_logs.items()}
    return serialized_logs


def _deserialize_active_logs(serialized_logs: dict) -> ActiveProcessDict:
    deserialized_dict = {
        key: ProcessLog.from_dict(value) for key, value in serialized_logs.items()
    }
    return deserialized_dict


def _is_string_in_file(filepath, search_string):
    search_bytes = search_string.encode("utf-8")
    with open(filepath, "rb") as f:
        if os.fstat(f.fileno()).st_size == 0:
            return False
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return mm.find(search_bytes) != -1


def check_top_process(process_id, active_ids) -> tuple[bool, str]:
    string_set = set(active_ids)
    parts = process_id.split("_")
    if len(parts) <= 1:
        return True, process_id
    for i in range(1, len(parts)):
        prefix = "_".join(parts[:i])
        if prefix in string_set:
            return False, prefix
    return True, process_id


def get_top_level_proccesses(active_ids) -> list[str]:
    string_set = set(active_ids)
    result = []
    for s in active_ids:
        parts = s.split("_")
        is_shortest_prefix = True
        for i in range(1, len(parts)):
            prefix = "_".join(parts[:i])
            if prefix in string_set:
                is_shortest_prefix = False
                break
        if is_shortest_prefix:
            result.append(s)
    return result


class MetadataStore:
    def _save_active_logs(self, logs: ActiveProcessDict) -> None:
        logs = _serialize_active_logs(logs)
        with open(self.active_file, "w") as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: ColumnHistoryDict) -> None:
        with open(self.column_history_file, "w") as f:
            json.dump(columns_history, f, indent=4)

    def _save_table_history(self, table_history: TableHistoryDict) -> None:
        with open(self.table_history_file, "w") as f:
            json.dump(table_history, f, indent=4)

    def _save_table_temp(self, table_temp: TableTempDict) -> None:
        with open(self.table_temp_file, "w") as f:
            json.dump(table_temp, f, indent=4)

    def _get_active_logs(self) -> ActiveProcessDict:
        with open(self.active_file, "r") as file:
            data = json.load(file)
            logs = _deserialize_active_logs(data)
            return logs

    def _get_column_history(self) -> ColumnHistoryDict:
        with open(self.column_history_file, "r") as file:
            columns_history = json.load(file)
            return columns_history

    def _get_table_history(self) -> TableHistoryDict:
        with open(self.table_history_file, "r") as file:
            table_history = json.load(file)
            return table_history

    def _get_table_temp(self) -> TableTempDict:
        with open(self.table_temp_file, "r") as file:
            table_temp = json.load(file)
            return table_temp

    def _write_to_history(self, log_entry: ProcessLog) -> None:
        log_entry.log_time = time.time()
        log_entry_ = asdict(log_entry)
        with open(self.log_file, "a") as file:
            file.write(json.dumps(log_entry_) + "\n")

    def _write_to_completed(self, process_id: str) -> None:
        with open(self.completed_file, "a") as file:
            file.write(process_id + "\n")

    def _check_written(self, process_id: str) -> bool:
        return _is_string_in_file(self.completed_file, process_id)

    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, "metadata")
        self.log_file = os.path.join(meta_dir, constants.META_LOG_FILE)
        self.column_history_file = os.path.join(meta_dir, constants.META_CHIST_FILE)
        self.table_history_file = os.path.join(meta_dir, constants.META_THIST_FILE)
        self.table_temp_file = os.path.join(meta_dir, constants.META_TEMP_FILE)
        self.active_file = os.path.join(meta_dir, constants.META_ALOG_FILE)
        self.completed_file = os.path.join(meta_dir, constants.META_CLOG_FILE)
        meta_lock = os.path.join(meta_dir, constants.META_LOG_LOCK_FILE)
        self.lock = FileLock(meta_lock)

    def _create_table_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        columns_history = self._get_column_history()
        columns_history[table_name] = {}
        self._save_column_history(columns_history)
        table_history = self._get_table_history()
        table_history[table_name] = {}
        self._save_table_history(table_history)
        table_temp = self._get_table_temp()
        table_temp[table_name] = []
        self._save_table_temp(table_temp)

    def _delete_table_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        table_history = self._get_table_history()
        if table_name in table_history:
            del table_history[table_name]
        self._save_table_history(table_history)
        column_history = self._get_column_history()
        if table_name in column_history:
            del column_history[table_name]
        self._save_column_history(column_history)
        table_temp = self._get_table_temp()
        if table_name in table_temp:
            del table_temp[table_name]
        self._save_table_temp(table_temp)

    def _delete_instance_operation(self, log: ProcessLog) -> None:
        table_history = self._get_table_history()
        column_history = self._get_column_history()
        table_temp = self._get_table_temp()
        instance_id = log.data["instance_id"]
        table_name = log.data["table_name"]
        if instance_id in table_history[table_name]:
            del table_history[table_name][instance_id]
        if instance_id in column_history[table_name]:
            del column_history[table_name][instance_id]
        if instance_id in table_temp[table_name]:
            table_temp[table_name].remove(instance_id)
        self._save_table_history(table_history)
        self._save_column_history(column_history)
        self._save_table_temp(table_temp)

    def _rename_table_operation(self, log: ProcessLog) -> None:
        table_history = self._get_table_history()
        column_history = self._get_column_history()
        table_temp = self._get_table_temp()
        table_name = log.data["table_name"]
        new_table_name = log.data["new_table_name"]
        if table_name in table_history:
            table_history[new_table_name] = table_history.pop(table_name)
        if table_name in column_history:
            column_history[new_table_name] = column_history.pop(table_name)
        if table_name in table_temp:
            table_temp[new_table_name] = table_temp.pop(table_name)
        self._save_table_history(table_history)
        self._save_column_history(column_history)
        self._save_table_temp(table_temp)

    def _create_instance_operation(self, log: ProcessLog) -> None:
        table_temp = self._get_table_temp()
        table_name = log.data["table_name"]
        instance_id = log.data["instance_id"]
        if instance_id not in table_temp[table_name]:
            table_temp[table_name].append(instance_id)
        self._save_table_temp(table_temp)

    def _materialize_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        perm_instance_id = log.data["perm_instance_id"]
        instance_id = log.data["instance_id"]
        changed_columns = log.data["changed_columns"]
        all_columns = log.data["all_columns"]
        origin_id = log.data["origin_id"]
        origin_table = log.data["origin_table"]
        success = log.data["success"]
        table_history = self._get_table_history()
        table_temp = self._get_table_temp()
        table_metadata = get_description(
            instance_id="", table_name=table_name, db_dir=self.db_dir
        )
        if table_metadata[constants.TABLE_SIDE_EFFECTS] and len(changed_columns) > 0:
            for id in table_history[table_name]:
                changed_time, mat_time, stop_time, success = table_history[table_name][
                    id
                ]
                if stop_time is None:
                    table_history[table_name][id] = (
                        changed_time,
                        mat_time,
                        log.log_time,
                        success,
                    )
        if len(changed_columns) > 0:
            table_history[table_name][perm_instance_id] = (
                log.log_time,
                log.log_time,
                None,
                success,
            )
        else:
            prev_changed_time = table_history[origin_table][origin_id][0]
            table_history[table_name][perm_instance_id] = (
                prev_changed_time,
                log.log_time,
                None,
                success,
            )

        self._save_table_history(table_history)
        columns_history = self._get_column_history()
        columns_history[table_name][perm_instance_id] = {}
        for column in all_columns:
            if column in changed_columns:
                columns_history[table_name][perm_instance_id][column] = log.log_time
            else:
                columns_history[table_name][perm_instance_id][column] = columns_history[
                    origin_table
                ][origin_id][column]
        self._save_column_history(columns_history)
        if instance_id in table_temp[table_name]:
            table_temp[table_name].remove(instance_id)
        self._save_table_temp(table_temp)

    def _write_process(self, process_id: str) -> None:
        logs = self._get_active_logs()
        if process_id not in logs:
            raise TVProcessError("No Active Process")
        log = logs[process_id]
        if log.execution_success is None and log.start_success is not False:
            raise TVProcessError("Process id {process_id} not completed.")
        if log.operation not in constants.VALID_OPS:
            raise TVProcessError("Operation {log.operation} not supported")
        if log.execution_success:
            if log.operation == constants.CREATE_TABLE_OP:
                self._create_table_operation(log)
            if log.operation == constants.CREATE_INSTANCE_OP:
                self._create_instance_operation(log)
            elif log.operation == constants.DELETE_TABLE_OP:
                self._delete_table_operation(log)
            elif log.operation == constants.DELETE_INSTANCE_OP:
                self._delete_instance_operation(log)
            elif log.operation == constants.MAT_OP:
                self._materialize_operation(log)
            elif log.operation == constants.RENAME_TABLE_OP:
                self._rename_table_operation(log)
        self._write_to_history(log)
        self._write_to_completed(process_id)
        del logs[process_id]
        self._save_active_logs(logs)
        logger.info(f"Completed {log.operation}: {process_id}")

    def write_process(self, process_id: str) -> None:
        with self.lock:
            self._write_process(process_id)

    def check_written(self, process_id: str) -> bool:
        with self.lock:
            return _is_string_in_file(self.completed_file, process_id)

    def start_new_process(
        self,
        process_id: str,
        author: str,
        operation: str,
        pid: int,
        force_takedonwn: bool,
    ) -> str:
        with self.lock:
            logs = self._get_active_logs()
            start_time = time.time()
            if process_id in logs:
                raise TVProcessError(f"{process_id} already initiated.")
            completed_ = self._check_written(process_id)
            if completed_:
                raise TVProcessError(f"{process_id} already written.")
            logs[process_id] = ProcessLog(
                process_id,
                author,
                start_time,
                start_time,
                operation,
                [],
                [],
                {},
                None,
                None,
                None,
                pid,
                force_takedonwn,
            )
            self._save_active_logs(logs)
            return process_id

    def update_process_start_status(
        self, process_id: str, success: bool, error: tuple[str, str] = ("", "")
    ) -> None:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("No process id {process_id} found.")
            log = logs[process_id]
            if log.execution_success is not None:
                raise TVProcessError("Process id {process_id} already completed.")
            if log.start_success is not None:
                raise TVProcessError("Process id {process_id} already started.")
            log.start_success = success
            log.log_time = time.time()
            log.error = error
            log.start_success = success
            self._save_active_logs(logs)

    def update_process_execution_status(
        self, process_id: str, success: bool, error: tuple[str, str] = ("", "")
    ) -> None:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("No process id {process_id} found.")
            log = logs[process_id]
            if log.execution_success is not None:
                raise TVProcessError("Process id {process_id} already completed.")
            if log.start_success is None:
                raise TVProcessError("Process id {process_id} not started.")
            log.error = error
            log.execution_success = success
            log.log_time = time.time()
            self._save_active_logs(logs)

    def update_process_data(self, process_id: str, data: dict) -> None:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("No Active Process")
            log = logs[process_id]
            if log.execution_success is not None:
                raise TVProcessError(
                    "Process id {process_id} completed. Cannot write afterwards."
                )
            log.log_time = time.time()
            logs[process_id].data.update(data)
            self._save_active_logs(logs)

    def _update_process_step_internal(self, process_id: str, step: str) -> None:
        logs = self._get_active_logs()
        if process_id not in logs:
            raise TVProcessError("No Active Process")
        log = logs[process_id]
        if log.execution_success is not None:
            raise TVProcessError(
                "Process id {process_id} completed. Cannot write afterwards."
            )
        if log.start_success is None:
            raise TVProcessError(
                "Process id {process_id} not started. Cannot write before."
            )
        log.complete_steps.append(step)
        log.step_times.append(time.time())
        log.log_time = time.time()
        self._save_active_logs(logs)

    def update_process_step(self, process_id: str, step: str) -> None:
        with self.lock:
            self._update_process_step_internal(process_id, step)

    def get_table_times(
        self, instance_id: str, table_name: str
    ) -> tuple[float, float, Optional[float], bool]:
        with self.lock:
            table_history = self._get_table_history()
            return table_history[table_name][instance_id]

    def get_column_times(
        self, column_name: str, instance_id: str, table_name: str
    ) -> tuple[float, float, Optional[float]]:
        with self.lock:
            column_history = self._get_column_history()
            table_history = self._get_table_history()
            mat_time = column_history[table_name][instance_id][column_name]
            _, start_time, end_time, _ = table_history[table_name][instance_id]
            return mat_time, start_time, end_time

    def _get_last_table_update(
        self,
        table_name: str,
        version: str = "",
        before_time: Optional[float] = None,
        active_only: bool = True,
        success_only=False,
    ) -> tuple[float, float, str]:
        table_history = self._get_table_history()
        max_changed_time = 0
        max_start_time = 0
        max_id = ""
        for instance_id, (changed_time, start_time, end_time, success) in table_history[
            table_name
        ].items():
            if success_only and not success:
                continue
            if (
                version is not None
                and version != ""
                and not instance_id.startswith(version)
            ):
                continue
            if active_only and end_time is not None:
                continue
            if start_time > max_start_time and (
                before_time is None or start_time <= before_time
            ):
                max_start_time = start_time
                max_changed_time = changed_time
                max_id = instance_id
        return max_changed_time, max_start_time, max_id

    def get_last_table_update(
        self,
        table_name: str,
        version: str = "",
        before_time: Optional[float] = None,
        active_only: bool = True,
        success_only=False,
    ) -> tuple[float, float, str]:
        return self._get_last_table_update(
            table_name, version, before_time, active_only, success_only
        )

    def get_last_column_update(
        self,
        table_name: str,
        column: str,
        before_time: Optional[float] = None,
        version: str = "base",
        active_only: bool = True,
        success_only: bool = False,
    ) -> tuple[float, float, str]:
        """Returns 0 when we didn't find any tables that meet
        conditions.

        Return -1 when the table was last updated after before_times and it can only
        have one active version.
        """
        with self.lock:
            _, max_start_time, max_id = self._get_last_table_update(
                table_name, version, before_time, active_only, success_only
            )
            columns_history = self._get_column_history()
            max_mat_time = columns_history[table_name][max_id][column]
            return max_mat_time, max_start_time, max_id

    def get_active_processes(self) -> ActiveProcessDict:
        with self.lock:
            active_logs = self._get_active_logs()
            return active_logs

    def get_table_instances(
        self, table_name: str, version: str, include_temp=False
    ) -> None | list[str]:
        with self.lock:
            table_history = self._get_table_history()
            if table_name == "":
                return list(table_history.keys())
            if table_name not in table_history:
                return None
            instances = list(table_history[table_name].keys())
            instances.sort(key=lambda x: table_history[table_name][x][1])
            if include_temp:
                table_temp = self._get_table_temp()
                if table_name in table_temp:
                    instances += table_temp[table_name]
            if version != "":
                instances_ = [instance for instance in instances if version in instance]
                instances = instances_
                return instances
            else:
                return instances

    def update_process_pid(
        self, process_id: str, pid: Optional[int], force: bool = False
    ) -> Optional[int]:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("Process ID not active {process_id}")
            old_pid = logs[process_id].pid
            for process_id_ in logs:
                if process_id_ == process_id:
                    continue
                if (
                    logs[process_id_].operation == constants.STOP_PROCESS_OP
                    and logs[process_id_].start_success
                ):
                    if logs[process_id_].pid == old_pid:
                        raise TVProcessError(
                            "Process ID {process_id} in operation to being stopped"
                        )

            if force or old_pid == pid:
                logs[process_id].pid = pid
                self._save_active_logs(logs)
                return old_pid
            try:
                psutil.Process(old_pid)

            except psutil.NoSuchProcess:
                logs[process_id].pid = pid
                self._save_active_logs(logs)
                return old_pid
            raise TVProcessError(
                "Process ID {process_id} already running at: {old_pid}"
            )

    def stop_operation(
        self, process_id: str, force: bool
    ) -> tuple[ActiveProcessDict, list[str]]:
        with self.lock:
            logs = self._get_active_logs()
            pid = os.getpid()
            if process_id not in logs:
                raise TVProcessError("Process not currently active.")
            if logs[process_id].operation == constants.STOP_PROCESS_OP:
                raise TVArgumentError(
                    f"Cannot stop another stop_process operation. Instead rerun it with its process_id ({process_id})"
                )
            check, _ = check_top_process(process_id, list(logs.keys()))
            if not check:
                raise TVArgumentError("Only can stop top-level processes.")
            old_pid = logs[process_id].pid
            if old_pid != pid:
                try:
                    proc = psutil.Process(old_pid)
                    if not force:
                        raise TVProcessError(
                            """Process {process_id} Currently Running.
                            Cannot be stopped unless forced."""
                        )
                    if force and pid != old_pid:
                        try:
                            proc.terminate()
                            proc.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                except psutil.NoSuchProcess:
                    pass
                logs[process_id].pid = pid
                relevant_logs = []
                for process_id_ in logs:
                    if process_id_.startswith(process_id):
                        logs[process_id_].pid = pid
                        relevant_logs.append(process_id_)
                relevant_logs.sort(reverse=True)
                self._save_active_logs(logs)
            else:
                relevant_logs = []
                for process_id_ in logs:
                    if process_id_.startswith(process_id):
                        relevant_logs.append(process_id_)
                relevant_logs.sort(reverse=True)
            return (logs, relevant_logs)
