import json
import os
from typing import Optional, Any
import time
from filelock import FileLock
from tablevault._defintions.tv_errors import TVArgumentError, TVProcessError
import pprint
import mmap
from tablevault._defintions.types import ProcessLog, ColumnHistoryDict, TableHistoryDict, TableMultipleDict, ActiveProcessDict
from dataclasses import asdict
from tablevault._defintions import constants
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
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return mm.find(search_bytes) != -1


class MetadataStore:

    def _save_active_logs(self, logs: ActiveProcessDict) -> None:
        logs = _serialize_active_logs(logs)
        with open(self.active_file, "w") as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: ColumnHistoryDict) -> None:
        with open(self.column_history_file, "w") as f:
            json.dump(columns_history, f, indent=4)

    def _save_table_history(self, table_history: TableHistoryDict):
        with open(self.table_history_file, "w") as f:
            json.dump(table_history, f, indent=4)

    def _save_table_multiple(self, table_multiples: TableMultipleDict):
        with open(self.table_multiple_file, "w") as f:
            json.dump(table_multiples, f, indent=4)

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

    def _get_table_multiple(self) -> TableMultipleDict:
        with open(self.table_multiple_file, "r") as file:
            table_multiples = json.load(file)
            return table_multiples

    def _write_to_history(self, log_entry: ProcessLog) -> None:
        log_entry.log_time = time.time()
        log_entry = asdict(log_entry)
        with open(self.log_file, "a") as file:
            file.write(json.dumps(log_entry) + "\n")

    def _write_to_completed(self, process_id: str) -> None:
        with open(self.completed_file, "a") as file:
            file.write(process_id + "\n")

    def _check_written(self, process_id: str) -> bool:
        return _is_string_in_file(self.completed_file, process_id)

    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, "metadata")
        self.log_file = os.path.join(meta_dir, "logs.txt")
        self.column_history_file = os.path.join(meta_dir, "columns_history.json")
        self.table_history_file = os.path.join(meta_dir, "tables_history.json")
        self.table_multiple_file = os.path.join(meta_dir, "tables_multiple.json")
        self.table_start_file = os.path.join(meta_dir, "tables_start.json")
        self.active_file = os.path.join(meta_dir, "active_logs.json")
        self.completed_file = os.path.join("completed_logs.txt")
        meta_lock = os.path.join(meta_dir, "LOG.lock")
        self.lock = FileLock(meta_lock)

    def _setup_table_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        columns_history = self._get_column_history()
        columns_history[table_name] = {}
        self._save_column_history(columns_history)
        table_history = self._get_table_history()
        table_history[table_name] = {}
        self._save_table_history(table_history)
        table_multiples = self._get_table_multiple()
        table_multiples[table_name] = log.data["allow_multiple"]
        self._save_table_multiple(table_multiples)

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
        table_multiples = self._get_table_multiple()
        if table_name in table_multiples:
            del table_multiples[table_name]
        self._save_table_multiple(table_multiples)

    def _delete_instance_operation(self, log: ProcessLog) -> None:
        table_history = self._get_table_history()
        column_history = self._get_column_history()
        instance_id = log.data["instance_id"]
        table_name = log.data["table_name"]
        if instance_id in table_history[table_name]:
            del table_history[table_name][instance_id]
        if instance_id in column_history[table_name]:
            del column_history[table_name][instance_id]
        self._save_table_history(table_history)
        self._save_column_history(column_history)

    def _execute_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        instance_id = log.data["perm_instance_id"]
        changed_columns = log.data["to_change_columns"]
        all_columns = log.data["all_columns"]
        prev_instance_id = log.data["origin"]
        table_history = self._get_table_history()
        if len(changed_columns) > 0:
            table_history[table_name][instance_id] = (log.log_time, log.log_time)
        else:
            prev_changed_time = table_history[table_name][prev_instance_id][0]
            table_history[table_name][instance_id] = (prev_changed_time, log.log_time)
        self._save_table_history(table_history)
        columns_history = self._get_column_history()
        columns_history[table_name][instance_id] = {}
        for column in all_columns:
            if column in changed_columns:
                columns_history[table_name][instance_id][column] = log.log_time
            else:
                columns_history[table_name][instance_id][column] = columns_history[
                    table_name
                ][prev_instance_id][column]
        self._save_column_history(columns_history)
        
    def write_process(self, process_id: str) -> None:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("No Active Process")
            log = logs[process_id]
            if log.execution_success is None and log.start_success is not False:
                raise  TVProcessError("Process id {process_id} not completed.")
            if log.operation not in constants.VALID_OPS:
                raise TVProcessError("Operation {log.operation} not supported")
            if log.execution_success:
                if log.operation == constants.SETUP_TABLE_OP:
                    self._setup_table_operation(log)
                elif log.operation == constants.DELETE_TABLE_OP:
                    self._delete_table_operation(log)
                elif log.operation == constants.DELETE_INSTANCE_OP:
                    self._delete_instance_operation(log)
                elif log.operation ==  constants.EXECUTE_OP:
                    self._execute_operation(log)
            self._write_to_history(log)
            self._write_to_completed(process_id)
            del logs[process_id]
            self._save_active_logs(logs)
            logger.info(f"Completed {log.operation}: {process_id}")

    def start_new_process(
        self,
        process_id: str,
        author: str,
        operation: str,
        pid: int
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
                {},
                None,
                None,
                pid
            )
            self._save_active_logs(logs)
            return process_id
    
    def update_process_start_status(self, process_id: str, success: bool, error:tuple[str, str] = ('', '')) -> None:
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
            if success == True:
                self._save_active_logs(logs)
            if success == False:
                log.execution_success = success
                self._write_to_history(log)
                self._write_to_completed(process_id)
                del logs[process_id]
                self._save_active_logs(logs)
    
    def update_process_execution_status(self, process_id: str, success: bool, error:tuple[str, str] = ('', '')) -> None:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("No process id {process_id} found.")
            log = logs[process_id]
            if log.execution_success is not None:
                raise  TVProcessError("Process id {process_id} already completed.")
            if log.start_success is None:
                raise  TVProcessError("Process id {process_id} not started.")
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
                raise  TVProcessError("Process id {process_id} completed. Cannot write afterwards.")
            if log.start_success is None:
                raise  TVProcessError("Process id {process_id} not started. Cannot write before.")
            log.log_time = time.time()
            logs[process_id].data.update(data)
            self._save_active_logs(logs)

    def _update_process_step_internal(
        self, process_id: str, step: str
    ) -> None:
        logs = self._get_active_logs()
        if process_id not in logs:
                raise TVProcessError("No Active Process")
        log = logs[process_id]
        if log.execution_success is not None:
                raise  TVProcessError("Process id {process_id} completed. Cannot write afterwards.")
        if log.start_success is None:
                raise  TVProcessError("Process id {process_id} not started. Cannot write before.")
        log.complete_steps.append(step)
        log.step_times.append(time.time())
        log.log_time = time.time()
        self._save_active_logs(logs)

    def update_process_step(self, process_id: str, step: str) -> None:
        with self.lock:
            self._update_process_step_internal(process_id, step)

    def _get_multiple_internal(self, table_name) -> bool:
        allow_multiples = self._get_table_multiple()
        return allow_multiples[table_name]

    def get_table_multiple(self, table_name: str) -> bool:
        with self.lock:
            return self._get_multiple_internal(table_name)

    def get_table_times(self, instance_id: str, table_name: str) -> tuple[float, float]:
        with self.lock:
            table_history = self._get_table_history()
            return table_history[table_name][instance_id]

    def get_column_times(
        self, column_name: str, instance_id: str, table_name: str
    ) -> tuple[float, float]:
        with self.lock:
            column_history = self._get_column_history()
            table_history = self._get_table_history()
            mat_time = column_history[table_name][instance_id][column_name]
            start_time = table_history[table_name][instance_id][1]
            return mat_time, start_time

    def get_last_table_update(
        self,
        table_name: str,
        version: str = "",
        before_time: Optional[int] = None,
    ) -> tuple[float, float, str]:
        """
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and
        it can only have one active version.
        """
        with self.lock:
            table_history = self._get_table_history()
            max_changed_time = 0
            max_start_time = 0
            max_id = ""
            for instance_id, (changed_time, start_time) in table_history[
                table_name
            ].items():
                if version != "" and not instance_id.startswith(version):
                    continue
                if start_time > max_start_time and (
                    before_time is None or start_time < before_time
                ):
                    max_start_time = start_time
                    max_changed_time = changed_time
                    max_id = instance_id
            return max_changed_time, max_start_time, max_id

    def get_last_column_update(
        self, table_name: str, column: str, before_time: Optional[int] = None
    ) -> tuple[float, float, str]:
        """
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only
        have one active version.
        """
        with self.lock:
            columns_history = self._get_column_history()
            table_history = self._get_table_history()
            max_start_time = 0
            max_mat_time = 0
            max_id = 0
            for instance_id in columns_history[table_name]:
                if column in columns_history[table_name][instance_id]:
                    mat_time = columns_history[table_name][instance_id][column]
                    _, start_time = table_history[table_name][instance_id]
                    if mat_time > max_mat_time and (
                        before_time is None or mat_time < before_time
                    ):
                        max_mat_time = mat_time
                        max_start_time = start_time
                        max_id = instance_id
            return max_mat_time, max_start_time, max_id

    def print_active_processes(
        self, print_all: bool
    ) -> None:
        with self.lock:
            active_logs = self._get_active_logs()
            if print_all:
                pprint.pprint(active_logs)
            elif print:
                print(active_logs)
            
    def get_active_processes(
        self, print: bool = True, print_all: bool = False
    ) -> ActiveProcessDict:
        with self.lock:
            active_logs = self._get_active_logs()
            if print_all:
                pprint.pprint(active_logs)
            elif print:
                #logs = "\n".join(list(active_logs.keys()))
                print(active_logs)
            else:
                return active_logs

    def get_table_instances(
        self, table_name: str, instance_id: str, to_print: bool = True
    ) -> None | list[str]:
        with self.lock:
            table_history = self._get_table_history()
            instances = list(table_history[table_name].keys())
            if instance_id != "":
                instances_ = [
                    instance
                    for instance in instances
                    if instance.startswith(instance_id)
                ]
                instances = instances_
            if to_print:
                instances = "\n".join(instances)
                print(instances)
            else:
                return instances

    def check_table_existance(
        self, table_name: str, instance_id: str = "", column_name: str = ""
    ) -> bool:
        with self.lock:
            column_history = self._get_column_history()
            exists = False
            if table_name in column_history:
                if instance_id == "" and column_name != "":
                    raise TVArgumentError(
                        "Cannot determine column name without instance id."
                    )
                elif instance_id == "" and column_name == "":
                    exists = True
                elif (
                    instance_id != ""
                    and column_name == ""
                    and instance_id in column_history[table_name]
                ):
                    exists = True
                elif (
                    column_name != ""
                    and column_name in column_history[table_name][instance_id]
                ):
                    exists = True
            return exists
    

    def check_completed_log(self, process_id: str) -> bool:
        with self.lock:
            self._check_written(process_id)

    def update_process_pid(self, process_id:str, 
                           pid:int, 
                           force:bool = False) -> int:
        with self.lock:
            logs = self._get_active_logs()
            if process_id not in logs:
                raise TVProcessError("Process ID not active {process_id}")
            old_pid = logs[process_id].pid
            if force:
                logs[process_id].pid = pid
                self._save_active_logs(logs)
                return old_pid
            try:
                psutil.Process(old_pid)
                raise TVProcessError("Process ID {process_id} already running at: {pid}")
            except psutil.NoSuchProcess:
                logs[process_id].pid = pid
                self._save_active_logs(logs)
                return old_pid
