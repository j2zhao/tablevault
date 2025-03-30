import json
import os
from typing import Optional, Any
import time
from filelock import FileLock
from tablevault.defintions.tv_errors import TVArgumentError, TVProcessError
import pprint
import mmap
from tablevault.defintions.types import ProcessLog, ColumnHistoryDict, TableHistoryDict, ActiveProcessDict, TableMetadataDict, InstanceMetadataDict
from dataclasses import asdict
from tablevault.defintions import constants
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

    def _save_table_meta(self, table_meta: TableMetadataDict):
        with open(self.table_meta_file, "w") as f:
            json.dump(table_meta, f, indent=4)
    
    def _save_instance_meta(self, instance_meta: InstanceMetadataDict):
        with open(self.instance_meta_file, "w") as f:
            json.dump(instance_meta, f, indent=4)

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

    def _get_table_meta(self) -> TableMetadataDict:
        with open(self.table_meta_file, "r") as file:
            table_meta = json.load(file)
            return table_meta
    
    def _get_instance_meta(self) -> InstanceMetadataDict:
        with open(self.instance_meta_file, "r") as file:
            instance_meta = json.load(file)
            return instance_meta

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
        self.column_history_file = os.path.join(meta_dir, constants.META_CHIST_FILE)
        self.table_history_file = os.path.join(meta_dir, constants.META_THIST_FILE)
        self.table_meta_file = os.path.join(meta_dir, constants.META_TABLE_FILE)
        self.instance_meta_file = os.path.join(meta_dir, constants.META_INSTANCE_FILE)
        self.active_file = os.path.join(meta_dir, constants.META_ALOG_FILE)
        self.completed_file = os.path.join(meta_dir, constants.META_CLOG_FILE)
        meta_lock = os.path.join(meta_dir, "LOG.lock")
        self.lock = FileLock(meta_lock)

    def start_execute_operation(self, table_name:str) -> None:
        with self.lock:
            start_time = time.time()
            table_metadata = self._get_table_meta()
            table_history = self._get_table_history()
            if table_metadata[table_name][constants.TABLE_SIDE_EFFECTS]:
                for id in table_history[table_name]:
                    changed_time, mat_time, stop_time = table_history[table_name][id]
                    if stop_time is None:
                        table_history[table_name][id] = (changed_time, mat_time, start_time)
            self._save_table_history(table_history)

    def _setup_table_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        columns_history = self._get_column_history()
        columns_history[table_name] = {}
        self._save_column_history(columns_history)
        table_history = self._get_table_history()
        table_history[table_name] = {}
        self._save_table_history(table_history)
        table_metadata = self._get_table_meta()
        table_metadata[table_name] = {}
        table_metadata[table_name][constants.TABLE_ALLOW_MARTIFACT] = log.data[constants.TABLE_ALLOW_MARTIFACT]
        table_metadata[table_name][constants.TABLE_SIDE_EFFECTS] = log.data[constants.TABLE_SIDE_EFFECTS]
        self._save_table_meta(table_metadata)
        instance_metadata = self._get_instance_meta()
        instance_metadata[table_name] = {}
        self._save_instance_meta(instance_metadata)

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
        table_metadata = self._get_table_meta()
        del table_metadata[table_name]
        self._save_table_meta(table_metadata)
        instance_metadata = self._get_instance_meta()
        del instance_metadata[table_name]
        self._save_instance_meta(instance_metadata)

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
        instance_metadata = self._get_instance_meta()
        del instance_metadata[table_name][instance_id]
        self._save_instance_meta(instance_metadata)

    def _setup_instance_operation(self, log:ProcessLog) -> None:
        table_name = log.data["table_name"]
        instance_id = log.data["instance_id"]
        prev_id = log.data["prev_id"]
        instance_metadata = self._get_instance_meta()
        instance_metadata[table_name][instance_id] = {}
        instance_metadata[table_name][instance_id][constants.INSTANCE_ORIGIN] = prev_id
        self._save_instance_meta(instance_metadata)

    def _execute_operation(self, log: ProcessLog) -> None:
        table_name = log.data["table_name"]
        perm_instance_id = log.data["perm_instance_id"]
        changed_columns = log.data["to_change_columns"]
        all_columns = log.data["all_columns"]
        prev_instance_id = log.data["prev_instance_id"]
        temp_instance_id = log.data["instance_id"]
        table_history = self._get_table_history()
        table_metadata = self._get_table_meta()
        instance_metadata = self._get_instance_meta()
        instance_metadata[table_name][perm_instance_id] = instance_metadata[table_name][temp_instance_id]
        del instance_metadata[table_name][temp_instance_id]
        self._save_instance_meta(instance_metadata)
        
        if len(changed_columns) > 0:
            table_history[table_name][perm_instance_id] = (log.log_time, log.log_time, None)
        else:
            prev_changed_time = table_history[table_name][prev_instance_id][0]
            table_history[table_name][perm_instance_id] = (prev_changed_time, log.log_time, None)
        if table_metadata[table_name][constants.TABLE_SIDE_EFFECTS]:
            for id in table_history[table_name]:
                changed_time, mat_time, stop_time = table_history[table_name][id]
                if stop_time is None:
                    table_history[table_name][id] = (changed_time, mat_time, log.log_time)
        self._save_table_history(table_history)
        columns_history = self._get_column_history()
        columns_history[table_name][perm_instance_id] = {}
        for column in all_columns:
            if column in changed_columns:
                columns_history[table_name][perm_instance_id][column] = log.log_time
            else:
                columns_history[table_name][perm_instance_id][column] = columns_history[
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
                pass
                if log.operation == constants.SETUP_TABLE_INNER_OP:
                    self._setup_table_operation(log)
                if log.operation == constants.SETUP_TEMP_INNER_OP:
                    self._setup_instance_operation(log)
                elif log.operation == constants.DELETE_TABLE_OP:
                    self._delete_table_operation(log)
                elif log.operation == constants.DELETE_INSTANCE_OP:
                    self._delete_instance_operation(log)
                elif log.operation ==  constants.EXECUTE_OP:
                    self._execute_operation(log)
            else:
                pass
            self._write_to_history(log)
            self._write_to_completed(process_id)
            del logs[process_id]
            self._save_active_logs(logs)
            logger.info(f"Completed {log.operation}: {process_id}")

    def check_written(self, process_id: str) -> bool:
        with self.lock:
            return _is_string_in_file(self.completed_file, process_id)
    
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
                [],
                {},
                None,
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
            log.start_success = success
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

    def _get_table_property(self, table_name:str, property:str) -> Any:
        table_meta = self._get_table_meta()
        if table_name == '':
            return table_meta
        if table_name not in table_meta:
            raise TVArgumentError("Table Name Does not exist")
        if property == "":
            return table_meta[table_name]
        else:
            return table_meta[table_name][property]
        
    def _get_instance_property(self, table_name: str, instance_id: str, property:str) -> Any:
        with self.lock:
            instance_meta = self._get_instance_meta()
            if table_name not in instance_meta:
                raise TVArgumentError("Table Name Does not exist")
            if instance_id not in instance_meta[table_name]:
                raise TVArgumentError("Instance ID doesn't exist")
            if property == "":
                return instance_meta[table_name][instance_id]
            else:
                return instance_meta[table_name][instance_id][property]

    def get_table_property(self, table_name: str = '', property:str='', instance_id:str = '') -> Any:
        with self.lock:
            if instance_id == '':
                return self._get_table_property(table_name, property)
            else:
                return self._get_instance_property(table_name, instance_id, property)
        

    def get_table_times(self, instance_id: str, table_name: str) -> tuple[float, float, float]:
        with self.lock:
            table_history = self._get_table_history()
            return table_history[table_name][instance_id]

    def get_column_times(
        self, column_name: str, instance_id: str, table_name: str
    ) -> tuple[float, float, float]:
        with self.lock:
            column_history = self._get_column_history()
            table_history = self._get_table_history()
            mat_time = column_history[table_name][instance_id][column_name]
            _, start_time, end_time = table_history[table_name][instance_id]
            return mat_time, start_time, end_time
        
    def _get_last_table_update(
        self,
        table_name: str,
        version: str = "",
        before_time: Optional[int] = None,
        active_only:bool = True
    ) -> tuple[float, float, str]:
        table_history = self._get_table_history()
        allow_multiple_artifacts = self._get_table_property(table_name, constants.TABLE_ALLOW_MARTIFACT)
        max_changed_time = 0
        max_start_time = 0
        max_id = ""
        for instance_id, (changed_time, start_time, end_time) in table_history[
            table_name
        ].items():
            if version != "" and not instance_id.startswith(version):
                continue
            if active_only and not allow_multiple_artifacts and end_time is not None:
                    continue
            if start_time > max_start_time and (
                before_time is None or start_time < before_time
            ):
                max_start_time = start_time
                max_changed_time = changed_time
                max_id = instance_id
        if max_id == 0:
            raise TVArgumentError(f"Cannot find instance for table_name: {table_name}, version: {version}")
        return max_changed_time, max_start_time, max_id

    def get_last_table_update(
        self,
        table_name: str,
        version: str = "",
        before_time: Optional[int] = None,
        active_only:bool = True
    ) -> tuple[float, float, str]:
        return self._get_last_table_update(table_name,version, before_time, active_only) 
    
    def get_last_column_update(
        self, table_name: str, column: str, before_time: Optional[int] = None,
        version: str = "base", active_only:bool = True
    ) -> tuple[float, float, str]:
        """
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only
        have one active version.
        """
        with self.lock:
            _, max_start_time, max_id = self._get_last_table_update(table_name,version, before_time, active_only) 
            columns_history = self._get_column_history()
            max_mat_time = columns_history[table_name][max_id][column]
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
        self
    ) -> ActiveProcessDict:
        with self.lock:
            active_logs = self._get_active_logs()
            return active_logs

    def get_table_instances(
        self, table_name: str, version: str
    ) -> None | list[str]:
        with self.lock:
            table_history = self._get_table_history()
            instances = list(table_history[table_name].keys())
            instances.sort(key = lambda x: table_history[table_name][1])
            if version != "":
                instances_ = [
                    instance
                    for instance in instances
                    if instance.startswith(version)
                ]
                instances = instances_
            else:
                return instances

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
