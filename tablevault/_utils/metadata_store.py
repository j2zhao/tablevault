import json
import os
from dataclasses import dataclass, asdict
from dataclasses_json import dataclass_json
from typing import Optional, Any
import time
from filelock import FileLock
from tablevault._utils.errors import TVArgumentError, TVProcessError
import pprint
import mmap
from tablevault._utils.file_operations import delete_from_temp, cleanup_temp


@dataclass_json
@dataclass
class ProcessLog:
    process_id: str
    author: str
    start_time: float
    log_time: float
    table_name: str
    instance_id: str
    operation: str
    complete_steps: list[str]
    steps_data: list[dict[str, Any]]
    data: dict[str, Any]
    success: Optional[bool]
    parent_id: str

    def get_completed_step(self):
        last_index = 0
        for i in range(len(self.complete_steps) - 1, -1, -1):
            if self.complete_steps[i] == "restart_forced":
                last_index = i
                break
        return self.complete_steps[last_index:]


ColumnHistoryDict = dict[str, dict[str, dict[str, dict[str, float]]]]
TableHistoryDict = dict[str, dict[str, tuple[float, float]]]
TableMultipleDict = dict[str, bool]
ActiveProcessDict = dict[str, ProcessLog]


def _serialize_active_log(temp_logs: ActiveProcessDict) -> dict:
    serialized_logs = {key: value.to_dict() for key, value in temp_logs.items()}
    return serialized_logs


def _deserialize_active_log(serialized_logs: dict) -> ActiveProcessDict:
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
    valid_operations = [
        "copy_files",
        "delete_table",
        "delete_instance",
        "execute_instance",
        "setup_temp_instance",
        "setup_table",
        "copy_database_files",
        "restart_database",
    ]

    def _save_active_log(self, temp_logs: ActiveProcessDict) -> None:
        logs = _serialize_active_log(temp_logs)
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

    def _get_active_log(self) -> ActiveProcessDict:
        with open(self.active_file, "r") as file:
            data = json.load(file)
            temp_logs = _deserialize_active_log(data)
            return temp_logs

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

    def _write_to_log_entry(self, log_entry: ProcessLog) -> None:
        log_entry.log_time = time.time()
        log_entry = asdict(log_entry)
        with open(self.log_file, "a") as file:
            file.write(json.dumps(log_entry) + "\n")

    def _write_to_completed_log(self, process_id: str) -> None:
        with open(self.completed_file, "a") as file:
            file.write(process_id + "\n")

    def _check_completed_log(self, process_id: str) -> bool:
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
        table_name = log.table_name
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
        table_name = log.table_name
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
        instance_id = log.instance_id
        table_name = log.table_name
        if instance_id in table_history[table_name]:
            del table_history[table_name][instance_id]
        if instance_id in column_history[table_name]:
            del column_history[table_name][instance_id]
        self._save_table_history(table_history)
        self._save_column_history(column_history)

    def _execute_operation(self, log: ProcessLog) -> None:
        table_name = log.table_name
        # start_time = log.data["start_time"]
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

    def _write_to_log(
        self, process_id: str, success: bool = True, first_level=False
    ) -> None:
        logs = self._get_active_log()
        if process_id not in logs:
            return
        if first_level and logs[process_id].parent_id != "":
            return
        log = logs[process_id]
        if success is not None:
            log.success = success
        log.complete_steps.append("write_log")
        log.steps_data.append({"step_time": time.time()})
        log.log_time = time.time()
        self._save_active_log(logs)
        if log.operation == "setup_table":
            self._setup_table_operation(log)
        elif log.operation == "delete_table":
            self._delete_table_operation(log)
        elif log.operation == "delete_instance":
            self._delete_instance_operation(log)
        elif log.operation == "execute_instance" and success:
            self._execute_operation(log)
        elif log.operation in self.valid_operations:
            pass
        else:
            raise TVProcessError("Operation {log.operation} not supported")
        for log_id in logs:
            if logs[log_id].parent_id == process_id:
                self._write_to_log(log_id, success=success)
        self._write_to_log_entry(log)
        self._write_to_completed_log(process_id)
        del logs[process_id]
        self._save_active_log(logs)
        delete_from_temp(process_id, self.db_dir)

    def write_to_log(self, process_id: str, success: bool = True) -> None:
        with self.lock:
            self._write_to_log(process_id, success, first_level=True)

    def start_new_process(
        self,
        process_id: str,
        author: str,
        operation: str,
        table_name: str = "",
        instance_id: str = "",
        start_time: Optional[float] = None,
        data: dict[str, Any] = {},
        parent_id: str = "",
    ) -> str:
        with self.lock:
            active_processes = self._get_active_log()
            cleanup_temp(
                list(active_processes.keys()), self.db_dir
            )  # NOTE: Cleanup files
            if not start_time:
                start_time = time.time()
            if process_id in active_processes:
                raise TVProcessError(f"{process_id} already initiated")
            active_processes[process_id] = ProcessLog(
                process_id,
                author,
                start_time,
                start_time,
                table_name,
                instance_id,
                operation,
                [],
                [],
                data,
                None,
                parent_id,
            )
            self._save_active_log(active_processes)
            return process_id

    def update_process_data(self, process_id: str, data: dict) -> None:
        with self.lock:
            active_processes = self._get_active_log()
            active_processes[process_id].log_time = time.time()
            active_processes[process_id].data.update(data)
            self._save_active_log(active_processes)

    def _update_process_step_internal(
        self, process_id: str, step: str, data: dict
    ) -> None:
        active_processes = self._get_active_log()
        data["step_time"] = time.time()
        active_processes[process_id].complete_steps.append(step)
        active_processes[process_id].steps_data.append(data)
        active_processes[process_id].log_time = time.time()
        self._save_active_log(active_processes)

    def update_process_step(self, process_id: str, step: str, data: dict = {}) -> None:
        with self.lock:
            self._update_process_step_internal(process_id, step, data)

    def _delete_process_internal(self, process_id: str):
        active_processes = self._get_active_log()
        del active_processes[process_id]
        self._save_active_log(active_processes)

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

    def get_active_processes(
        self, print_all: bool = False, to_print: bool = True
    ) -> None | ActiveProcessDict:
        with self.lock:
            active_logs = self._get_active_log()
            if print_all and to_print:
                pprint.pprint(active_logs)
            elif to_print:
                logs = "\n".join(list(active_logs.keys()))
                print(logs)
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
            return self._check_completed_log(process_id)
