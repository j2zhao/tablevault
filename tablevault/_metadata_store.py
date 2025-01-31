import json
import os
from dataclasses import dataclass, field, asdict
from dataclasses_json import dataclass_json
from typing import Optional, Union, Any, Dict
import time
from filelock import FileLock
import uuid
import pprint

#TODO: there is an edge case where i might write to log twice -> Okay for now


@dataclass_json
@dataclass
class ProcessLog():
    process_id: str
    author:str
    start_time:float
    log_time: float
    table_name: str
    instance_id: str
    restarts: list[tuple[str, float]]
    operation: str
    complete_steps: list[str]
    step_times: list[float]
    data: dict[str, Any]
    success: Optional[bool]


ColumnHistoryDict = dict[str, dict[str, dict[str, dict[str, float]]]]
TableHistoryDict = dict[str, dict[str, tuple[float, float]]]
TableMultipleDict = dict[str, bool]
ActiveProcessDict = Dict[str, ProcessLog]

def _serialize_active_log(temp_logs: ActiveProcessDict) -> dict:
    serialized_logs = {
            key: value.to_dict()  
            for key, value in temp_logs.items()
        }
    return serialized_logs

def _deserialize_active_log(serialized_logs: dict) -> ActiveProcessDict:
    deserialized_dict = {
                key: ProcessLog.from_dict(value)  
                for key, value in serialized_logs.items()
    }
    return deserialized_dict


class MetadataStore:
    def _save_active_log(self, temp_logs: ActiveProcessDict) -> None:
        logs = _serialize_active_log(temp_logs)
        with open(self.active_file, 'w') as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: ColumnHistoryDict) -> None:
        with open(self.column_history_file, 'w') as f:
            json.dump(columns_history, f, indent=4)
    
    def _save_table_history(self, table_history: TableHistoryDict):
        with open(self.table_history_file, 'w') as f:
            json.dump(table_history, f, indent=4)        
    
    def _save_table_multiple(self, table_multiples: TableMultipleDict):
        with open(self.table_multiple_file, 'w') as f:
            json.dump(table_multiples, f, indent=4)  

    def _get_active_log(self) -> ActiveProcessDict: 
        with open(self.active_file, 'r') as file:
            data = json.load(file)
            temp_logs = _deserialize_active_log(data) 
            return temp_logs

    def _get_column_history(self) -> ColumnHistoryDict:
        with open(self.column_history_file, 'r') as file:
            columns_history = json.load(file)
            return columns_history
    
    def _get_table_history(self) -> TableHistoryDict:
        with open(self.table_history_file, 'r') as file:
            table_history = json.load(file)
            return table_history
    
    def _get_table_multiple(self) -> TableMultipleDict:
        with open(self.table_multiple_file, 'r') as file:
            table_multiples = json.load(file)
            return table_multiples      
    
    def _write_to_log(self, log_entry: ProcessLog) -> None:
        log_entry.log_time = time.time()
        log_entry = asdict(log_entry)
        with open(self.log_file, 'a') as file:
            file.write(json.dumps(log_entry) + '\n')
        

    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, 'metadata')
        self.log_file = os.path.join(meta_dir, 'log.txt')
        self.column_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.table_history_file = os.path.join(meta_dir, 'tables_history.json')
        self.table_multiple_file = os.path.join(meta_dir, 'tables_multiple.json')
        self.table_start_file = os.path.join(meta_dir, 'tables_start.json')
        self.active_file = os.path.join(meta_dir, 'active_log.json')
        meta_lock = os.path.join(meta_dir, 'LOG.lock') 
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
        table_multiples[table_name] = log.data['allow_multiple']
        self._save_table_multiple(table_multiples)

    def _setup_instance_operation(self, log: ProcessLog) -> None:
        "Nothing happens -> temp instance shouldn't impact metadata"
        pass

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
        start_time = log.data['start_time']
        instance_id = log.data['perm_instance_id']
        changed_columns = log.data['to_change_columns']
        all_columns = log.data['all_columns']
        prev_instance_id = log.data['origin']
        table_history = self._get_table_history()
        if len(changed_columns) > 0:
            table_history[table_name][instance_id] = (start_time, log.log_time)
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
                columns_history[table_name][instance_id][column] = columns_history[table_name][prev_instance_id][column]
        self._save_column_history(columns_history)

    def _restart_operation(self, log: ProcessLog) -> None:
        "Nothing Happens For Now"
        pass

    def write_to_log(self, process_id:str, success:Optional[bool] = True):
        with self.lock:
            logs = self._get_active_log()
            log = logs[process_id]
            if success != None:
                log.success = success
            log.complete_steps.append('write_log')
            log.step_times.append(time.time())
            log.log_time = time.time()
            self._save_active_log(logs)
            if log.operation == 'setup_table': 
                self._setup_table_operation(log)
            elif log.operation == 'setup_table_instance': 
                self._setup_instance_operation(log)
            elif log.operation == 'delete_table':
                self._delete_table_operation(log)
            elif log.operation == 'delete_table_instance':
                self._delete_instance_operation(log)
            elif log.operation == 'restart_database':
                self._restart_operation(log)
            elif log.operation == 'execute_table' and success:
                self._execute_operation(log)
            elif log.operation == 'execute_table':
                pass
            else:
                raise NotImplementedError()
            self._write_to_log(log)
            del logs[process_id]
            self._save_active_log(logs)
    
    def start_new_process(self, author:str, operation: str, table_name:str, instance_id:str = '', start_time: Optional[float] = None, data:dict[str, Any] = {}) -> float:
        with self.lock:
            process_id = str(uuid.uuid4())
            active_processes = self._get_active_log()
            if not start_time:
                start_time = time.time()
            restarts = []
            active_processes[process_id] = ProcessLog(process_id, author, start_time, start_time, table_name, instance_id, restarts, operation, [], [], data, None)
            self._save_active_log(active_processes)
            return process_id
    
    def update_process_data(self, process_id:str, data:dict):
        with self.lock:
            active_processes = self._get_active_log()
            active_processes[process_id].log_time = time.time()
            active_processes[process_id].data.update(data)
            self._save_active_log(active_processes)
    
    def _update_process_step_internal(self,  process_id:str, step: str):
        active_processes = self._get_active_log()
        active_processes[process_id].complete_steps.append(step)
        active_processes[process_id].step_times.append(time.time())
        active_processes[process_id].log_time = time.time()
        self._save_active_log(active_processes)
    
    def update_process_step(self, process_id:str, step: str):
        with self.lock:
            self._update_process_step_internal(process_id, step)
    
    def update_process_restart(self, author:str, process_id:str) -> ProcessLog:
        with self.lock:
            restart_time = time.time()
            active_processes = self._get_active_log()
            active_processes[process_id].restarts.append((author, restart_time))
            active_processes[process_id].log_time = time.time()
            self._save_active_log(active_processes)
            return active_processes[process_id]
    
    def _delete_process_internal(self, process_id: str):
        active_processes = self._get_active_log()
        del active_processes[process_id]
        self._save_active_log(active_processes)
        

    def _get_multiple_internal(self, table_name):
        allow_multiples = self._get_table_multiple()
        return allow_multiples[table_name]
    
    def get_table_multiple(self, table_name:str):
        with self.lock:
            return self._get_multiple_internal(table_name)

    def get_table_times(self, instance_id:str, table_name:str) ->  tuple[float, float]:
        with self.lock:
            table_history = self._get_table_history() 
            return table_history[table_name][instance_id]

    def get_column_times(self, column_name, instance_id:str, table_name:str) -> tuple[float, float]:
        with self.lock:
            column_history = self._get_column_history() 
            table_history = self._get_table_history() 
            mat_time = column_history[table_name][instance_id][column_name]
            start_time = table_history[table_name][instance_id][1]
            return mat_time, start_time
   
    def get_last_table_update(self, table_name:str, version: Optional[str] = None, before_time: Optional[int] = None) -> tuple[float, float, str]:
        '''
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only have one active version.
        '''
        with self.lock:
            table_history = self._get_table_history() 
            max_mat_time = 0
            max_start_time = 0
            max_id = 0
            for instance_id, (mat_time, start_time) in table_history[table_name].items():
                if version != None and not instance_id.startswith(version):
                    continue
                if start_time > max_start_time and (before_time == None or start_time < before_time):
                    max_start_time = start_time
                    max_mat_time = mat_time
                    max_id = instance_id
            return max_mat_time, max_start_time, max_id
        
    def get_last_column_update(self, table_name:str, column:str, 
                               before_time: Optional[int] = None) -> tuple[float, float, str]:
        '''
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only have one active version.
        '''
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
                    if start_time >= max_start_time and (before_time == None or start_time < before_time):
                        max_mat_time = mat_time
                        max_start_time = start_time
                        max_id = instance_id
            return max_mat_time,max_start_time, max_id


    def get_active_processes(self) -> ActiveProcessDict:
        with self.lock:
            return self._get_active_log() 
        
    def print_active_processes(self, all = False) -> None:
        with self.lock:
            active_logs = self._get_active_log()
            if all:
                pprint(active_logs)
            else:
                logs = '\n'.join(list(active_logs.keys()))
                print(logs)

    def print_table_instances(self, table_name:str, instance_id:str) -> None:
        with self.lock:
            table_history = self._get_table_history()
            instances = list(table_history[table_name].keys())
            if instance_id != '':
                instances_ = [instance for instance in instances if instance.startswith(instance_id)]
                instances = instances_
            instances = '\n'.join(instances)
            print(instances)

    