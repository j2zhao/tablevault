"""
For each operation we want to be able to write the logs to db, and fetch locks

"""

from tablevault._defintions import tv_errors, constants
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper.metadata_store import MetadataStore
from tablevault._operations._setup_operations import SETUP_MAP
from tablevault._operations._takedown_operations import TAKEDOWN_MAP
from tablevault._operations._table_execution import execute_instance
import inspect
from tablevault._helper.utils import gen_tv_id
from typing import Callable
from tablevault._helper.file_operations import get_prompts, get_db_prompts
import os
import logging
import multiprocessing
import psutil
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def filter_by_function_args(kwargs:dict, 
                            func:Callable):
    func_params = list(inspect.signature(func).parameters.keys())
    args = {key: kwargs[key] for key in func_params if key in kwargs}
    return args

def background_instance_execution(process_id, db_dir):
    # TODO: make cleaner somehow?
    db_metadata = MetadataStore(db_dir)
    db_locks = DatabaseLock(process_id, db_dir)
    funct_kwargs = db_metadata.get_active_processes()[process_id].data
    funct_kwargs["db_metadata"] = db_metadata
    funct_kwargs["process_id"] = process_id
    funct_kwargs = filter_by_function_args(funct_kwargs, execute_instance)
    try:
        execute_instance(**funct_kwargs)
    except tv_errors.TableVaultError as e:
        error = (e.__class__.__name__, str(e))
        db_metadata.update_process_execution_status(process_id, success=False, error=error)
        TAKEDOWN_MAP[constants.EXECUTE_OP](process_id, db_metadata, db_locks, False)
        db_metadata.write_process(process_id)
        raise e
    db_metadata.update_process_execution_status(process_id, success=True)
    TAKEDOWN_MAP[constants.EXECUTE_OP](process_id, db_metadata, db_locks, True)
    db_metadata.write_process(process_id)

def tablevault_operation(tablevault_func: Callable) -> Callable:
    def wrapper(**kwargs):
        db_metadata = MetadataStore(kwargs["db_dir"])
        kwargs["db_metadata"] = db_metadata
        op_name = tablevault_func.__name__
        if kwargs["process_id"] != "":
            process_id = kwargs["process_id"]
        else:
            process_id = gen_tv_id()
            kwargs["process_id"] = process_id
        db_locks = DatabaseLock(process_id, db_metadata.db_dir)
        logs = db_metadata.get_active_processes()
        funct_kwargs = None
        if process_id in logs:
            log = logs[process_id]
            db_metadata.update_process_pid(process_id, os.getpid())
            if log.execution_success is False:
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, False)
                db_metadata.write_process(process_id)
                if log.error is not None:
                    err = getattr(tv_errors, log.error[0], RuntimeError)
                    raise err(log.error[1])
            elif log.execution_success is True:
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks,  True)
                db_metadata.write_process(process_id)
                return
            elif log.start_success is False:
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, True)
                db_metadata.write_process(process_id)
                if log.error is not None:
                    err = getattr(tv_errors, log.error[0], RuntimeError)
                    raise err(log.error[1])
            elif log.start_success is True:
                funct_kwargs = logs[process_id].data
            elif log.start_success is None:
                start_time = log.start_time
        else:
            start_time = db_metadata.start_new_process(process_id, 
                                          kwargs["author"], 
                                          op_name,
                                          os.getpid()
                                          )
        if funct_kwargs == None:
            try:
                setup_func = SETUP_MAP[op_name]
                setup_func['start_time'] = start_time
                setup_kwargs["db_locks"] = db_locks
                setup_kwargs = filter_by_function_args(kwargs, setup_func)
                funct_kwargs = setup_func(**setup_kwargs)
            except Exception as e:
                error = (e.__class__.__name__, str(e))
                db_metadata.update_process_start_status(process_id, success=False, error=error)
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, False)
                db_metadata.write_process(process_id)
                raise e
            db_metadata.update_process_start_status(process_id, success=True)
        
        if op_name == constants.EXECUTE_OP and "background" in kwargs and kwargs["background"]:
            p = multiprocessing.Process(target=background_instance_execution, args=(process_id, db_metadata.db_dir))
            db_metadata.update_process_pid(process_id, p.pid, force=True)
            logger.info(f"Start background execution {op_name}: ({process_id}, {p.pid})")
            p.start()
        else:
            funct_kwargs["db_metadata"] = db_metadata
            funct_kwargs["process_id"] = process_id
            funct_kwargs = filter_by_function_args(funct_kwargs, tablevault_func)
            logger.info(f"Start execution {op_name}: {process_id}")
            try:
                logger.info(f"Start execution {op_name}: {process_id}")
                tablevault_func(**funct_kwargs)
            except tv_errors.TableVaultError as e:
                error = (e.__class__.__name__, str(e))
                db_metadata.update_process_execution_status(process_id, success=False, error=error)
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, False)
                db_metadata.write_process(process_id)
                raise e
            db_metadata.update_process_execution_status(process_id, success=True)
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, True)
            db_metadata.write_process(process_id)
    return wrapper

def stop_operation(process_id:str, db_dir:str, force:bool):
    db_metadata = MetadataStore(db_dir)
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        old_pid = logs[process_id].pid
        # get status
        try:
            proc = psutil.Process(old_pid)
            if not force:
                raise tv_errors.TVProcessError("Process {process_id} Currently Running. Cannot be stopped unless forced.")
            if force:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except psutil.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        except psutil.NoSuchProcess:
            pass
        db_locks = DatabaseLock(process_id, db_dir)
        TAKEDOWN_MAP[logs[process_id].operation](process_id, db_metadata, db_locks,  True)
        db_metadata.write_process(process_id)
    else:
        raise tv_errors.TVProcessError(f"Process {process_id} not active.")