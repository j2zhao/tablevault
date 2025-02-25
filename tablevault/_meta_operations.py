"""
For each operation we want to be able to write the logs to db, and fetch locks

"""

from tablevault._utils.errors import TableVaultError
from tablevault._utils.database_lock import DatabaseLock, get_all_process_ids
from tablevault._utils.metadata_store import MetadataStore
from tablevault._setup_operations import SETUP_MAP
import inspect
from tablevault._utils.utils import gen_process_id
from typing import Callable
from tablevault._roll_back_operations import ROLLBACK_MAP


def filter_by_function_args(kwargs, func):
    func_params = inspect.signature(func).parameters
    return {key: kwargs[key] for key in func_params if key in kwargs}


def tablevault_operation(tablevault_func: Callable) -> Callable:
    def wrapper(**kwargs):
        db_metadata = MetadataStore(kwargs["db_dir"])
        cleanup_locks(db_metadata)
        kwargs["db_metadata"] = db_metadata
        operation = tablevault_func.__name__
        setup_func = SETUP_MAP[operation]
        if kwargs["process_id"] != "":
            process_id = kwargs["process_id"]
            if db_metadata.check_completed_log(process_id):
                print(f"{process_id} already complete")
                return
        else:
            process_id = gen_process_id()
            kwargs["process_id"] = process_id
        db_locks = DatabaseLock(process_id, db_metadata.db_dir)
        try:
            setup_kwargs = filter_by_function_args(kwargs, setup_func)
            setup_kwargs["db_locks"] = db_locks
            funct_kwargs = setup_func(**setup_kwargs)
            funct_kwargs["db_metadata"] = db_metadata
            funct_kwargs["process_id"] = process_id
            funct_kwargs["parent_id"] = kwargs["parent_id"]
            funct_kwargs = filter_by_function_args(funct_kwargs, funct_kwargs)
            tablevault_func(**funct_kwargs)
        except TableVaultError:
            if operation in ROLLBACK_MAP:
                ROLLBACK_MAP[operation](process_id, db_metadata)
            db_metadata.write_to_log(process_id, success=False)
            db_locks.release_all_locks()
            raise
        db_metadata.write_to_log(process_id, success=True)
        db_locks.release_all_locks()

    return wrapper


def cleanup_locks(db_metadata: MetadataStore):
    process_ids = get_all_process_ids(db_metadata.db_dir)
    active_ids = db_metadata.get_active_processes()
    for id in process_ids:
        if id not in active_ids:
            DatabaseLock(id, db_metadata.db_dir).release_all_locks()
