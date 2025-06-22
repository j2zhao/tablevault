from tablevault._defintions import tv_errors, constants
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper.metadata_store import MetadataStore, check_top_process
from tablevault._operations._takedown_operations import TAKEDOWN_MAP
from tablevault._operations._setup_operations import SETUP_MAP
from tablevault._operations._table_execution import execute_instance
from tablevault._helper.copy_write_file import CopyOnWriteFile
import inspect
from tablevault._helper.utils import gen_tv_id
from typing import Callable, Any
import os
import logging
import multiprocessing

logger = logging.getLogger(__name__)


def filter_by_function_args(kwargs: dict, func: Callable) -> dict[str, Any]:
    func_params = list(inspect.signature(func).parameters.keys())
    args = {key: kwargs[key] for key in func_params if key in kwargs}
    return args


def background_instance_execution(
    process_id: str, db_dir: str, force_takedown: bool, has_hardlink: bool
) -> None:
    file_writer = CopyOnWriteFile(db_dir, has_hardlink=has_hardlink)  # TODO
    db_metadata = MetadataStore(db_dir)
    db_locks = DatabaseLock(process_id, db_dir)
    funct_kwargs = db_metadata.get_active_processes()[process_id].data
    funct_kwargs["db_metadata"] = db_metadata
    funct_kwargs["process_id"] = process_id
    funct_kwargs["file_writer"] = file_writer
    funct_kwargs = filter_by_function_args(funct_kwargs, execute_instance)
    try:
        execute_instance(**funct_kwargs)
    except tv_errors.TableVaultError as e:
        error = (e.__class__.__name__, str(e))
        db_metadata.update_process_execution_status(
            process_id, success=False, error=error
        )
        TAKEDOWN_MAP[constants.EXECUTE_OP](
            process_id, db_metadata, db_locks, file_writer
        )
        db_metadata.write_process(process_id)
        raise
    except Exception as e:
        if force_takedown:
            error = (e.__class__.__name__, str(e))
            db_metadata.update_process_execution_status(
                process_id, success=False, error=error
            )
            TAKEDOWN_MAP[constants.EXECUTE_OP](
                process_id, db_metadata, db_locks, file_writer
            )
            db_metadata.write_process(process_id)
        raise
    db_metadata.update_process_execution_status(process_id, success=True)
    TAKEDOWN_MAP[constants.EXECUTE_OP](process_id, db_metadata, db_locks, file_writer)
    db_metadata.write_process(process_id)


def tablevault_operation(
    author: str,
    op_name: str,
    op_funct: Callable,
    db_dir: str,
    process_id: str,
    file_writer: CopyOnWriteFile,
    setup_kwargs: dict[str, Any],
    parent_id: str,
    background: bool = False,
) -> str:
    db_metadata = MetadataStore(db_dir)
    logs = db_metadata.get_active_processes()
    if process_id == "":
        process_id = gen_tv_id()
        if parent_id != "":
            process_id = parent_id + "_" + process_id
            force_takedown = logs[parent_id].force_takedown
        else:
            force_takedown = True
    else:
        active_ids = list(logs.keys())
        if process_id in active_ids:
            _, top_parent_id = check_top_process(process_id, active_ids)
            if parent_id != "" and not process_id.startswith(parent_id):
                if top_parent_id != process_id:
                    raise tv_errors.TVProcessError(
                        "Can only modify/run subprocesses or top-level processes"
                    )
            force_takedown = logs[top_parent_id].force_takedown
        else:
            if parent_id != "" and not process_id.startswith(parent_id + "_"):
                process_id = parent_id + "_" + process_id
            _, top_parent_id = check_top_process(process_id, active_ids)
            if top_parent_id in active_ids:
                force_takedown = logs[top_parent_id].force_takedown
            else:
                force_takedown = False
    db_locks = DatabaseLock(process_id, db_metadata.db_dir)
    funct_kwargs = None
    if process_id in logs:
        log = logs[process_id]
        if "background" in log.data:
            background = log.data["background"]
        db_metadata.update_process_pid(process_id, os.getpid())
        if log.execution_success is False:
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
            db_metadata.write_process(process_id)
            if log.error is not None:
                err = getattr(tv_errors, log.error[0], RuntimeError)
                raise err(log.error[1])
        elif log.execution_success is True:
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
            db_metadata.write_process(process_id)
            return process_id
        elif log.start_success is False:
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
            db_metadata.write_process(process_id)
            if log.error is not None:
                err = getattr(tv_errors, log.error[0], RuntimeError)
                raise err(log.error[1])
        elif log.start_success is True:
            funct_kwargs = logs[process_id].data
        elif log.start_success is None:
            start_time = log.start_time
    else:
        start_time = db_metadata.start_new_process(
            process_id, author, op_name, os.getpid(), force_takedown
        )
    if background:
        db_metadata.update_process_data(process_id, {"background": background})
    if funct_kwargs is None:
        try:
            setup_kwargs["start_time"] = start_time
            setup_kwargs["db_locks"] = db_locks
            setup_kwargs["db_metadata"] = db_metadata
            setup_kwargs["process_id"] = process_id
            setup_kwargs["file_writer"] = file_writer
            setup_kwargs["author"] = author
            setup_kwargs = filter_by_function_args(setup_kwargs, SETUP_MAP[op_name])
            funct_kwargs = SETUP_MAP[op_name](**setup_kwargs)
        except tv_errors.TableVaultError as e:
            error = (e.__class__.__name__, str(e))
            db_metadata.update_process_start_status(
                process_id, success=False, error=error
            )
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
            db_metadata.write_process(process_id)
            raise
        except Exception as e:
            if force_takedown:
                error = (e.__class__.__name__, str(e))
                db_metadata.update_process_start_status(
                    process_id, success=False, error=error
                )
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
                db_metadata.write_process(process_id)
            raise

        db_metadata.update_process_start_status(process_id, success=True)

    if op_name == constants.EXECUTE_OP and background:
        p = multiprocessing.Process(
            target=background_instance_execution,
            args=(
                process_id,
                db_metadata.db_dir,
                force_takedown,
                file_writer._hardlink_supported,
            ),
        )
        p.start()
        db_metadata.update_process_pid(process_id, p.pid, force=True)
        logger.info(f"Start background execution {op_name}: ({process_id}, {p.pid})")
    else:
        funct_kwargs["db_metadata"] = db_metadata
        funct_kwargs["process_id"] = process_id
        funct_kwargs["file_writer"] = file_writer
        funct_kwargs["author"] = author
        funct_kwargs = filter_by_function_args(funct_kwargs, op_funct)
        logger.info(f"Start execution {op_name}: {process_id}")
        try:
            op_funct(**funct_kwargs)
        except tv_errors.TableVaultError as e:
            error = (e.__class__.__name__, str(e))
            db_metadata.update_process_execution_status(
                process_id, success=False, error=error
            )
            TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
            db_metadata.write_process(process_id)
            raise
        except Exception as e:
            if force_takedown:
                error = (e.__class__.__name__, str(e))
                db_metadata.update_process_execution_status(
                    process_id, success=False, error=error
                )
                TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
                db_metadata.write_process(process_id)
            raise
        db_metadata.update_process_execution_status(process_id, success=True)
        TAKEDOWN_MAP[op_name](process_id, db_metadata, db_locks, file_writer)
        db_metadata.write_process(process_id)
    return process_id
