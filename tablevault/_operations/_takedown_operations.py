from tablevault._defintions import constants, tv_errors
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper.metadata_store import MetadataStore
from tablevault._helper import file_operations
from tablevault._helper.copy_write_file import CopyOnWriteFile
from tablevault._dataframe_helper import table_operations


def takedown_copy_files(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir, file_writer)
    file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
    db_locks.release_all_locks()


def takedown_rename_table(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        try:
            file_operations.rename_table(
                log.data["table_name"],
                log.data["new_table_name"],
                db_metadata.db_dir,
                file_writer,
            )
        except tv_errors.TVFileError:
            pass
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"])
    else:
        if "new_table_name" in log.data:
            db_locks.delete_lock_path(log.data["new_table_name"])
    db_locks.release_all_locks()


def takedown_delete_table(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir, file_writer)
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"])
    file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
    db_locks.release_all_locks()


def takedown_delete_instance(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir, file_writer)
        db_locks.make_lock_path(log.data["table_name"], log.data["instance_id"])
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
    db_locks.release_all_locks()


def takedown_materialize_instance(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        try:
            file_operations.rename_table_instance(
                log.data["instance_id"],
                log.data["perm_instance_id"],
                log.data["table_name"],
                db_metadata.db_dir,
                file_writer,
            )
        except Exception:
            pass
        try:
            file_operations.move_artifacts_from_table(
                log.data["instance_id"],
                log.data["perm_instance_id"],
                log.data["table_name"],
                db_metadata.db_dir,
                file_writer,
            )
        except Exception:
            pass
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir, file_writer)
    if log.start_success is False or log.execution_success is False:
        if (
            "table_name" in log.data and "perm_instance_id" in log.data
        ):  # there is a slight bug here where the path is created
            # but not logged (okay for now)
            db_locks.delete_lock_path(
                log.data["table_name"], log.data["perm_instance_id"]
            )
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
    db_locks.release_all_locks()


def takedown_write_instance_inner(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir, file_writer)
    file_operations.delete_from_temp(process_id, db_metadata.db_dir, file_writer)
    db_locks.release_all_locks()


def takedown_write_instance(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data and "perm_instance_id" in log.data:
            db_locks.delete_lock_path(
                log.data["table_name"], log.data["perm_instance_id"]
            )
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    db_locks.release_all_locks()


def takedown_execute_instance_inner(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = logs[process_id]
        if log.start_success is True and log.execution_success is False:
            log = db_metadata.get_active_processes()[process_id]
            table_operations.make_df(
                log.data["instance_id"],
                log.data["table_name"],
                db_metadata.db_dir,
                file_writer=file_writer,
            )
    db_locks.release_all_locks()


def takedown_execute_instance(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data and "perm_instance_id" in log.data:
            db_locks.delete_lock_path(
                log.data["table_name"], log.data["perm_instance_id"]
            )
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    db_locks.release_all_locks()


def takedown_create_instance(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        try:
            file_operations.delete_table_folder_2(
                log.data["table_name"],
                db_metadata.db_dir,
                file_writer,
                log.data["instance_id"],
            )
        except Exception:
            pass
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data and "instance_id" in log.data:
            db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    db_locks.release_all_locks()


def takedown_create_table(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        try:
            file_operations.delete_table_folder_2(
                log.data["table_name"], db_metadata.db_dir, file_writer
            )
        except FileNotFoundError:
            pass
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data:
            db_locks.delete_lock_path(log.data["table_name"])
    db_locks.release_all_locks()


def takedown_restart_database(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    db_locks.release_all_locks()


def takedown_stop_process(
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    db_locks.release_all_locks()


TAKEDOWN_MAP = {
    constants.CREATE_CODE_MODULE_OP: takedown_copy_files,
    constants.DELTE_CODE_MODULE_OP: takedown_copy_files,
    constants.CREATE_BUILDER_FILE_OP: takedown_copy_files,
    constants.DELETE_BUILDER_FILE_OP: takedown_copy_files,
    constants.RENAME_TABLE_OP: takedown_rename_table,
    constants.DELETE_TABLE_OP: takedown_delete_table,
    constants.DELETE_INSTANCE_OP: takedown_delete_instance,
    constants.MAT_OP: takedown_materialize_instance,
    constants.WRITE_INSTANCE_OP: takedown_write_instance,
    constants.WRITE_INSTANCE_INNER_OP: takedown_write_instance_inner,
    constants.EXECUTE_INNER_OP: takedown_execute_instance_inner,
    constants.EXECUTE_OP: takedown_execute_instance,
    constants.CREATE_INSTANCE_OP: takedown_create_instance,
    constants.CREATE_TABLE_OP: takedown_create_table,
    constants.RESTART_OP: takedown_restart_database,
    constants.STOP_PROCESS_OP: takedown_stop_process,
}
