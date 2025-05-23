from tablevault.defintions import constants, tv_errors
from tablevault.helper.database_lock import DatabaseLock
from tablevault.helper.metadata_store import MetadataStore
from tablevault.helper import file_operations


def takedown_copy_files(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


def takedown_delete_table(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"])
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


def takedown_delete_instance(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
        db_locks.make_lock_path(log.data["table_name"], log.data["instance_id"])
    if log.execution_success is True:
        db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


def takedown_materialize_instance(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        file_operations.delete_from_temp(process_id, db_metadata.db_dir)
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        try:
            file_operations.rename_table_instance(
                log.data["instance_id"],
                log.data["perm_instance_id"],
                log.data["table_name"],
                db_metadata.db_dir,
            )
        except tv_errors.TVFileError:
            pass
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
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
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


def takedown_write_table_inner(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


def takedown_write_table(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    db_locks.release_all_locks()


def takedown_execute_instance_inner(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    db_locks.release_all_locks()


def takedown_execute_instance(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    db_locks.release_all_locks()


def takedown_setup_temp_instance(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.start_success is False:
        if "table_name" in log.data and "instance_id" in log.data:
            db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    db_locks.release_all_locks()


def takedown_setup_temp_instance_innner(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.execution_success is False:
        file_operations.delete_table_folder_2(
            log.data["table_name"], db_metadata.db_dir, log.data["instance_id"]
        )
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data and "instance_id" in log.data:
            db_locks.delete_lock_path(log.data["table_name"], log.data["instance_id"])
    db_locks.release_all_locks()


def takedown_setup_table(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    logs = db_metadata.get_active_processes()
    if process_id in logs:
        log = db_metadata.get_active_processes()[process_id]
    else:
        db_locks.release_all_locks()
        return
    if log.start_success is False:
        if "table_name" in log.data:
            db_locks.delete_lock_path(log.data["table_name"])
    db_locks.release_all_locks()


def takedown_setup_table_inner(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
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
                log.data["table_name"], db_metadata.db_dir
            )
        except FileNotFoundError:
            pass
    if log.start_success is False or log.execution_success is False:
        if "table_name" in log.data:
            db_locks.delete_lock_path(log.data["table_name"])
    db_locks.release_all_locks()


def takedown_copy_database_files(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    db_locks.release_all_locks()


def takedown_restart_database(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    db_locks.release_all_locks()


def takedown_stop_process(
    process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
):
    pass


TAKEDOWN_MAP = {
    constants.COPY_FILE_OP: takedown_copy_files,
    constants.DELETE_TABLE_OP: takedown_delete_table,
    constants.DELETE_INSTANCE_OP: takedown_delete_instance,
    constants.MAT_OP: takedown_materialize_instance,
    constants.WRITE_TABLE_OP: takedown_write_table,
    constants.WRITE_TABLE_INNER_OP: takedown_write_table_inner,
    constants.EXECUTE_INNER_OP: takedown_execute_instance_inner,
    constants.EXECUTE_OP: takedown_execute_instance,
    constants.SETUP_TEMP_OP: takedown_setup_temp_instance,
    constants.SETUP_TABLE_OP: takedown_setup_table,
    constants.COPY_DB_OP: takedown_copy_database_files,
    constants.RESTART_OP: takedown_restart_database,
    constants.SETUP_TEMP_INNER_OP: takedown_setup_temp_instance_innner,
    constants.SETUP_TABLE_INNER_OP: takedown_setup_table_inner,
    constants.STOP_PROCESS_OP: takedown_stop_process,
}
