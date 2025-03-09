from tablevault.defintions import constants
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper.metadata_store import MetadataStore
from tablevault._helper import file_operations

def takedown_copy_files(process_id:str,
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    if not execution_success:
        try:
            file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
        except:
            pass
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()

def takedown_delete_table(process_id:str, 
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    args = db_metadata.get_active_processes()[process_id].data

    if not execution_success:
        try:
            file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
            if "table_name" in args:
                db_locks.make_lock_path(args["table_name"])
        except:
            pass
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    if execution_success:
        db_locks.delete_lock_path(args["table_name"])
    db_locks.release_all_locks()

def takedown_delete_instance(process_id:str,
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    args = db_metadata.get_active_processes()[process_id].data
    if not execution_success:
        try:
            file_operations.copy_temp_to_db(process_id, db_metadata.db_dir)
            if "instance_id" in args:
                db_locks.make_lock_path(args["table_name"], args["instance_id"])
        except:
            pass
    
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    if execution_success:
        db_locks.delete_lock_path(args["table_name"], args["instance_id"])
    db_locks.release_all_locks()

def takedown_execute_instance(process_id:str, 
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    args = db_metadata.get_active_processes()[process_id].data
    if not execution_success:
        if 'instance_id' in args:
            db_locks.make_lock_path(args["table_name"], args["instance_id"])
        if "perm_instance_id" in args:
            db_locks.delete_lock_path(args["table_name"], args["perm_instance_id"])
    if execution_success:
        db_locks.delete_lock_path(args["table_name"], args["instance_id"])
    db_locks.release_all_locks()

def takedown_setup_temp_instance(process_id:str, 
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    args = db_metadata.get_active_processes()[process_id].data
    if not execution_success:
        if 'instance_id' in args:
            db_locks.make_lock_path(args["table_name"], args["instance_id"])
        try:
            file_operations.delete_table_folder(args["table_name"], db_metadata.db_dir, args["instance_id"])
        except:
            pass
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()

def takedown_setup_table(process_id:str, 
                        db_metadata:MetadataStore, 
                        db_locks:DatabaseLock, 
                        execution_success: bool):
    args = db_metadata.get_active_processes()[process_id].data
    if not execution_success:
        if "table_name" in args:
            db_locks.make_lock_path(args["table_name"])
        try:
            file_operations.delete_table_folder(args["table_name"], db_metadata.db_dir)
        except:
            pass
    file_operations.delete_from_temp(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()

def takedown_copy_database_files(process_id:str, 
                        db_metadata:MetadataStore, db_locks:DatabaseLock, execution_success: bool):
    db_locks.release_all_locks()

def takedown_restart_database(process_id:str, db_metadata:MetadataStore, db_locks:DatabaseLock, execution_success: bool):
    db_locks.release_all_locks()

TAKEDOWN_MAP = {
    constants.COPY_FILE_OP: takedown_copy_files,
    constants.DELETE_TABLE_OP: takedown_delete_table,
    constants.DELETE_INSTANCE_OP: takedown_delete_instance,
    constants.EXECUTE_OP: takedown_execute_instance,
    constants.SETUP_TEMP_OP: takedown_setup_temp_instance,
    constants.SETUP_TABLE_OP: takedown_setup_table,
    constants.COPY_DB_OP: takedown_copy_database_files,
    constants.RESTART_OP: takedown_restart_database,
}