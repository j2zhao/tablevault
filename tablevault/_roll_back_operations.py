from tablevault._utils.metadata_store import MetadataStore
from tablevault._utils.file_operations import (
    copy_temp_to_folder,
    delete_table_folder,
    delete_database_folder,
)


def rollback_copy_files(process_id: str, db_metadata: MetadataStore):
    db_metadata.update_process_step(process_id, step="rollback")
    process = db_metadata.get_active_processes()[process_id]
    if process.table_name != "":
        copy_temp_to_folder(
            process_id,
            db_dir=db_metadata.db_dir,
            table_name=process.table_name,
            subfolder="prompts",
        )
    else:
        copy_temp_to_folder(
            process_id, db_dir=db_metadata.db_dir, subfolder="code_functions"
        )


def rollback_delete_table(process_id: str, db_metadata: MetadataStore):
    db_metadata.update_process_step(process_id, step="rollback")
    process = db_metadata.get_active_processes()[process_id]
    copy_temp_to_folder(
        process_id, db_dir=db_metadata.db_dir, table_name=process.table_name
    )


def rollback_delete_instance(process_id: str, db_metadata: MetadataStore):
    db_metadata.update_process_step(process_id, step="rollback")
    process = db_metadata.get_active_processes()[process_id]
    copy_temp_to_folder(
        process_id,
        db_dir=db_metadata.db_dir,
        instance_id=process.instance_id,
        table_name=process.table_name,
    )


def rollback_execute_instance(process_id: str, db_metadata: MetadataStore):
    return


def rollback_setup_temp_instance(process_id: str, db_metadata: MetadataStore):
    db_metadata.update_process_step(process_id, step="rollback")
    process = db_metadata.get_active_processes()[process_id]
    copy_temp_to_folder(
        process_id,
        db_dir=db_metadata.db_dir,
        instance_id=process.instance_id,
        table_name=process.table_name,
    )


def rollback_setup_table(process_id: str, db_metadata: MetadataStore):
    db_metadata.update_process_step(process_id, step="rollback")
    process = db_metadata.get_active_processes()[process_id]
    delete_table_folder(process.table_name, db_metadata.db_dir)


def rollback_database(process_id: str, db_metadata: MetadataStore):
    delete_database_folder(db_metadata.db_dir)


ROLLBACK_MAP = {
    "copy_files": rollback_copy_files,
    "delete_table": rollback_delete_table,
    "delete_instance": rollback_delete_instance,
    "execute_instance": rollback_execute_instance,
    "setup_temp_instance": rollback_setup_temp_instance,
    "setup_table": rollback_setup_table,
    "copy_database_files": rollback_database,
}
