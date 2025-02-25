from tablevault._utils import file_operations
from tablevault._utils.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._meta_operations import tablevault_operation
from tablevault._utils.database_lock import DatabaseLock
from tablevault import _table_execution
from tablevault._prompt_parsing.types import Prompt, ExternalDeps
from tablevault._roll_back_operations import ROLLBACK_MAP
import os


def setup_database(db_dir: str, replace: bool = False):
    file_operations.setup_database_folder(db_dir, replace)


def active_processes(db_dir: str, all_info: bool) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_active_processes(all_info, to_print=False)


def list_instances(table_name: str, db_dir: str, version: str) -> list[str]:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version, to_print=False)


def _stop_process(
    process_id: str, db_metadata: MetadataStore, processes: ActiveProcessDict
):
    process = processes[process_id]
    if "step_ids" in process.data:
        for pid in process.data["step_ids"]:
            if pid in processes:
                _stop_process(pid, db_metadata, processes)
    if process.operation in ROLLBACK_MAP:
        ROLLBACK_MAP[process.operation](process_id, db_metadata)


def stop_process(process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    db_metadata.update_process_step(process_id, step="rollback")
    processes = db_metadata.get_active_processes()
    _stop_process(process_id, db_metadata, processes)
    db_metadata.write_to_log(process_id, success=False)
    db_locks = DatabaseLock(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()


@tablevault_operation
def copy_files(file_dir: str, table_name: str, db_metadata: MetadataStore):
    file_operations.copy_files(file_dir, table_name, db_metadata.db_dir)


@tablevault_operation
def delete_table(table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir)


@tablevault_operation
def delete_instance(instance_id: str, table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir, instance_id)


@tablevault_operation
def execute_instance(
    table_name: str,
    instance_id: str,
    perm_instance_id: str,
    prompts: dict[Prompt],
    top_pnames: list[str],
    to_change_columns: list[str],
    all_columns: list[str],
    column_dtypes: dict[str, str],
    external_deps: ExternalDeps,
    prev_instance_id: str,
    prev_completed_steps: list[str],
    update_rows: bool,
    process_id: str,
    db_metadata: MetadataStore,
):
    _table_execution.execute_instance(
        table_name,
        instance_id,
        perm_instance_id,
        prompts,
        top_pnames,
        to_change_columns,
        all_columns,
        column_dtypes,
        external_deps,
        prev_instance_id,
        prev_completed_steps,
        update_rows,
        process_id,
        db_metadata,
    )


@tablevault_operation
def setup_temp_instance(
    version: str,
    instance_id: str,
    table_name: str,
    prev_id: str,
    prompts: list[str],
    execute: bool,
    process_id: str,
    db_metadata: MetadataStore,
    step_ids: list[str],
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    if step_ids[0] not in complete_steps:
        file_operations.setup_table_instance_folder(
            instance_id, table_name, db_metadata.db_dir, prev_id, prompts
        )
        db_metadata.update_process_step(process_id, step_ids[0])
    if execute and step_ids[1] not in complete_steps:
        execute_instance(
            table_name=table_name,
            version=version,
            author=process_id,
            force_restart=False,
            force_execute=False,
            process_id=step_ids[1],
            db_dir=db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[1])


@tablevault_operation
def setup_table(
    table_name: str,
    yaml_dir: str,
    execute: bool,
    create_temp: bool,
    process_id: str,
    db_metadata: MetadataStore,
    step_ids: list[str],
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    if step_ids[0] not in complete_steps:
        file_operations.setup_table_folder(table_name, db_metadata.db_dir)
        db_metadata.update_process_step(process_id, step_ids[0])
    if yaml_dir != "" and step_ids[1] not in complete_steps:
        copy_files(
            author=process_id,
            table_name=table_name,
            file_dir=yaml_dir,
            process_id=step_ids[1],
            db_dir=db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[1])
    if yaml_dir and create_temp and step_ids[2] not in complete_steps:
        setup_temp_instance(
            author=process_id,
            version="",
            table_name=table_name,
            prev_id="",
            copy_previous=False,
            prompts=True,
            execute=execute,
            process_id=step_ids[2],
            db_dir=db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[2])


@tablevault_operation
def copy_database_files(
    yaml_dir: str,
    table_names: list[str],
    code_dir: str,
    execute: bool,
    allow_multiple_tables: list,
    process_id: str,
    db_metadata: MetadataStore,
    step_ids: list[str],
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    index = 0
    if code_dir != "" and step_ids[index] not in complete_steps:
        copy_files(
            author=process_id,
            table_name="",
            file_dir=code_dir,
            process_id=step_ids[1],
            db_dir=db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[index])
        index += 1
    for tname in table_names:
        if tname in allow_multiple_tables:
            allow_multiple = True
        else:
            allow_multiple = False
        pdir = os.path.join(yaml_dir, tname)
        setup_table(
            author=process_id,
            table_name=tname,
            yaml_dir=pdir,
            execute=execute,
            allow_multiple=allow_multiple,
            process_id=step_ids[index],
            db_dir=db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[index])
        index += 1


@tablevault_operation
def restart_database(
    process_id: str,
    db_metadata: MetadataStore,
):
    active_processes = db_metadata.get_active_processes()
    for pid in active_processes:
        data = active_processes[process_id].data
        if active_processes[pid].operation == "copy_files":
            copy_files(
                author=process_id,
                table_name="",
                file_dir="",
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "delete_table":
            delete_table(
                author=process_id,
                table_name="",
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "delete_instance":
            delete_instance(
                author=process_id,
                table_name="",
                instance_id="",
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "execute_instance":
            execute_instance(
                author=process_id,
                table_name=data["table_name"],
                version=data["version"],
                force_restart=False,
                force_execute=False,
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "setup_temp_instance":
            setup_temp_instance(
                author=process_id,
                version="",
                table_name="",
                prev_id="",
                copy_previous=False,
                prompts=[],
                execute=False,
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "setup_table":
            setup_table(
                author=process_id,
                table_name="",
                yaml_dir="",
                execute=False,
                allow_multiple=False,
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[pid].operation == "copy_database_files":
            copy_database_files(
                author=process_id,
                yaml_dir="",
                code_dir="",
                execute=False,
                allow_multiple_tables=[],
                process_id=pid,
                db_dir=db_metadata.db_dir,
            )
        db_metadata.update_process_step(process_id, step=pid)
