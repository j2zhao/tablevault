from tablevault._helper import file_operations
from tablevault._helper.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._operations._meta_operations import tablevault_operation, stop_operation
from tablevault._operations import _table_execution
from tablevault._defintions.types import Prompt, ExternalDeps
from tablevault._operations._takedown_operations import TAKEDOWN_MAP
from tablevault._defintions import constants
import os


def setup_database(db_dir: str, replace: bool = False) -> None:
    file_operations.setup_database_folder(db_dir, replace)

def print_active_processes(db_dir: str, print_all: bool) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.print_active_processes(print_all)

def active_processes(db_dir: str) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_active_processes(to_print=False)

def list_instances(table_name: str, db_dir: str, version: str) -> list[str]:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version, to_print=False)


def stop_process(process_id: str, db_dir: str, force: bool):
    stop_operation(process_id, db_dir, force)


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
    top_pnames: list[str],
    to_change_columns: list[str],
    all_columns: list[str],
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
        top_pnames,
        to_change_columns,
        all_columns,
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
    prompt_names: list[str],
    execute: bool,
    process_id: str,
    db_metadata: MetadataStore,
    step_ids: list[str],
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        file_operations.setup_table_instance_folder(
            instance_id, table_name, db_metadata.db_dir, prev_id, prompt_names
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
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
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
            prompt_names=True,
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
            create_temp= execute,
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
    for prid in active_processes:
        try:
            if active_processes[prid].operation == constants.COPY_FILE_OP:
                copy_files(
                    author=process_id,
                    table_name="",
                    file_dir="",
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.DELETE_TABLE_OP:
                delete_table(
                    author=process_id,
                    table_name="",
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.DELETE_INSTANCE_OP:
                delete_instance(
                    author=process_id,
                    table_name="",
                    instance_id="",
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.EXECUTE_OP:
                execute_instance(
                    author=process_id,
                    table_name="",
                    version="",
                    force_execute=False,
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.SETUP_TEMP_OP:
                setup_temp_instance(
                    author=process_id,
                    version="",
                    table_name="",
                    prev_id="",
                    copy_previous=False,
                    prompt_names=[],
                    execute=False,
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.SETUP_TABLE_OP:
                setup_table(
                    author=process_id,
                    table_name="",
                    yaml_dir="",
                    execute=False,
                    allow_multiple=False,
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            elif active_processes[prid].operation == constants.COPY_DB_OP:
                copy_database_files(
                    author=process_id,
                    yaml_dir="",
                    code_dir="",
                    execute=False,
                    allow_multiple_tables=[],
                    process_id=prid,
                    db_dir=db_metadata.db_dir,
                )
            db_metadata.update_process_step(process_id, step=prid)
        except:
            continue
