from tablevault.helper import file_operations, database_lock, utils
from tablevault.helper.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._operations._meta_operations import stop_operation, tablevault_operation
from tablevault._operations import _table_execution
from tablevault.defintions.types import ExternalDeps
from tablevault.prompts.utils import artifact, table_operations
from tablevault.defintions import constants
import os


def setup_database(db_dir: str, replace: bool = False) -> None:
    file_operations.setup_database_folder(db_dir, replace)

def print_active_processes(db_dir: str, print_all: bool) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.print_active_processes(print_all)

def active_processes(db_dir: str) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_active_processes()

def complete_process(process_id: str, db_dir: str) -> bool:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.check_written(process_id)

def list_instances(table_name: str, db_dir: str, version: str) -> list[str]:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version)

def fetch_table(instance_id:str, version:str, table_name:str, db_dir:str, active_only:bool, safe_locking: bool):
    db_metadata = MetadataStore(db_dir)
    if instance_id == '':
        _ , _ , instance_id = db_metadata.get_last_table_update(table_name, version, active_only=active_only)
    if safe_locking:
        process_id = utils.gen_tv_id()
        db_lock = database_lock.DatabaseLock(process_id, db_dir)
        db_lock.acquire_shared_lock(table_name, instance_id)
    try:
        df = table_operations.get_table(instance_id, table_name, db_dir)
    finally:
        if safe_locking:
            db_lock.release_all_locks()
    return df

def stop_process(process_id: str, db_dir: str, force: bool):
    stop_operation(process_id, db_dir, force)

#tablevault_operation
def _copy_files(file_dir: str, table_name: str, db_metadata: MetadataStore):
    if table_name == '':
        sub_folder = constants.CODE_FOLDER
    else:
        sub_folder = constants.PROMPT_FOLDER
    file_operations.copy_files(file_dir, sub_folder, "", table_name, db_metadata.db_dir)

def copy_files(author:str,
               table_name:str,
               file_dir:str,
               process_id:str,
               db_dir:str):
    setup_kwargs = {
        'file_dir': file_dir,
        'table_name': table_name
    }
    return tablevault_operation(author,
                        constants.COPY_FILE_OP,
                        _copy_files,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )

#tablevault_operation
def _delete_table(table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir)

def delete_table(author:str,
               table_name:str,
               process_id:str,
               db_dir:str):
    setup_kwargs = {
        'table_name': table_name
    }
    return tablevault_operation(author,
                        constants.DELETE_TABLE_OP,
                        _delete_table,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )

#tablevault_operation
def _delete_instance(instance_id: str, table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir, instance_id)

def delete_instance(author:str,
               table_name:str,
               instance_id: str,
               process_id:str,
               db_dir:str):
    setup_kwargs = {
        'table_name': table_name,
        'instance_id':instance_id
    }
    return tablevault_operation(author,
                        constants.DELETE_INSTANCE_OP,
                        _delete_instance,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )

def _execute_instance(
    table_name: str,
    instance_id: str,
    perm_instance_id: str,
    top_pnames: list[str],
    to_change_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
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
        prev_completed_steps,
        update_rows,
        process_id,
        db_metadata,
    )

def execute_instance(author:str,
               table_name:str,
               version: str,
               force_execute:str,
               process_id:str,
               db_dir:str,
               background:bool):
    setup_kwargs = {
        'table_name': table_name,
        'version':version,
        'force_execute':force_execute
    }
    return tablevault_operation(author,
                        constants.EXECUTE_OP,
                        _execute_instance,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        background)


def _setup_temp_instance_inner(instance_id:str,
                               table_name:str,
                               prev_id:str,
                               prompt_names: list[str],
                               db_metadata: MetadataStore):
    file_operations.setup_table_instance_folder(
            instance_id, table_name, db_metadata.db_dir, prev_id, prompt_names
        )


def setup_temp_instance_inner(author:str,
                              table_name:str,
                              instance_id:str,
                              prev_id:str,
                              prompt_names: list[str],
                              process_id: str,
                              db_dir:str):
    setup_kwargs = {
        "table_name":table_name,
        "instance_id": instance_id,
        "prev_id":prev_id,
        "prompt_names":prompt_names
    }
    return tablevault_operation(author=author,
                         op_name=constants.SETUP_TEMP_INNER_OP,
                         op_funct=_setup_temp_instance_inner,
                        db_dir=db_dir, 
                        process_id=process_id,
                        setup_kwargs=setup_kwargs
                         )


def _setup_temp_instance(
    version: str,
    instance_id: str,
    table_name: str,
    prev_id: str,
    prompt_names: list[str],
    execute: bool,
    background_execute: bool,
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        setup_temp_instance_inner(author=process_id,
                                  instance_id=instance_id,
                                  table_name=table_name,
                                  prev_id=prev_id,
                                  prompt_names=prompt_names,
                                  process_id=step_ids[0],
                                  db_dir=db_metadata.db_dir)
        db_metadata.update_process_step(process_id, step_ids[0])
    if execute and step_ids[1] not in complete_steps:
        execute_instance(
            table_name=table_name,
            version=version,
            author=process_id,
            force_execute=False,
            process_id=step_ids[1],
            db_dir=db_metadata.db_dir,
            background=background_execute
        )
        db_metadata.update_process_step(process_id, step_ids[1])

def setup_temp_instance(author:str,
               table_name:str,
               version: str,
               prev_id:str,
               copy_version: bool,
               prompt_names: list[str] | bool,
               execute: bool,
               process_id:str,
               db_dir:str,
               background_execute:bool):
    setup_kwargs = {
        'table_name': table_name,
        'version':version,
        'prev_id': prev_id,
        'copy_version': copy_version, #TODO
        'prompt_names': prompt_names,
        'execute': execute,
        'background_execute': background_execute
    }
    return tablevault_operation(author,
                        constants.SETUP_TEMP_OP,
                        _setup_temp_instance,
                        db_dir, 
                        process_id,
                        setup_kwargs)

def _setup_table_inner(table_name:str,
                       db_metadata: MetadataStore,):
    file_operations.setup_table_folder(table_name, db_metadata.db_dir)

def setup_table_inner(author:str,
                      table_name:str,
                      allow_multiple_artifacts:bool,
                      has_side_effects:bool,
                      process_id:str,
                      db_dir:str
                    ):
    setup_kwargs = {
        'table_name': table_name,
        'allow_multiple_artifacts':allow_multiple_artifacts,
        'has_side_effects':has_side_effects,
    }
    return tablevault_operation(author,
                        constants.SETUP_TABLE_INNER_OP,
                        _setup_table_inner,
                        db_dir, 
                        process_id,
                        setup_kwargs)

def _setup_table(
    table_name: str,
    yaml_dir: str,
    create_temp: bool,
    execute: bool,
    background_execute: bool,
    allow_multiple_artifacts: bool,
    has_side_effects: bool,
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        setup_table_inner(author=process_id,
                      table_name=table_name,
                      allow_multiple_artifacts=allow_multiple_artifacts,
                      has_side_effects=has_side_effects,
                      process_id=step_ids[0],
                      db_dir=db_metadata.db_dir
                    )
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
            copy_version=False,
            prompt_names=True,
            execute=execute,
            process_id=step_ids[2],
            db_dir=db_metadata.db_dir,
            background_execute=background_execute,
        )
        db_metadata.update_process_step(process_id, step_ids[2])

def setup_table(author:str,
               table_name:str,
               create_temp: bool,
               execute:bool,
               background_execute:bool,
               allow_multiple_artifacts: bool,
               has_side_effects: bool,
               yaml_dir: str,
               process_id:str,
               db_dir:str,
               ):
    setup_kwargs = {
        'table_name': table_name,
        'create_temp':create_temp,
        'execute': execute,
        'background_execute': background_execute,
        'allow_multiple_artifacts':allow_multiple_artifacts,
        'has_side_effects':has_side_effects,
        'yaml_dir': yaml_dir,
    }
    return tablevault_operation(author,
                        constants.SETUP_TABLE_OP,
                        _setup_table,
                        db_dir, 
                        process_id,
                        setup_kwargs)

def _copy_database_files(
    yaml_dir: str,
    table_names: list[str],
    code_dir: str,
    execute: bool,
    allow_multiple_artifacts: list[str],
    has_side_effects: list[str],
    step_ids: list[str],
    background_execute:bool,
    process_id: str,
    db_metadata: MetadataStore,
):  
    # print(step_ids)
    # print(table_names)
    # print(yaml_dir)
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
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
        allow_m_artifacts = tname in allow_multiple_artifacts
        has_s_effects = tname in has_side_effects
        pdir = os.path.join(yaml_dir, tname)
        setup_table(
            author=process_id,
            table_name=tname,
            yaml_dir=pdir,
            create_temp= execute,
            execute=execute,
            allow_multiple_artifacts=allow_m_artifacts,
            has_side_effects=has_s_effects,
            process_id=step_ids[index],
            db_dir=db_metadata.db_dir,
            background_execute=background_execute
        )
        db_metadata.update_process_step(process_id, step_ids[index])
        index += 1

def copy_database_files(author:str,
               yaml_dir:str,
               code_dir: str,
               execute:bool,
               allow_multiple_artifacts: list[str],
               has_side_effects: list[str],
               process_id:str,
               db_dir:str,
               background_execute: bool):
    setup_kwargs = {
        'yaml_dir': yaml_dir,
        'code_dir': code_dir,
        'execute': execute,
        'background_execute': background_execute,
        'allow_multiple_artifacts': allow_multiple_artifacts,
        'has_side_effects': has_side_effects,
    }
    return tablevault_operation(author,
                        constants.COPY_DB_OP,
                        _copy_database_files,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )
    
def _restart_database(
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

def restart_database(
        author:str,
        process_id: str,
        db_metadata: MetadataStore,
        ):
    return tablevault_operation(author,
                        constants.RESTART_OP,
                        _restart_database,
                        db_metadata.db_dir, 
                        process_id,
                        setup_kwargs={},
                        )
