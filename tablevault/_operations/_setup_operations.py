from tablevault.helper.metadata_store import MetadataStore
from tablevault.defintions import tv_errors
from tablevault.helper.database_lock import DatabaseLock
from tablevault.helper import file_operations
from tablevault.helper.utils import gen_tv_id
from tablevault.defintions import constants, types 
from tablevault.defintions.types import SETUP_OUTPUT, ExternalDeps
from tablevault.prompts.base_ptype import TVPrompt, order_tables_by_prompts
from tablevault.prompts.load_prompt import load_prompt
from tablevault.prompts.utils.utils import topological_sort
from tablevault.prompts.utils import table_operations
import pandas as pd
from typing import Optional

def setup_copy_files(
    file_dir: str,
    table_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if table_name == "":
        db_locks.acquire_exclusive_lock(constants.CODE_FOLDER)
        file_operations.copy_folder_to_temp(process_id, 
                                            db_metadata.db_dir,
                                            subfolder=constants.CODE_FOLDER)
    else:
        db_locks.acquire_exclusive_lock(table_name, constants.PROMPT_FOLDER)
        file_operations.copy_folder_to_temp(process_id, 
                                            db_metadata.db_dir,
                                            table_name=table_name,
                                            subfolder=constants.PROMPT_FOLDER)
    funct_kwargs = {"file_dir": file_dir, "table_name": table_name}
    db_metadata.update_process_data(process_id,funct_kwargs)
    return funct_kwargs


def setup_delete_table(
    table_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    db_locks.acquire_exclusive_lock(table_name)
    file_operations.copy_folder_to_temp(process_id, 
                                        db_metadata.db_dir,
                                        table_name=table_name)
    funct_kwargs = {"table_name": table_name}
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_delete_instance(
    table_name: str,
    instance_id: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    file_operations.copy_folder_to_temp(process_id, 
                                        db_metadata.db_dir,
                                        instance_id=instance_id,
                                        table_name=table_name)
    funct_kwargs = {"table_name": table_name, "instance_id": instance_id}
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs

def setup_materialize_instance(instance_id:str,
                                table_name: str,
                                version:str,
                                perm_instance_id:str,
                                origin_id:str,
                                origin_table:str,
                                changed_columns: list[str],
                                all_columns: list[str],
                                artifact_columns: list[str],
                                dependencies: list[tuple[str, str]],
                                process_id: str,
                                db_metadata: MetadataStore,
                                db_locks: DatabaseLock):
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    
    if "_" not in process_id:
        instance_id = constants.TEMP_INSTANCE + version
        if perm_instance_id == "":
            start_time = db_metadata.get_active_processes()[process_id].start_time
            perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
            perm_instance_id = version + perm_instance_id

    
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    db_locks.make_lock_path(table_name, perm_instance_id)
    db_locks.acquire_exclusive_lock(table_name, perm_instance_id)
    table_data = file_operations.get_description(instance_id = "", table_name=table_name, db_dir=db_metadata.db_dir)
    
    if "_" not in process_id:
        instance_data = file_operations.get_description(instance_id, table_name, db_metadata.db_dir)
        if not instance_data[constants.DESCRIPTION_EDIT]:
            raise tv_errors.TVArgumentError("External edit table cannot be executed for this Instance.")
        if constants.DESCRIPTION_ORIGIN in instance_data:
            origin_id, origin_table = instance_data[constants.DESCRIPTION_ORIGIN]
        table_df = table_operations.get_table(instance_id, table_name, db_metadata.db_dir)
        if origin_id != "":
            try:
                temp_lock = db_locks.acquire_shared_lock(origin_table, origin_id)
                changed_columns = table_operations.check_changed_columns(table_df, origin_id, origin_table)
                db_locks.release_lock(temp_lock)
            except tv_errors.TVLockError:
                pass
        
    if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
        db_locks.acquire_exclusive_lock(table_name, constants.ARTIFACT_FOLDER)
        file_operations.copy_folder_to_temp(process_id, 
                                    db_metadata.db_dir,
                                    table_name=table_name,
                                    subfolder=constants.ARTIFACT_FOLDER)
    funct_kwargs = {
                    "instance_id": instance_id, 
                    "table_name": table_name,
                    "perm_instance_id": perm_instance_id, 
                    "origin_id": origin_id,
                    "origin_table": origin_table,
                    "artifact_columns": artifact_columns,
                    "all_columns": all_columns,
                    "changed_columns": changed_columns,
                    "dependencies": dependencies}
    
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs

def setup_stop_process(to_stop_process_id:str,
                      force: bool,
                      materialize:bool,
                      process_id:str,
                      db_metadata: MetadataStore,
                      db_locks: DatabaseLock):
    logs = db_metadata.get_active_processes()
    process_ids = []
    step_ids = []
    step_ids.append(process_id + '_' + gen_tv_id())

    instance_ids = []
    table_names = []
    perm_instance_ids = []
    origin_ids = []
    origin_tables = []
    artifact_columns = []
    all_columns = []
    changed_columns = []
    dependencies = []
    for process_id_ in logs:
        if process_id_.startswith(to_stop_process_id):
            process_ids.append(process_id_)
            if materialize and logs[process_id_].start_success:
                if logs[process_id_].operation == constants.EXECUTE_OP:
                    step_ids.append(process_id + '_' + gen_tv_id())
                    instance_ids.append(logs[process_id_].data["instance_id"])
                    table_names.append(logs[process_id_].data["table_name"])
                    perm_instance_ids.append(logs[process_id_].data["perm_instance_id"])
                    origin_ids.append(logs[process_id_].data["origin_id"])
                    origin_tables.append(logs[process_id_].data["origin_table"])
                    artifact_columns.append([])
                    all_columns.append(logs[process_id_].data["all_columns"])
                    changed_columns.append(logs[process_id_].data["changed_columns"])
                    dependencies.append(logs[process_id_].data["dependencies"])
                elif logs[process_id_].operation == constants.WRITE_TABLE_OP:
                    step_ids.append(process_id + '_' + gen_tv_id())
                    instance_ids.append(logs[process_id_].data["instance_id"])
                    table_names.append(logs[process_id_].data["table_name"])
                    perm_instance_ids.append(logs[process_id_].data["perm_instance_id"])
                    origin_ids.append("")
                    origin_tables.append("")
                    artifact_columns.append(logs[process_id_].data["artifact_columns"])
                    all_columns.append(logs[process_id_].data["all_columns"])
                    changed_columns.append(logs[process_id_].data["changed_columns"])
                    dependencies.append(logs[process_id_].data["dependencies"])
                elif logs[process_id_].operation == constants.MAT_OP:
                    if logs[process_id_].data["top_level"]:
                        step_ids.append(process_id + '_' + gen_tv_id())
                        instance_ids.append(logs[process_id_].data["instance_id"])
                        table_names.append(logs[process_id_].data["table_name"])
                        perm_instance_ids.append(logs[process_id_].data["perm_instance_id"])
                        origin_ids.append(logs[process_id_].data["origin_id"])
                        origin_tables.append(logs[process_id_].data["origin_table"])
                        artifact_columns.append(logs[process_id_].data["artifact_columns"])
                        all_columns.append(logs[process_id_].data["all_columns"])
                        changed_columns.append(logs[process_id_].data["changed_columns"])
                        dependencies.append(logs[process_id_].data["dependencies"])
                    
    process_ids.sort(reverse=True)
    funct_kwargs = {
        "process_ids":process_ids,
        "instance_ids": instance_ids,
        "table_names": table_names,
        "perm_instance_ids": perm_instance_ids,
        "origin_ids": origin_ids,
        "origin_tables": origin_tables,
        "dependencies": dependencies,
        "force":force,
        "step_ids":step_ids
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs
                



def setup_write_table_inner(table_df: Optional[pd.DataFrame],
                            instance_id:str,
                            table_name:str,
                            process_id:str,
                            db_metadata: MetadataStore,
                            db_locks: DatabaseLock,):
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    funct_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "table_df": None
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    funct_kwargs["table_df"] = table_df
    return funct_kwargs

def setup_write_table(table_df:Optional[pd.DataFrame],
                      table_name:str,
                      version:str,
                      artifact_columns: list[str],
                      dependencies: list[tuple[str, str]],
                      process_id: str,
                      db_metadata: MetadataStore,
                      db_locks: DatabaseLock,):
    step_ids = []
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if len(table_df.columns) == 0 or len(table_df) == 0:
        raise tv_errors.TVArgumentError("Empty Table")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    instance_id = constants.TEMP_INSTANCE + version
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    instance_data = file_operations.get_description(instance_id, table_name, db_metadata.db_dir)
    if not instance_data[constants.DESCRIPTION_EDIT]:
        raise tv_errors.TVArgumentError("External edit table cannot be executed for this Instance.")

    step_ids.append(process_id + '_' + gen_tv_id())
    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    start_time = db_metadata.get_active_processes()[process_id].start_time
    perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
    perm_instance_id = version + perm_instance_id
    db_locks.make_lock_path(table_name, perm_instance_id)
    db_locks.acquire_exclusive_lock(table_name, perm_instance_id)
    if not table_data[constants.TABLE_ALLOW_MARTIFACT] and len(artifact_columns) > 0:
        db_locks.acquire_exclusive_lock(table_name, constants.ARTIFACT_FOLDER)
    step_ids.append(process_id + '_' + gen_tv_id())

    all_columns = list(table_df.columns)
    changed_columns = all_columns
    origin_id, origin_table = instance_data[constants.DESCRIPTION_EDIT]
    if origin_id != "":
        try:
            temp_lock = db_locks.acquire_shared_lock(origin_table, origin_id)
            changed_columns = table_operations.check_changed_columns(table_df, origin_id, origin_table)
            db_locks.release_lock(temp_lock)
        except tv_errors.TVLockError:
            pass
    
    funct_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "artifact_columns": artifact_columns,
        "dependencies":dependencies,
        "perm_instance_id": perm_instance_id,
        "step_ids": step_ids,
        "table_df": None,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "all_columns": all_columns,
        "changed_columns": changed_columns,
        "all_columns": list(table_df.columns) #TODO: STOPPED HERE
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    funct_kwargs["table_df"] = table_df
    return funct_kwargs


def setup_execute_instance_inner(instance_id:str,
                                 table_name:str,
                                 top_pnames: list[str],
                                 changed_columns: list[str],
                                 all_columns: list[str],
                                 external_deps: ExternalDeps,
                                 origin_id:str,
                                 origin_table:str,
                                 process_id: str,
                                 db_metadata: MetadataStore,
                                 db_locks: DatabaseLock,
                                 ):
    

    funct_kwargs = {
        "table_name": table_name,
        'instance_id': instance_id,
        "top_pnames": top_pnames,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "update_rows": True,
    }
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    if origin_id != "":
        db_locks.acquire_shared_lock(origin_table, origin_id)
    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            db_locks.acquire_shared_lock(table_name=table, instance_id=instance)
            table_data = file_operations.get_description("", table, db_metadata.db_dir)
            if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
                db_locks.acquire_shared_lock(table, constants.ARTIFACT_FOLDER)
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs

# artifacts -> means that we only have one version of the artifacts? -> NO that isn't how we should deal with artifacts....UGH.
# deal with side effect on datasets
def setup_execute_instance(
    table_name: str,
    version: str,
    force_execute: bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    start_time = db_metadata.get_active_processes()[process_id].start_time
    perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
    perm_instance_id = version + perm_instance_id
    instance_id = constants.TEMP_INSTANCE + version
    instance_data = file_operations.get_description(instance_id, table_name, db_metadata.db_dir)
    if not instance_data[constants.DESCRIPTION_EDIT]:
        raise tv_errors.TVArgumentError("Internal edit table cannot be executed.")

    db_metadata.update_process_data(process_id, {"perm_instance_id": perm_instance_id, 
                                                 "instance_id": instance_id, 
                                                 "table_name": table_name,
                                                 "version": version})
    db_locks.make_lock_path(table_name, perm_instance_id)

    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    if not table_data[constants.TABLE_SIDE_EFFECTS]:
        db_locks.acquire_exclusive_lock(table_name, instance_id)
        db_locks.acquire_exclusive_lock(table_name, perm_instance_id)
        if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
            db_locks.acquire_exclusive_lock(table_name, constants.ARTIFACT_FOLDER)
    else:
        db_locks.acquire_exclusive_lock(table_name)
            
    yaml_prompts = file_operations.get_yaml_prompts(instance_id, table_name, db_metadata.db_dir)
    prompts = {pname: load_prompt(yprompt) for pname, yprompt in yaml_prompts.items()}

    if not force_execute:
        origin_table = ""
        origin_id = ""
        
        if constants.DESCRIPTION_ORIGIN in instance_data:
            origin_table, origin_id = instance_data[constants.DESCRIPTION_ORIGIN]
        if origin_id == "":
            _, _, origin_id = db_metadata.get_last_table_update(
                table_name, version, before_time=start_time
            )
            origin_table = table_name
        if origin_id == "":
            _, _, origin_id = db_metadata.get_last_table_update(
                table_name, "", before_time=start_time
            )
            origin_table = table_name

        if origin_id != "":
            db_locks.acquire_shared_lock(origin_table, origin_id)
    else:
        origin_id = ''
        origin_table = ''
    (
        top_pnames,
        changed_columns,
        all_columns,
        internal_prompt_deps,
        external_deps,
    ) = parse_prompts(
        prompts,
        db_metadata,
        start_time,
        instance_id,
        table_name,
        origin_id,
        origin_table,
    )
    funct_kwargs = {
        "table_name": table_name,
        'instance_id': instance_id,
        "perm_instance_id": perm_instance_id,
        "top_pnames": top_pnames,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "update_rows": True,
        "internal_prompt_deps": internal_prompt_deps,
    }
    funct_kwargs["step_ids"] = [process_id + '_' + gen_tv_id()]
    funct_kwargs["step_ids"].append(process_id + '_' + gen_tv_id())

    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            db_locks.acquire_shared_lock(table_name=table, instance_id=instance)
            try:
                db_locks.acquire_shared_lock(table, constants.ARTIFACT_FOLDER)
            except tv_errors.TVLockError:
                pass

    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs

def setup_setup_temp_instance(
    table_name: str,
    version: str,
    origin_id: str,
    origin_table:str,
    external_edit:bool,
    copy_version: bool,
    prompt_names: list[str],
    execute: bool,
    background_execute:bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if external_edit:
        if len(prompt_names) > 0 or execute or background_execute or origin_id != "" or copy_version:
            raise tv_errors.TVArgumentError("Non-Executable Table does not have prompts, origin, or execute.")
    if background_execute and not execute:
        raise tv_errors.TVArgumentError("background_execute cannot be True when execute is False.")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    instance_id = constants.TEMP_INSTANCE + version
    start_time = db_metadata.get_active_processes()[process_id].start_time
    if copy_version:
        _, _, origin_id = db_metadata.get_last_table_update(
            table_name, version, before_time=start_time
        )
        origin_table = table_name
        db_locks.acquire_shared_lock(table_name, origin_id)
    elif origin_id != "":
        if origin_table == "":
            origin_table = table_name
        db_locks.acquire_shared_lock(origin_table, origin_id)
    elif len(prompt_names) != 0:
        db_locks.acquire_shared_lock(table_name, constants.PROMPT_FOLDER)
    funct_kwargs = {
        "version": version,
        "instance_id": instance_id,
        "table_name": table_name,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "external_edit":external_edit,
        "prompt_names": prompt_names,
        "execute": execute,
        "background_execute": background_execute
    }
    funct_kwargs["step_ids"] = [process_id + '_' + gen_tv_id()]
    if execute:
        funct_kwargs["step_ids"].append(process_id + '_' + gen_tv_id())
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.make_lock_path(table_name, instance_id)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    return funct_kwargs

def setup_setup_table(
    table_name: str,
    create_temp: bool,
    execute: bool,
    background_execute:bool,
    allow_multiple_artifacts: bool,
    has_side_effects: bool,
    yaml_dir: str,
    description:str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if execute and yaml_dir == "":
        raise tv_errors.TVArgumentError(f"Cannot Execute {table_name} without Prompts Directory")
    funct_kwargs = {
        "table_name": table_name,
        "yaml_dir": yaml_dir,
        "create_temp": create_temp,
        "execute": execute,
        "background_execute":background_execute,
        "allow_multiple_artifacts":allow_multiple_artifacts,
        "has_side_effects": has_side_effects,
        "description":description,
        
    }
    funct_kwargs["step_ids"] = [process_id + '_' + gen_tv_id()]
    if yaml_dir != "":
        funct_kwargs["step_ids"].append(process_id + '_' + gen_tv_id())
    if execute:
        funct_kwargs["step_ids"].append(process_id + '_' + gen_tv_id())
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.make_lock_path(table_name)
    db_locks.make_lock_path(table_name, constants.PROMPT_FOLDER)
    db_locks.make_lock_path(table_name, constants.ARTIFACT_FOLDER)
    db_locks.acquire_exclusive_lock(table_name)
    return funct_kwargs

def setup_copy_database_files(
    yaml_dir: str,
    code_dir: str,
    execute: bool,
    background_execute: bool,
    allow_multiple_artifacts: list[str],
    has_side_effects: list[str],
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    step_ids = []
    if code_dir != "":
        db_locks.acquire_exclusive_lock(constants.CODE_FOLDER)
        step_ids.append(process_id + '_' + gen_tv_id())
    table_names = []
    if yaml_dir != "":
        try:
            yaml_prompts = file_operations.get_external_yaml_prompts(yaml_dir)
            for table_name in yaml_prompts:
                prompts = {pname: load_prompt(yprompt) for pname, yprompt in yaml_prompts[table_name].items()}
                yaml_prompts[table_name] = prompts
            table_names = order_tables_by_prompts(yaml_prompts)
        except Exception as e:
            raise tv_errors.TVFileError(f"Error ordering {yaml_dir}: {e}")
        for table_name in table_names:
            db_locks.make_lock_path(table_name)
            db_locks.acquire_exclusive_lock(table_name)
            step_ids.append(process_id + '_' + gen_tv_id())
    funct_kwargs = {
        "yaml_dir": yaml_dir,
        "table_names": table_names,
        "code_dir": code_dir,
        "execute": execute,
        'allow_multiple_artifacts':allow_multiple_artifacts,
        'has_side_effects': has_side_effects,
        "step_ids": step_ids,
        'background_execute': background_execute,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_restart_database(db_locks: DatabaseLock
                           ) -> SETUP_OUTPUT:
    db_locks.acquire_exclusive_lock(constants.RESTART_LOCK)
    return {}

def setup_setup_temp_instance_innner(
        table_name:str,
        instance_id:str,
        origin_id:str,
        origin_table:str,
        external_edit:bool,
        prompt_names: list[str],
        process_id: str,
        db_metadata: MetadataStore,
        db_locks: DatabaseLock, 
    )-> SETUP_OUTPUT:
    if prev_table == "":
        prev_table = table_name
    funct_kwargs = {
        "table_name": table_name,
        "instance_id": instance_id,
        "origin_id": origin_id,
        "origin_table":origin_table,
        "external_edit":external_edit,
        "prompt_names": prompt_names,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    if origin_id != '':
        db_locks.acquire_shared_lock(table_name, origin_id)
    if len(prompt_names) > 0:
        db_locks.acquire_shared_lock(table_name, constants.PROMPT_FOLDER)
    return funct_kwargs

def setup_setup_table_inner(table_name:str,
                            allow_multiple_artifacts:bool,
                            has_side_effects:bool,
                            description: str,
                            process_id: str,
                            db_metadata: MetadataStore,
                            db_locks: DatabaseLock
                            )-> SETUP_OUTPUT :
    funct_kwargs = {
        'table_name': table_name,
        'description': description,
        constants.TABLE_ALLOW_MARTIFACT:allow_multiple_artifacts,
        constants.TABLE_SIDE_EFFECTS:has_side_effects,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.acquire_exclusive_lock(table_name)
    return funct_kwargs

SETUP_MAP = {
    constants.COPY_FILE_OP: setup_copy_files,
    constants.DELETE_TABLE_OP: setup_delete_table,
    constants.DELETE_INSTANCE_OP: setup_delete_instance,
    constants.MAT_OP: setup_materialize_instance,
    constants.STOP_PROCESS_OP: setup_stop_process,
    constants.WRITE_TABLE_OP: setup_write_table,
    constants.WRITE_TABLE_INNER_OP: setup_write_table_inner,
    constants.EXECUTE_INNER_OP: setup_execute_instance_inner,
    constants.EXECUTE_OP: setup_execute_instance,
    constants.SETUP_TEMP_OP: setup_setup_temp_instance,
    constants.SETUP_TABLE_OP: setup_setup_table,
    constants.COPY_DB_OP: setup_copy_database_files,
    constants.RESTART_OP: setup_restart_database,
    constants.SETUP_TEMP_INNER_OP: setup_setup_temp_instance_innner,
    constants.SETUP_TABLE_INNER_OP: setup_setup_table_inner,
    
}


def _parse_dependencies(
    prompts: dict[str, TVPrompt],
    table_name: str,
    start_time: float,
    db_metadata: MetadataStore,
) -> tuple[types.PromptDeps, types.InternalDeps, types.ExternalDeps]:
    table_generator = ""
    for prompt in prompts:
        if prompt.startswith(f"gen_{table_name}") and table_generator == "":
            table_generator = prompt
        elif prompt.startswith(f"gen_{table_name}") and table_generator != "":
            raise tv_errors.TVPromptError(
                f"Can only have one prompt that starts with: gen_{table_name}"
            )
    if table_generator == "":
        raise tv_errors.TVPromptError(
            f"Needs one generator prompt that starts with gen_{table_name}"
        )
    external_deps = {}
    internal_prompt_deps = {}
    internal_deps = {}
    gen_columns = prompts[table_generator].changed_columns
    for pname in prompts:
        external_deps[pname] = set()
        if pname != table_generator:
            internal_deps[pname] = set(gen_columns)
            internal_prompt_deps[pname] = {table_generator}
        else:
            internal_deps[pname] = set()
            internal_prompt_deps[pname] = set()

        for dep in prompts[pname].dependencies:
            if dep.table == constants.TABLE_SELF:
                internal_deps[pname].union(dep.columns)
                for pn in prompts:
                    for col in dep.columns:
                        if col in prompts[pn].changed_columns:
                            internal_prompt_deps[pname].add(pn)
                continue
            if dep.table == table_name:
                active_only = False
            else:
                active_only = True
            if dep.columns != None:
                for col in dep.columns:
                    if col != constants.TABLE_INDEX:
                        mat_time, _, instance = (
                                db_metadata.get_last_column_update(dep.table,
                                                                    col,
                                                                    start_time,
                                                                    version=dep.version,
                                                                    active_only=active_only)
                            )
                        external_deps[pname].add((dep.table, col, instance, mat_time, dep.version))
                    else:
                        mat_time, _, instance = db_metadata.get_last_table_update(
                                dep.table, start_time= start_time, version=dep.version, active_only=active_only
                            )
                        external_deps[pname].add((dep.table, None, instance, mat_time, dep.version))
                
            else:
                mat_time, _, instance = db_metadata.get_last_table_update(
                        dep.table, start_time= start_time, version=dep.version, active_only=active_only
                    )
                external_deps[pname].add((dep.table, None, instance, mat_time, dep.version))
        external_deps[pname] = list(external_deps[pname])
        internal_deps[pname] = list(internal_deps[pname])
        internal_prompt_deps[pname] = list(internal_prompt_deps[pname])
    return internal_prompt_deps, internal_deps, external_deps


def parse_prompts(
    prompts: dict[str, TVPrompt],
    db_metadata: MetadataStore,
    start_time: float,
    instance_id: str,
    table_name: str,
    origin_id: str,
    origin_table: str,
) -> tuple[list[str], list[str], list[str], list[str], types.InternalDeps, types.ExternalDeps]:
    internal_prompt_deps, internal_deps, external_deps = _parse_dependencies(
        prompts, table_name, start_time, db_metadata
    )
    pnames = list(prompts.keys())
    top_pnames = topological_sort(pnames, internal_prompt_deps)
    all_columns = []
    changed_columns = []
    for pname in top_pnames:
        all_columns += prompts[pname].changed_columns

    if origin_id != "":
        to_execute = []
        prev_mat_time, _ , _ = db_metadata.get_table_times(origin_id, table_name)
        prev_prompts = file_operations.get_prompt_names(
            origin_id, origin_table, db_metadata.db_dir
        )

        for pname in top_pnames:
            execute = False
            for dep in internal_prompt_deps[pname]:
                if dep in to_execute and dep != top_pnames[0]:
                    execute = True
                    break
            if not execute:
                for dep in external_deps[pname]:
                    if dep[3] >= prev_mat_time:
                        execute = True
                        break
            if not execute:
                if pname not in prev_prompts:
                    execute = True
                elif not file_operations.check_prompt_equality(
                    pname, instance_id, table_name, origin_id, origin_table, db_metadata.db_dir
                ):
                    execute = True
            if execute:
                to_execute.append(pname)
                changed_columns += prompts[pname].changed_columns
    else:
        for pname in top_pnames:
            changed_columns += prompts[pname].changed_columns

    return (
        top_pnames,
        changed_columns,
        all_columns,
        internal_deps,
        external_deps,
    )
