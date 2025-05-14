from tablevault.helper import file_operations, database_lock, utils
from tablevault.helper.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._operations._meta_operations import tablevault_operation
from tablevault._operations import _table_execution
from tablevault.defintions.types import ExternalDeps
from tablevault.prompts.utils import table_operations
from tablevault.defintions import constants
from tablevault.defintions import tv_errors
from tablevault.helper.database_lock import DatabaseLock
from tablevault._operations._takedown_operations import TAKEDOWN_MAP
from tablevault.prompts.utils import artifact 
import os
import pandas as pd 
from typing import Optional

def get_artifact_folder(instance_id:str, 
                        table_name:str, 
                        version:str,
                        is_temp: bool,
                        db_dir:str):
    if instance_id == "":
        if is_temp:
            instance_id = constants.TEMP_INSTANCE + version
        else:
            db_metadata = MetadataStore(db_dir)
            _, _, instance_id =  db_metadata.get_last_table_update(table_name, version)
    return artifact.get_artifact_folder(instance_id, table_name, db_dir)

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

def fetch_table(instance_id:str, version:str, table_name:str, db_dir:str, temporary:bool, active_only:bool, safe_locking: bool):
    
    db_metadata = MetadataStore(db_dir)
    if instance_id != '' and temporary:
        raise tv_errors.TVArgumentError("Shouldn't reques temporary while defining instance id.")
    elif instance_id == '' and not temporary:
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

def _materialize_instance(instance_id: str,
                          table_name:str,
                          perm_instance_id: str,
                          origin_id:str,
                          origin_table:str,
                          artifact_columns: list[str],
                          dependencies: list[tuple[str, str]],
                          db_metadata: MetadataStore):

    artifact_dtypes = {}
    for col in artifact_columns:
        artifact_dtypes[col] = constants.ARTIFACT_DTYPE
    
    table_operations.update_dtypes(artifact_dtypes, instance_id, table_name, db_metadata.db_dir)
    
    table_operations.check_table_artifacts(instance_id, table_name, db_metadata.db_dir, rows=constants.ARTIFACT_ROWS)

    file_operations.rename_table_instance(perm_instance_id, instance_id, table_name, db_metadata.db_dir)
    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
        file_operations.move_artifacts_to_table(db_metadata.db_dir, table_name, perm_instance_id)
    
    instance_descript = file_operations.get_description(perm_instance_id, table_name, db_metadata.db_dir)
    instance_descript[constants.DESCRIPTION_DEPENDENCIES] = dependencies
    file_operations.write_description(instance_descript, perm_instance_id, table_name, db_metadata.db_dir)

    if origin_id != '':
        parent_descript = file_operations.get_description(origin_id, origin_table, db_metadata.db_dir)
        if constants.DESCRIPTION_CHILDREN not in parent_descript:
            parent_descript[constants.DESCRIPTION_CHILDREN] = []
        recorded = False
        for id, name in parent_descript[constants.DESCRIPTION_CHILDREN]:
            if id == perm_instance_id or name == table_name:
                recorded = True
        if not recorded:
            parent_descript[constants.DESCRIPTION_FUTURE].append([perm_instance_id, table_name])
            file_operations.write_description(parent_descript, origin_id, origin_table, db_metadata.db_dir)
    
    for dep_table, dep_instance in dependencies:
        parent_descript = file_operations.get_description(dep_instance, dep_table, db_metadata.db_dir)
        if constants.DESCRIPTION_CHILDREN not in parent_descript:
            parent_descript[constants.DESCRIPTION_CHILDREN] = []
        recorded = False
        for id, name in parent_descript[constants.DESCRIPTION_CHILDREN]:
            if id == perm_instance_id or name == table_name:
                recorded = True
        if not recorded:
            parent_descript[constants.DESCRIPTION_CHILDREN].append([perm_instance_id, table_name])
            file_operations.write_description(parent_descript, dep_instance, dep_table, db_metadata.db_dir)

def materialize_instance(author:str,
                         instance_id:str,
                         table_name:str,
                         version:str,
                         perm_instance_id: str,
                         origin_id:str,
                         origin_table:str,
                         artifact_columns: list[str],
                         changed_columns: list[str],
                         all_columns: list[str],
                         dependencies: list[tuple[str, str]],
                         process_id:str,
                         db_dir:str):
    setup_kwargs = {
        "table_name": table_name,
        "instance_id":instance_id,
        "version": version,
        "perm_instance_id":perm_instance_id,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "artifact_columns":artifact_columns,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "dependencies":dependencies,
    }
    return tablevault_operation(author,
                        constants.MAT_OP,
                        _materialize_instance,
                        db_dir, 
                        process_id,
                        setup_kwargs)


def _stop_process(process_ids:list[str],
                  instance_ids:list[str],
                  table_names:list[str],
                  perm_instance_ids:list[str],
                  origin_ids:list[str],
                  origin_tables:list[str],
                  artifact_columns:list[list[str]],
                  changed_columns:list[list[str]],
                  all_columns:list[list[str]],
                  dependencies:list[list[str]],
                  force:bool,
                  step_ids: list[str],
                  process_id: str,
                  db_metadata: MetadataStore,
                  ):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        new_process_ids = db_metadata.stop_operation(process_ids, force)
        for process_id_, op_name in new_process_ids.items():
            db_locks = DatabaseLock(process_id_, db_metadata.db_dir)
            TAKEDOWN_MAP[op_name](process_id_, db_metadata, db_locks)
            db_metadata.write_process(process_id_)
        
    for i, step_id in enumerate(step_ids[1:]):
        try:
            #TODO fix
            materialize_instance(process_id,
                                instance_ids[i-1], 
                                table_names[i-1],
                                "",
                                perm_instance_ids[i-1],
                                origin_ids[i-1],
                                origin_tables[i-1],
                                artifact_columns[i-1],
                                changed_columns[i-1],
                                all_columns[i-1],
                                dependencies[i-1],
                                step_id,
                                db_metadata.db_dir)
        finally:
            db_metadata.update_process_step(process_id, step_id)

def stop_process(author:str,
                 to_stop_process_id: str, 
                 force: bool,
                 materialize:bool,
                 process_id:str,
                 db_dir:str):
    setup_kwargs = {
        'to_stop_process_id':to_stop_process_id,
        'force':force,
        'materialize': materialize,
    }
    return tablevault_operation(author,
                        constants.STOP_PROCESS_OP,
                        _stop_process,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )


def _write_table_inner(table_df: Optional[pd.DataFrame],
                 instance_id:str,
                 table_name:str,
                 db_metadata: MetadataStore
                 ):
    if table_df == None:
        raise tv_errors.TVProcessError("Cannot Restart Write Table")
    table_operations.write_table(table_df, instance_id, table_name, db_metadata.db_dir)
    table_operations.write_dtype(table_df.dtypes, instance_id, table_name,db_metadata.db_dir)

def write_table_inner(author:str,
                 table_df: Optional[pd.DataFrame],
                 instance_id:str,
                 table_name:str,
                 process_id: str,
                 db_dir: str,
                 ):
    setup_kwargs = {
        'table_df':table_df,
        'instance_id':instance_id,
        'table_name': table_name,
    }
    return tablevault_operation(author,
                        constants.WRITE_TABLE_INNER_OP,
                        _write_table_inner,
                        db_dir, 
                        process_id,
                        setup_kwargs,
                        )
        
def _write_table(table_df: Optional[pd.DataFrame],
                 instance_id:str,
                 table_name:str,
                 perm_instance_id:str,
                 artifact_columns: list[str],
                 all_columns: list[str],
                 changed_columns: list[str],
                 dependencies: list[tuple[str, str]],
                 step_ids: list[str],
                 process_id: str,
                 db_metadata: MetadataStore
                 ):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        write_table_inner(process_id,
                          table_df,
                 instance_id,
                 table_name,
                 step_ids[0],
                 db_metadata.db_dir,
                 )
        db_metadata.update_process_step(process_id, step_ids[0])

    if step_ids[1] not in complete_steps:
        materialize_instance(process_id,
                             instance_id,
                             table_name,
                             "",
                             perm_instance_id,
                             "",
                             "",
                             artifact_columns,
                             all_columns,
                             changed_columns,
                             dependencies,
                             step_ids[1],
                             db_metadata.db_dir)
        db_metadata.update_process_step(process_id, step_ids[1])
    

def write_table(
        author:str,
        table_df: pd.DataFrame,
        table_name:str,
        version:str,
        artifact_columns: list[str],
        dependencies: list[tuple[str, str]],
        process_id:str,
        db_dir:str
        ):
    setup_kwargs = {
        'table_df': table_df,
        'table_name': table_name,
        'version':version,
        "artifact_columns":artifact_columns,
        'dependencies':dependencies
    }
    return tablevault_operation(author,
                        constants.WRITE_TABLE_OP,
                        _write_table,
                        db_dir, 
                        process_id,
                        setup_kwargs)



def _execute_instance_inner(instance_id: str,
    table_name: str,
    top_pnames: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id:str,
    origin_table:str,
    process_id: str,
    db_metadata: MetadataStore,
):
    _table_execution.execute_instance(
        table_name,
        instance_id,
        top_pnames,
        changed_columns,
        all_columns,
        external_deps,
        origin_id,
        origin_table,
        process_id,
        db_metadata,
    )
    # 

def execute_instance_inner(author:str,
                            instance_id: str,
                            table_name: str,
                            top_pnames: list[str],
                            changed_columns: list[str],
                            all_columns: list[str],
                            external_deps: ExternalDeps,
                            origin_id:str,
                            origin_table:str,
                            process_id: str,
                            db_dir: str,
                            ):
    setup_kwargs = {
        'instance_id': instance_id,
        "table_name": table_name,
        "top_pnames": top_pnames,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
    }
    return tablevault_operation(author,
                        constants.EXECUTE_INNER_OP,
                        _execute_instance_inner,
                        db_dir, 
                        process_id,
                        setup_kwargs)

def _execute_instance(instance_id: str,
                      perm_instance_id: str,
                      table_name: str,
                      top_pnames: list[str],
                      changed_columns: list[str],
                      all_columns: list[str],
                      external_deps: ExternalDeps,
                      origin_id:str,
                      origin_table:str,
                      step_ids: list[str],
                      process_id: str,
                      db_metadata: MetadataStore
                      ):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        execute_instance_inner(process_id,
                            instance_id,
                            table_name,
                            top_pnames,
                            changed_columns,
                            all_columns,
                            external_deps,
                            origin_id,
                            origin_table,
                            step_ids[0],
                            db_metadata.db_dir,
                            )
        db_metadata.update_process_step(process_id, step_ids[0])
    if step_ids[1] not in complete_steps:
        dependencies = []
        for prompt in external_deps:
            for tname, _, id, _, _ in external_deps[prompt]:
                dependencies.append([tname, id])

        materialize_instance(process_id,
                             instance_id,
                             table_name,
                             "",
                             perm_instance_id,
                             origin_id,
                             origin_table,
                             [],
                             changed_columns,
                             all_columns,
                             dependencies,
                             step_ids[1],
                             db_metadata.db_dir)
        db_metadata.update_process_step(process_id, step_ids[1])
    

def execute_instance(author:str,
               table_name:str,
               version: str,
               force_execute:bool,
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
                               origin_id:str,
                               origin_table:str,
                               external_edit:bool,
                               prompt_names: list[str],
                               description:str,
                               db_metadata: MetadataStore):
    file_operations.setup_table_instance_folder(
            instance_id, table_name, db_metadata.db_dir, origin_id, origin_table, prompt_names, external_edit
        )
    descript_yaml = {constants.DESCRIPTION_SUMMARY: description,
                     constants.DESCRIPTION_ORIGIN: [origin_id, origin_table],
                     constants.DESCRIPTION_EDIT: external_edit
                     }
    file_operations.write_description(descript_yaml, instance_id, table_name, db_metadata.db_dir)

def setup_temp_instance_inner(author:str,
                              table_name:str,
                              instance_id:str,
                              origin_id:str,
                              origin_table:str,
                              prompt_names: list[str],
                              process_id: str,
                              db_dir:str,
                              description:str):
    setup_kwargs = {
        "table_name":table_name,
        "instance_id": instance_id,
        "origin_id":origin_id,
        "origin_table":origin_table,
        "prompt_names":prompt_names,
        "description": description
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
    origin_id: str,
    origin_table:str,
    external_edit:bool,
    prompt_names: list[str],
    execute: bool,
    background_execute: bool,
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
    description: str
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        setup_temp_instance_inner(author=process_id,
                                  instance_id=instance_id,
                                  table_name=table_name,
                                  origin_id=origin_id,
                                  origin_table=origin_table,
                                  external_edit=external_edit,
                                  prompt_names=prompt_names,
                                  process_id=step_ids[0],
                                  db_dir=db_metadata.db_dir,
                                  description = description)
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
               origin_id:str,
               origin_table:str,
               external_edit:bool,
               copy_version: bool,
               prompt_names: list[str] | bool,
               execute: bool,
               process_id:str,
               db_dir:str,
               background_execute:bool):
    setup_kwargs = {
        'table_name': table_name,
        'version':version,
        'origin_id': origin_id,
        'origin_table': origin_table,
        'external_edit': external_edit,
        'copy_version': copy_version,
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
                       description: str,
                       allow_multiple_artifacts:bool,
                       has_side_effects:bool,
                       db_metadata: MetadataStore):
    file_operations.setup_table_folder(table_name, db_metadata.db_dir, allow_multiple_artifacts)
    descript_yaml = {constants.DESCRIPTION_SUMMARY: description,
                     constants.TABLE_ALLOW_MARTIFACT:allow_multiple_artifacts,
                     constants.TABLE_SIDE_EFFECTS:has_side_effects,
                     }
    file_operations.write_description(descript_yaml = descript_yaml,
                      instance_id = '', 
                      table_name = table_name, 
                      db_dir = db_metadata.db_dir)

def setup_table_inner(author:str,
                      table_name:str,
                      allow_multiple_artifacts:bool,
                      has_side_effects:bool,
                      process_id:str,
                      db_dir:str,
                      description:str,
                    ):
    setup_kwargs = {
        'table_name': table_name,
        'allow_multiple_artifacts':allow_multiple_artifacts,
        'has_side_effects':has_side_effects,
        'description': description
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
    description: str,
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
                      db_dir=db_metadata.db_dir,
                      description = description,
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
            origin_id="",
            origin_table="",
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
               description:str,
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
        'description':description
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
        if active_processes[prid].operation == constants.STOP_PROCESS_OP:
            stop_process(author=process_id,
                            to_stop_process_id= "", 
                            force = False,
                            materialize = False,
                            process_id=prid,
                            db_dir=db_metadata.db_dir)
    
    active_processes = db_metadata.get_active_processes()
    for prid in active_processes:
        if active_processes[prid].start_success is None:
            error = ("TVProcessError", "Restart Failure")
            db_metadata.update_process_start_status(prid, False, error)
    for prid in active_processes:
        if "_" in prid:
            continue
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
        elif active_processes[prid].operation == constants.MAT_OP:
            materialize_instance(
                author=process_id,
                instance_id="",
                table_name="",
                version="",
                perm_instance_id="",
                origin_id="",
                origin_table="",
                artifact_columns=[],
                changed_columns=[],
                all_columns=[],
                dependencies=[],
                process_id=prid,
                db_dir=db_metadata.db_dir
            )
        elif active_processes[prid].operation == constants.WRITE_TABLE_OP:
            write_table(
                author = process_id,
                table_df = None,
                table_name = "",
                version = "",
                artifact_column = "",
                artifact_dir_column = "",
                dependencies = [],
                process_id = prid,
                db_dir =db_metadata.db_dir,
                )
        elif active_processes[prid].operation == constants.EXECUTE_OP:
            execute_instance(
                author=process_id,
                table_name="",
                version="",
                force_execute=False,
                process_id=prid,
                db_dir=db_metadata.db_dir,
                background=False
            )
        elif active_processes[prid].operation == constants.SETUP_TEMP_OP:
            setup_temp_instance(
                author=process_id,
                table_name="",
                version="",
                origin_id="",
                origin_table="",
                external_edit = False,
                copy_version=False,
                prompt_names=[],
                execute=False,
                process_id=prid,
                db_dir=db_metadata.db_dir,
                background_execute=False
            )
        elif active_processes[prid].operation == constants.SETUP_TABLE_OP:
            setup_table(
                author=process_id,
                table_name="",
                create_temp=False,
                execute=False,
                background_execute=False,
                allow_multiple_artifacts=False,
                has_side_effects=False,
                description = '',
                yaml_dir = '',
                process_id=prid,
                db_dir=db_metadata.db_dir,

            )
        elif active_processes[prid].operation == constants.COPY_DB_OP:
            copy_database_files(
                author=process_id,
                yaml_dir="",
                code_dir="",
                execute=False,
                allow_multiple_artifacts = [],
                has_side_effects = [],
                process_id=prid,
                db_dir=db_metadata.db_dir,
                background_execute = False
            )
        
        db_metadata.update_process_step(process_id, step=prid)

def restart_database(
        author:str,
        process_id: str,
        db_dir: str,
        ):
    return tablevault_operation(author,
                        constants.RESTART_OP,
                        _restart_database,
                        db_dir, 
                        process_id,
                        setup_kwargs={},
                        )
