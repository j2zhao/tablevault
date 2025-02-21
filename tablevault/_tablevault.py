from tablevault._utils import file_operations
from tablevault._utils.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._meta_operations import datavault_operation
from tablevault._utils.database_lock import DatabaseLock
from tablevault import _table_execution
from tablevault._prompt_parsing.types import Prompt, ExternalDeps
from tablevault.errors import DVArgumentError

def setup_database(db_dir: str, replace: bool = False):
    file_operations.setup_database_folder(db_dir, replace)

def active_processes(db_dir:str, all_info: bool) -> ActiveProcessDict:
        db_metadata = MetadataStore(db_dir)
        return db_metadata.get_active_processes(all_info, to_print=False)

def list_instances(table_name: str, db_dir:str, version: str) -> list[str]:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version, to_print=False)

def stop_process(process_id:str, db_dir:str):
    """USE WITH CAUTION"""
    db_metadata = MetadataStore(db_dir)
    db_metadata.write_to_log(process_id, success=False)
    db_locks = DatabaseLock(process_id, db_metadata.db_dir)
    db_locks.release_all_locks()

@datavault_operation
def copy_table_files(
            prompt_dir: str,
            table_name:str, 
            db_metadata: MetadataStore
        ):
     file_operations.move_prompts(prompt_dir, table_name, db_metadata.db_dir)

@datavault_operation
def delete_table(table_name:str, 
                 db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir)

@datavault_operation
def delete_instance(instance_id:str, 
                    table_name:str, 
                    db_metadata: MetadataStore
                    ):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir, instance_id)

@datavault_operation
def execute_instance(table_name:str,
                  instance_id:str,
                  perm_instance_id:str,
                  prompts: dict[Prompt],
                  top_pnames: list[str],
                  to_change_columns: list[str],
                  all_columns: list[str],
                  external_deps: ExternalDeps,
                  prev_instance_id:str,
                  prev_completed_steps: list[str],
                  update_rows:bool,
                  process_id:str,
                  db_metadata: MetadataStore):
     _table_execution.execute_table(table_name,
                  instance_id,
                  perm_instance_id,
                  prompts,
                  top_pnames,
                  to_change_columns,
                  all_columns,
                  external_deps,
                  prev_instance_id,
                  prev_completed_steps,
                  update_rows,
                  process_id,
                  db_metadata)
# TODO: FIX
@datavault_operation
def setup_temp_instance(
            version:str,
            instance_id:str,
            table_name:str,
            prev_id:str,
            prompts: list[str],
            execute:bool,
            process_id:str,
            db_metadata: MetadataStore,
            step_ids:list[str],
        ):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    if step_ids[0] not in complete_steps:
        file_operations.setup_table_instance_folder(
            instance_id, table_name, db_metadata.db_dir, prev_id, prompts
        )
        db_metadata.update_process_step(process_id,step_ids[0])
    if execute and step_ids[1] not in complete_steps:
        execute_instance(table_name = table_name,
                      version = version,
                      author = process_id,
                      force_restart = False,
                      force_execute = False,
                      process_id = step_ids[1],
                      db_dir = db_metadata.db_dir)
        db_metadata.update_process_step(process_id,step_ids[1])

@datavault_operation
def setup_table(
            table_name:str, 
            prompt_dir: str,
            execute:bool,
            create_temp:bool,
            process_id:str,
            db_metadata: MetadataStore,
            step_ids:list[str],
        ):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    if step_ids[0] not in complete_steps:
        file_operations.setup_table_folder(table_name, db_metadata.db_dir)
        db_metadata.update_process_step(process_id,step_ids[0])
    if prompt_dir != '' and step_ids[1] not in complete_steps:
        copy_table_files(author = process_id,
                    table_name = table_name,
                    prompt_dir = prompt_dir,
                    process_id = step_ids[1],
                    db_dir = db_metadata.db_dir)
        db_metadata.update_process_step(process_id,step_ids[1])
    if prompt_dir and create_temp and step_ids[2] not in complete_steps:
        setup_temp_instance(author = process_id,
               version = '', 
               table_name = table_name,
               prev_id = '',
               copy_previous = False,
               prompts = True,
               execute = execute,
               process_id = step_ids[2],
               db_dir = db_metadata.db_dir)
        db_metadata.update_process_step(process_id,step_ids[2])
    
          

@datavault_operation
def copy_database_files(
          yaml_dirs:list[str],
          code_dir:str,
          execute: bool,
          allow_multiple_tables: list,
          process_id:str,
          db_metadata: MetadataStore,
          step_ids:list[str]
          ):
    """pipeline: setup_db, for table in yaml_dir: setup and copy, """
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps()
    index = 0
    if code_dir != '' and step_ids[index] not in complete_steps:
        file_operations.move_code(code_dir, db_metadata.db_dir)
        db_metadata.update_process_step(process_id, step_ids[index])
        index += 1
    for tname, pdir in yaml_dirs:
        if tname in allow_multiple_tables:
            allow_multiple = True
        else:
            allow_multiple = False
        setup_table(
            author = process_id,
            table_name= tname,
            prompt_dir=pdir,
            execute = False,
            allow_multiple = allow_multiple,
            process_id = step_ids[index],
            db_dir = db_metadata.db_dir
        )
        db_metadata.update_process_step(process_id, step_ids[index])
        index += 1

@datavault_operation
def restart_database(process_id:str,
                     db_metadata: MetadataStore,
                     ):
    active_processes = db_metadata.get_active_processes()
    for pid in active_processes:
        data = active_processes[process_id].data
        if active_processes[pid].operation == 'copy_table_files':
            copy_table_files(author= process_id,
                             table_name= '',
                             prompt_dir = '',
                             process_id = pid,
                             db_dir = db_metadata.db_dir
                             )
        elif active_processes[pid].operation == 'delete_table':
            delete_table(author = process_id,
                         table_name = '',
                         process_id = pid,
                         db_dir = db_metadata.db_dir
                         )
        elif active_processes[pid].operation == 'delete_instance':
            delete_instance(author = process_id,
                            table_name = '',
                            instance_id = '',
                            process_id = pid,
                            db_dir = db_metadata.db_dir
                            )
        elif active_processes[pid].operation == 'execute_instance':
            execute_instance(author = process_id,
                             table_name=data['table_name'],
                             version = data['version'],
                             force_restart = False,
                             force_execute = False,
                             process_id = pid,
                             db_dir = db_metadata.db_dir
                             )
        elif active_processes[pid].operation == 'setup_temp_instance':
            setup_temp_instance(author = process_id,
                                version = '',
                                table_name = '',
                                prev_id = '',
                                copy_previous = False,
                                prompts = [],
                                execute = False,
                                process_id = pid,
                                db_dir = db_metadata.db_dir
                                )
        elif active_processes[pid].operation == 'setup_table':
            setup_table(author = process_id,
                        table_name= '',
                        prompt_dir = '',
                        execute = False,
                        allow_multiple = False,
                        process_id = pid,
                        db_dir = db_metadata.db_dir
                        )
        elif active_processes[pid].operation == 'copy_database_files':
            copy_database_files(author = process_id,
                                yaml_dir = '',
                                code_dir = '',
                                execute = False,
                                allow_multiple_tables = [],
                                process_id = pid,
                                db_dir = db_metadata.db_dir
                                )
        db_metadata.update_process_step(process_id, step=pid)



