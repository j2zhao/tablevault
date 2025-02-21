from tablevault._utils.metadata_store import MetadataStore
from tablevault.errors import *
from tablevault._utils.database_lock import DatabaseLock
from tablevault._prompt_parsing import prompt_parser
from typing import Any
import time
import random
import string
from tablevault._utils import file_operations
from tablevault._utils.utils import gen_process_id
from tablevault._utils.constants import ILLEGAL_TABLE_NAMES
import os

DV_WKARGS= dict[str, Any]

def check_restart_operation(process_id:str, db_metadata: MetadataStore) -> tuple[bool, dict[str, Any]]:
    processes = db_metadata.get_active_processes()
    if process_id in processes:
        return True, processes[process_id].data
    else:
        return False, {}

def setup_copy_table_files(author:str,
                       table_name:str,
                       prompt_dir: str,
                       process_id:str,
                       db_metadata: MetadataStore,
                       db_locks:DatabaseLock) -> DV_WKARGS:
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name, 'prompts')
    if not db_metadata.check_table_existance(table_name):
        raise DVArgumentError(f'{table_name} does not exist')
    funct_kwargs = {'prompt_dir': prompt_dir, 'table_name': table_name}
    db_metadata.start_new_process(process_id, author, 'copy_table_files', table_name, data=funct_kwargs)
    return funct_kwargs


def setup_delete_table(author:str,
                       table_name:str,
                       process_id:str,
                       db_metadata: MetadataStore,
                       db_locks:DatabaseLock) -> DV_WKARGS:
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name)
    if not db_metadata.check_table_existance(table_name):
        raise DVArgumentError(f'{table_name} does not exist')
    funct_kwargs = {'table_name': table_name}
    db_metadata.start_new_process(process_id, author, 'delete_table', table_name, data=funct_kwargs)
    return funct_kwargs

def setup_delete_instance(author:str,
                       table_name:str,
                       instance_id: str,
                       process_id:str,
                       db_metadata: MetadataStore,
                       db_locks:DatabaseLock) -> DV_WKARGS:
    
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    if not db_metadata.check_table_existance(table_name, instance_id=instance_id):
        raise DVArgumentError(f'{table_name} {instance_id}does not exist')
    funct_kwargs = {'table_name': table_name, 'instance_id': instance_id}
    db_metadata.start_new_process(process_id, author, 'delete_instance', table_name, instance_id=instance_id, data=funct_kwargs)
    return funct_kwargs

def setup_execute_instance(author:str,
                        table_name: str,
                        version: str,
                        force_restart: bool,
                        force_execute: bool,
                        process_id:str,
                        db_metadata: MetadataStore, 
                        db_locks:DatabaseLock) -> DV_WKARGS:
    table_lid = db_locks.acquire_shared_lock(table_name)
    if not db_metadata.check_table_existance(table_name):
        raise DVArgumentError(f"{table_name} doesn't exist.")
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if allow_multiple and version == '':
        version = 'base'
    start_time = time.time()
    rand_str = "".join(random.choices(string.ascii_letters, k=5))
    perm_instance_id = "_" + str(int(start_time)) + "_" + rand_str
    perm_instance_id = version + perm_instance_id
    instance_id = "TEMP_" + version
    db_locks.acquire_exclusive_lock((table_name, instance_id))
    db_locks.acquire_exclusive_lock((table_name, perm_instance_id))
    prompts = file_operations.get_prompts(instance_id, table_name, db_metadata.db_dir)
    for pname, prompt in prompts.items():
        prompts[pname]["parsed_changed_columns"] = prompt_parser.get_changed_columns(prompt)
    
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart and not force_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        funct_kwargs["prompts"] = prompts
        funct_kwargs["prev_completed_steps"] = db_metadata.get_active_processes()[process_id].get_completed_step()
        return funct_kwargs
    elif check_restart and force_restart:
        db_metadata.update_process_step(process_id, step='force_restart', data=funct_kwargs)

    instance_exists = file_operations.check_temp_instance_existance(
        instance_id, table_name, db_metadata.db_dir
    )
    if not instance_exists:
        raise DVArgumentError(
            f"Temporary Instance {instance_id} Does not Exist For Table {table_name}"
        )
    
    if not force_execute:
        
        _, _, prev_instance_id = db_metadata.get_last_table_update(table_name, version, before_time=start_time)
        if prev_instance_id != '':
            db_locks.acquire_shared_lock((table_name))
    
    (
        top_pnames,
        to_change_columns,
        all_columns,
        internal_prompt_deps,
        external_deps,
    ) = prompt_parser.parse_prompts(
        prompts,
        db_metadata,
        start_time,
        instance_id,
        table_name,
        prev_instance_id,
    )
    funct_kwargs = {"table_name":table_name,
                    "instance_id":instance_id,
                    "perm_instance_id": perm_instance_id,
                    "top_pnames": top_pnames,
                    "to_change_columns": to_change_columns,
                    "all_columns": all_columns,
                    "external_deps": external_deps,
                    "prev_instance_id": prev_instance_id,
                    "prev_completed_steps": [], 
                    "update_rows": True,
                    "internal_prompt_deps": internal_prompt_deps,
                    "version":version
                    }
    for pname in external_deps:
        for table, _ , instance, _ , _ in external_deps[pname]:
            db_locks.acquire_shared_lock(table_name=table, instance_id=instance)
    db_metadata.start_new_process(process_id, author, 'execute_instance', table_name, instance_id=instance_id, data=funct_kwargs, start_time=start_time)
    funct_kwargs['prompts'] = prompts
    db_locks.release_lock(table_lid)
    return funct_kwargs

def setup_setup_temp_instance(author:str,
               version:str, 
               table_name:str,
               prev_id:str,
               copy_previous:bool,
               prompts:list[str],
               execute:bool,
               process_id:str,
               db_metadata: MetadataStore,
               db_locks:DatabaseLock)-> DV_WKARGS:
    
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs

    tlid = db_locks.acquire_shared_lock(table_name)
    check_existance = db_metadata.check_table_existance(table_name)
    if not check_existance:
        raise DVArgumentError(f"Table {table_name} Does not Exist")
    
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if not allow_multiple and version == "":
        instance_id = "TEMP_"
    elif not allow_multiple and version != "":
        raise DVArgumentError("Cannot Define Instance ID for Table without Versioning")
    elif allow_multiple and version == "":
        version = 'base'
    else:
        instance_id = "TEMP_" + version
    start_time = time.time()
    if copy_previous:
        _, _, prev_id = db_metadata.get_last_table_update(table_name, version, before_time=start_time)
        if prev_id == '':
            raise DVArgumentError(f'Version {version} does not have materialized instances')
        db_locks.acquire_shared_lock(table_name, prev_id) 
    elif prev_id != '':
        check_existance = db_metadata.check_table_existance(table_name, prev_id)
        if not check_existance:
            raise DVArgumentError(f"Previous Instance ID {prev_id} doesn't exist")
        db_locks.acquire_shared_lock(table_name, prev_id)
    elif isinstance(prompts, list) and len(prompts) != 0:
        db_locks.acquire_shared_lock(table_name, 'prompts')
        gen_prompt = False 
        if len(prompts) != 0:
            for prompt in prompts:
                if prompt.startswith(f'gen_{table_name}') and not gen_prompt:
                    gen_prompt = True
                elif prompt.startswith(f'gen_{table_name}') and gen_prompt:
                    raise DVArgumentError(f'Can only have one prompt that starts with: gen_{table_name}')
            if gen_prompt == False:
                raise DVArgumentError(f'Needs one generator prompt that starts with gen_{table_name}')
    elif isinstance(prompts, bool) and prompts:
        db_locks.acquire_shared_lock(table_name, 'prompts')
        prompts = file_operations.get_prompt_names('', table_name, db_metadata.db_dir)
        gen_prompt = False 
        if len(prompts) != 0:
            for prompt in prompts:
                if prompt.startswith(f'gen_{table_name}') and not gen_prompt:
                    gen_prompt = True
                elif prompt.startswith(f'gen_{table_name}') and gen_prompt:
                    raise DVArgumentError(f'Can only have one prompt that starts with: gen_{table_name}')
            if gen_prompt == False:
                raise DVArgumentError(f'Needs one generator prompt that starts with gen_{table_name}')
    db_locks.release_lock(tlid)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    funct_kwargs = {'version': version,
                    'instance_id': instance_id,
                    'table_name': table_name,
                    'prev_id': prev_id,
                    'prompts': prompts,
                    'execute': execute}
    funct_kwargs['step_ids'] = [gen_process_id()]
    if execute:
        funct_kwargs['step_ids'].append(gen_process_id())
    db_metadata.start_new_process(process_id, author, 'setup_temp_instance', table_name, instance_id=instance_id, data=funct_kwargs, start_time=start_time)
    return funct_kwargs



def setup_setup_table(author:str,
                      table_name:str,
                      prompt_dir: str,
                      create_temp:bool,
                      execute:bool,
                      allow_multiple:bool,
                      process_id:str,
                      db_metadata: MetadataStore,
                      db_locks:DatabaseLock)-> DV_WKARGS:

    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs
    if execute and prompt_dir == '':
        raise DVArgumentError(f"Cannot Execute {table_name} without Prompts Directory")
    if table_name in ILLEGAL_TABLE_NAMES:
        raise DVArgumentError('Forbidden Table Name: {table_name}')
        #raise DVArgumentError(f"Need version name to execute multi-version table {table_name}")
    db_locks.acquire_exclusive_lock(table_name)
    funct_kwargs = {
            "allow_multiple": allow_multiple,
            "table_name":table_name,
            "prompt_dir": prompt_dir,
            "execute": execute,
            "create_temp":create_temp,
        }
    funct_kwargs['step_ids'] = [gen_process_id()]
    if prompt_dir != '':
        funct_kwargs['step_ids'].append(gen_process_id())
    if execute:
        funct_kwargs['step_ids'].append(gen_process_id())
    db_metadata.start_new_process(process_id, author, 'setup_table', table_name, data=funct_kwargs)
    return funct_kwargs

def setup_copy_database_files(author:str,
                                yaml_dir:str,
                                code_dir:str,
                                execute: bool,
                                allow_multiple_tables:list[str],
                                process_id:str,
                                db_metadata: MetadataStore,
                                db_locks:DatabaseLock)-> DV_WKARGS:
    db_locks.acquire_exclusive_lock()
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        db_metadata.update_process_step(process_id, step='restart', data={'author': author})
        return funct_kwargs
    step_ids = []
    if code_dir != '':
        step_ids.append(gen_process_id())
    yaml_dirs = []
    if yaml_dir != '':
        try:
            for tname in os.walk(yaml_dir):
                pdir =  os.path.join(yaml_dir, tname)
                if os.path.isdir(pdir):
                    yaml_dirs.append(tname, pdir)
                    step_ids.append(gen_process_id())
        except Exception as e:
            raise DVFileError(f'Error accessing {yaml_dir}: {e}')

    funct_kwargs = {
        'yaml_dirs':yaml_dirs,
        'code_dir': code_dir,
        'execute': execute,
        'allow_multiple_tables':allow_multiple_tables,
        'step_ids': step_ids
    }
    db_metadata.start_new_process(process_id, author, 'copy_database_files', data=funct_kwargs)
    return funct_kwargs

def setup_restart_database(author:str,
                           process_id:str,
                           db_metadata: MetadataStore,
                           db_locks:DatabaseLock) -> DV_WKARGS:
    db_locks.acquire_exclusive_lock('restart')
    db_metadata.start_new_process(process_id, author, 'restart_database')
    return {}

SETUP_MAP = {
    'copy_table_files': setup_copy_table_files,
    'delete_table': setup_delete_table,
    'delete_instance': setup_delete_instance,
    'execute_instance': setup_execute_instance, 
    'setup_temp_instance': setup_setup_temp_instance,
    'setup_table': setup_setup_table,
    'copy_database_files': setup_copy_database_files,
    'restart_database': setup_restart_database,

    
}
