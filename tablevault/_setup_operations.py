from tablevault._utils.metadata_store import MetadataStore
from tablevault._utils.errors import TVRollbackError, TVArgumentError, TVFileError
from tablevault._utils.database_lock import DatabaseLock
from tablevault._prompt_parsing import prompt_parser
from typing import Any
import time
import random
import string
from tablevault._utils import file_operations
from tablevault._utils.utils import gen_process_id
from tablevault._utils.constants import ILLEGAL_TABLE_NAMES
from tablevault._prompt_parsing.prompt_parser_db import get_table_order

TV_WKARGS = dict[str, Any]


def check_restart_operation(
    process_id: str, db_metadata: MetadataStore
) -> tuple[bool, dict[str, Any]]:
    processes = db_metadata.get_active_processes()
    if process_id in processes:
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        return True, processes[process_id].data
    else:
        return False, {}


def setup_copy_files(
    author: str,
    file_dir: str,
    table_name: str,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name, "prompts")
    if not db_metadata.check_table_existance(table_name):
        raise TVArgumentError(f"{table_name} does not exist")
    funct_kwargs = {"file_dir": file_dir, "table_name": table_name}
    if table_name != "":
        file_operations.copy_folder_to_temp(
            process_id,
            db_dir=db_metadata.db_dir,
            table_name=table_name,
            subfolder="prompts",
        )
    else:
        file_operations.copy_folder_to_temp(
            process_id, db_dir=db_metadata.db_dir, subfolder="code_functions"
        )
    db_metadata.start_new_process(
        process_id,
        author,
        "copy_table_files",
        table_name,
        data=funct_kwargs,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_delete_table(
    author: str,
    table_name: str,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name)
    if not db_metadata.check_table_existance(table_name):
        raise TVArgumentError(f"{table_name} does not exist")
    funct_kwargs = {"table_name": table_name}
    file_operations.copy_folder_to_temp(
        process_id, db_dir=db_metadata.db_dir, table_name=table_name
    )

    db_metadata.start_new_process(
        process_id,
        author,
        "delete_table",
        table_name,
        data=funct_kwargs,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_delete_instance(
    author: str,
    table_name: str,
    instance_id: str,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    if not db_metadata.check_table_existance(table_name, instance_id=instance_id):
        raise TVArgumentError(f"{table_name} {instance_id}does not exist")
    funct_kwargs = {"table_name": table_name, "instance_id": instance_id}
    file_operations.copy_folder_to_temp(
        process_id,
        db_dir=db_metadata.db_dir,
        table_name=table_name,
        instance_id=instance_id,
    )
    db_metadata.start_new_process(
        process_id,
        author,
        "delete_instance",
        table_name,
        instance_id=instance_id,
        data=funct_kwargs,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_execute_instance(
    author: str,
    table_name: str,
    version: str,
    force_restart: bool,
    force_execute: bool,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:
    processes = db_metadata.get_active_processes()
    if process_id in processes and "rollback" in processes[process_id].complete_steps:
        raise TVRollbackError()
    table_lid = db_locks.acquire_shared_lock(table_name)
    if not db_metadata.check_table_existance(table_name):
        raise TVArgumentError(f"{table_name} doesn't exist.")
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if allow_multiple and version == "":
        version = "base"
    start_time = time.time()
    rand_str = "".join(random.choices(string.ascii_letters, k=5))
    perm_instance_id = "_" + str(int(start_time)) + "_" + rand_str
    perm_instance_id = version + perm_instance_id
    instance_id = "TEMP_" + version
    db_locks.acquire_exclusive_lock((table_name, instance_id))
    db_locks.acquire_exclusive_lock((table_name, perm_instance_id))
    prompts = file_operations.get_prompts(instance_id, table_name, db_metadata.db_dir)
    column_dtypes = {}
    for pname, prompt in prompts.items():
        prompts[pname]["parsed_changed_columns"], dtypes = (
            prompt_parser.get_changed_columns(prompt)
        )
        column_dtypes.update(dtypes)

    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart and not force_restart:
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        funct_kwargs["prompts"] = prompts
        funct_kwargs["prev_completed_steps"] = db_metadata.get_active_processes()[
            process_id
        ].get_completed_step()
        return funct_kwargs
    elif check_restart and force_restart:
        db_metadata.update_process_step(
            process_id, step="force_restart", data=funct_kwargs
        )

    instance_exists = file_operations.check_temp_instance_existance(
        instance_id, table_name, db_metadata.db_dir
    )
    if not instance_exists:
        raise TVArgumentError(
            f"Temporary Instance {instance_id} Does not Exist For Table {table_name}"
        )

    if not force_execute:

        _, _, prev_instance_id = db_metadata.get_last_table_update(
            table_name, version, before_time=start_time
        )
        if prev_instance_id != "":
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
    funct_kwargs = {
        "table_name": table_name,
        "instance_id": instance_id,
        "perm_instance_id": perm_instance_id,
        "top_pnames": top_pnames,
        "to_change_columns": to_change_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "prev_instance_id": prev_instance_id,
        "prev_completed_steps": [],
        "update_rows": True,
        "internal_prompt_deps": internal_prompt_deps,
        "version": version,
        "column_dtypes": column_dtypes,
    }
    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            db_locks.acquire_shared_lock(table_name=table, instance_id=instance)
    db_metadata.start_new_process(
        process_id,
        author,
        "execute_instance",
        table_name,
        instance_id=instance_id,
        data=funct_kwargs,
        start_time=start_time,
        parent_id=parent_id,
    )
    funct_kwargs["prompts"] = prompts
    db_locks.release_lock(table_lid)
    return funct_kwargs


def setup_setup_temp_instance(
    author: str,
    version: str,
    table_name: str,
    prev_id: str,
    copy_previous: bool,
    prompts: list[str],
    execute: bool,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:

    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs

    tlid = db_locks.acquire_shared_lock(table_name)
    check_existance = db_metadata.check_table_existance(table_name)
    if not check_existance:
        raise TVArgumentError(f"Table {table_name} Does not Exist")

    allow_multiple = db_metadata.get_table_multiple(table_name)
    if not allow_multiple and version == "":
        instance_id = "TEMP_"
    elif not allow_multiple and version != "":
        raise TVArgumentError("Cannot Define Instance ID for Table without Versioning")
    elif allow_multiple and version == "":
        version = "base"
    else:
        instance_id = "TEMP_" + version
    start_time = time.time()
    if copy_previous:
        _, _, prev_id = db_metadata.get_last_table_update(
            table_name, version, before_time=start_time
        )
        if prev_id == "":
            raise TVArgumentError(
                f"Version {version} does not have materialized instances"
            )
        db_locks.acquire_shared_lock(table_name, prev_id)
    elif prev_id != "":
        check_existance = db_metadata.check_table_existance(table_name, prev_id)
        if not check_existance:
            raise TVArgumentError(f"Previous Instance ID {prev_id} doesn't exist")
        db_locks.acquire_shared_lock(table_name, prev_id)
    elif isinstance(prompts, list) and len(prompts) != 0:
        db_locks.acquire_shared_lock(table_name, "prompts")
        gen_prompt = False
        if len(prompts) != 0:
            for prompt in prompts:
                if prompt.startswith(f"gen_{table_name}") and not gen_prompt:
                    gen_prompt = True
                elif prompt.startswith(f"gen_{table_name}") and gen_prompt:
                    raise TVArgumentError(
                        f"Can only have one prompt that starts with: gen_{table_name}"
                    )
            if not gen_prompt:
                raise TVArgumentError(
                    f"Needs one generator prompt that starts with gen_{table_name}"
                )
    elif isinstance(prompts, bool) and prompts:
        db_locks.acquire_shared_lock(table_name, "prompts")
        prompts = file_operations.get_prompt_names("", table_name, db_metadata.db_dir)
        gen_prompt = False
        if len(prompts) != 0:
            for prompt in prompts:
                if prompt.startswith(f"gen_{table_name}") and not gen_prompt:
                    gen_prompt = True
                elif prompt.startswith(f"gen_{table_name}") and gen_prompt:
                    raise TVArgumentError(
                        f"Can only have one prompt that starts with: gen_{table_name}"
                    )
            if not gen_prompt:
                raise TVArgumentError(
                    f"Needs one generator prompt that starts with gen_{table_name}"
                )
    db_locks.release_lock(tlid)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    funct_kwargs = {
        "version": version,
        "instance_id": instance_id,
        "table_name": table_name,
        "prev_id": prev_id,
        "prompts": prompts,
        "execute": execute,
    }
    funct_kwargs["step_ids"] = [gen_process_id()]
    if execute:
        funct_kwargs["step_ids"].append(gen_process_id())
    file_operations.copy_folder_to_temp(
        process_id,
        db_dir=db_metadata.db_dir,
        table_name=table_name,
        instance_id=instance_id,
    )
    db_metadata.start_new_process(
        process_id,
        author,
        "setup_temp_instance",
        table_name,
        instance_id=instance_id,
        data=funct_kwargs,
        start_time=start_time,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_setup_table(
    author: str,
    table_name: str,
    yaml_dir: str,
    create_temp: bool,
    execute: bool,
    allow_multiple: bool,
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:

    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs
    if execute and yaml_dir == "":
        raise TVArgumentError(f"Cannot Execute {table_name} without Prompts Directory")
    if table_name in ILLEGAL_TABLE_NAMES:
        raise TVArgumentError("Forbidden Table Name: {table_name}")
    db_locks.acquire_exclusive_lock(table_name)
    exists = db_metadata.check_table_existance(table_name)
    if exists:
        raise TVArgumentError("Table {table_name} already exists")
    funct_kwargs = {
        "allow_multiple": allow_multiple,
        "table_name": table_name,
        "yaml_dir": yaml_dir,
        "execute": execute,
        "create_temp": create_temp,
    }
    funct_kwargs["step_ids"] = [gen_process_id()]
    if yaml_dir != "":
        funct_kwargs["step_ids"].append(gen_process_id())
    if execute:
        funct_kwargs["step_ids"].append(gen_process_id())
    db_metadata.start_new_process(
        process_id,
        author,
        "setup_table",
        table_name,
        data=funct_kwargs,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_copy_database_files(
    author: str,
    yaml_dir: str,
    code_dir: str,
    execute: bool,
    allow_multiple_tables: list[str],
    process_id: str,
    parent_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> TV_WKARGS:
    db_locks.acquire_exclusive_lock()
    check_restart, funct_kwargs = check_restart_operation(process_id, db_metadata)
    if check_restart:
        processes = db_metadata.get_active_processes()
        if "rollback" in processes[process_id].complete_steps:
            raise TVRollbackError()
        db_metadata.update_process_step(
            process_id, step="restart", data={"author": author}
        )
        return funct_kwargs
    step_ids = []
    if code_dir != "":
        step_ids.append(gen_process_id())
    table_names = []
    if yaml_dir != "":
        try:
            table_names = get_table_order()
        except Exception as e:
            raise TVFileError(f"Error ordering {yaml_dir}: {e}")
        for _ in table_names:
            step_ids.append(gen_process_id())
    funct_kwargs = {
        "yaml_dir": yaml_dir,
        "table_names": table_names,
        "code_dir": code_dir,
        "execute": execute,
        "allow_multiple_tables": allow_multiple_tables,
        "step_ids": step_ids,
    }
    db_metadata.start_new_process(
        process_id,
        author,
        "copy_database_files",
        data=funct_kwargs,
        parent_id=parent_id,
    )
    return funct_kwargs


def setup_restart_database(
    author: str, process_id: str, db_metadata: MetadataStore, db_locks: DatabaseLock
) -> TV_WKARGS:
    db_locks.acquire_exclusive_lock("restart")
    db_metadata.start_new_process(process_id, author, "restart_database")
    return {}


SETUP_MAP = {
    "copy_files": setup_copy_files,
    "delete_table": setup_delete_table,
    "delete_instance": setup_delete_instance,
    "execute_instance": setup_execute_instance,
    "setup_temp_instance": setup_setup_temp_instance,
    "setup_table": setup_setup_table,
    "copy_database_files": setup_copy_database_files,
    "restart_database": setup_restart_database,
}
