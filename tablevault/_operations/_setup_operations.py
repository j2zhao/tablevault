from tablevault._helper.metadata_store import MetadataStore
from tablevault._defintions import tv_errors #import TVArgumentError, TVFileError
from tablevault._helper.database_lock import DatabaseLock
from tablevault._prompt_parsing import prompt_parser
from tablevault._helper import file_operations
from tablevault._helper.utils import gen_tv_id
from tablevault._defintions import constants # import ILLEGAL_TABLE_NAMES, RESTART_LOCK, CODE_FOLDER, PROMPT_FOLDER
from tablevault._prompt_parsing.prompt_parser_db import get_table_order
from tablevault._defintions.types import SETUP_OUTPUT

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
    table_lid = db_locks.acquire_shared_lock(table_name)
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if allow_multiple and version == "":
        version = constants.BASE_TABLE_VERSION
    start_time = db_metadata.get_active_processes()[process_id].start_time
    perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
    perm_instance_id = version + perm_instance_id
    instance_id = "TEMP_" + version
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    prompts = file_operations.get_prompts(instance_id, table_name, db_metadata.db_dir)
    db_locks.acquire_exclusive_lock(table_name, perm_instance_id)
    instance_exists = file_operations.check_temp_instance_existance(
        instance_id, table_name, db_metadata.db_dir
    )
    if not instance_exists:
        raise tv_errors.TVArgumentError(
            f"Temporary Instance {instance_id} Does not Exist For Table {table_name}"
        )
    if not force_execute:
        _, _, prev_instance_id = db_metadata.get_last_table_update(
            table_name, version, before_time=start_time
        )
        if prev_instance_id == "":
            _, _, prev_instance_id = db_metadata.get_last_table_update(
            table_name, "", before_time=start_time
        )
        if prev_instance_id != "":
            db_locks.acquire_shared_lock(table_name)

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
    }
    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            db_locks.acquire_shared_lock(table_name=table, instance_id=instance)
    db_metadata.update_process_data(process_id, funct_kwargs)
    funct_kwargs["prompts"] = prompts
    db_locks.release_lock(table_lid)
    return funct_kwargs


def setup_setup_temp_instance(
    table_name: str,
    version: str,
    prev_id: str,
    copy_previous: bool,
    prompt_names: list[str] | bool,
    execute: bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if version == "":
        version = constants.BASE_TABLE_VERSION
    elif not allow_multiple and version != "":
        raise tv_errors.TVArgumentError("Cannot Define Instance ID for Table without Versioning")
    instance_id = constants.TEMP_INSTANCE + version
    start_time = db_metadata.get_active_processes()[process_id].start_time
    if copy_previous:
        _, _, prev_id = db_metadata.get_last_table_update(
            table_name, version, before_time=start_time
        )
        if prev_id == "":
            raise tv_errors.TVArgumentError(
                f"Version {version} does not have materialized instances"
            )
        db_locks.acquire_shared_lock(table_name, prev_id)
    elif prev_id != "":
        db_locks.acquire_shared_lock(table_name, prev_id)
    elif isinstance(prompt_names, list) and len(prompt_names) != 0:
        db_locks.acquire_shared_lock(table_name, constants.PROMPT_FOLDER)
        gen_prompt = False
        if len(prompt_names) != 0:
            for prompt in prompt_names:
                if prompt.startswith(f"gen_{table_name}") and not gen_prompt:
                    gen_prompt = True
                elif prompt.startswith(f"gen_{table_name}") and gen_prompt:
                    raise tv_errors.TVArgumentError(
                        f"Can only have one prompt that starts with: gen_{table_name}"
                    )
            if not gen_prompt:
                raise tv_errors.TVArgumentError(
                    f"Needs one generator prompt that starts with gen_{table_name}"
                )
    elif isinstance(prompt_names, bool) and prompt_names:
        db_locks.acquire_shared_lock(table_name, constants.PROMPT_FOLDER)
        prompt_names = file_operations.get_prompt_names("", table_name, db_metadata.db_dir)
        gen_prompt = False
        for prompt in prompt_names:
            if prompt.startswith(f"gen_{table_name}") and not gen_prompt:
                gen_prompt = True
            elif prompt.startswith(f"gen_{table_name}") and gen_prompt:
                raise tv_errors.TVArgumentError(
                    f"Can only have one prompt that starts with: gen_{table_name}"
                )
        if not gen_prompt:
            raise tv_errors.TVArgumentError(
                f"Needs one generator prompt that starts with gen_{table_name}"
            )
    db_locks.make_lock_path(table_name, instance_id)
    db_locks.acquire_exclusive_lock(table_name, instance_id)
    funct_kwargs = {
        "version": version,
        "instance_id": instance_id,
        "table_name": table_name,
        "prev_id": prev_id,
        "prompt_names": prompt_names,
        "execute": execute,
    }
    funct_kwargs["step_ids"] = [gen_tv_id()]
    if execute:
        funct_kwargs["step_ids"].append(gen_tv_id())
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_setup_table(
    table_name: str,
    create_temp: bool,
    execute: bool,
    allow_multiple: bool,
    yaml_dir: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if execute and yaml_dir == "":
        raise tv_errors.TVArgumentError(f"Cannot Execute {table_name} without Prompts Directory")
    db_locks.make_lock_path(table_name)
    db_locks.acquire_exclusive_lock(table_name)
    funct_kwargs = {
        "allow_multiple": allow_multiple,
        "table_name": table_name,
        "yaml_dir": yaml_dir,
        "execute": execute,
        "create_temp": create_temp,
    }
    funct_kwargs["step_ids"] = [gen_tv_id()]
    if yaml_dir != "":
        funct_kwargs["step_ids"].append(gen_tv_id())
    if execute:
        funct_kwargs["step_ids"].append(gen_tv_id())
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_copy_database_files(
    yaml_dir: str,
    code_dir: str,
    execute: bool,
    allow_multiple_tables: list[str],
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
) -> SETUP_OUTPUT:
    step_ids = []
    if code_dir != "":
        db_locks.acquire_exclusive_lock(constants.CODE_FOLDER)
        step_ids.append(gen_tv_id())
    table_names = []
    if yaml_dir != "":
        try:
            table_names = get_table_order()
        except Exception as e:
            raise tv_errors.TVFileError(f"Error ordering {yaml_dir}: {e}")
        for table_name in table_names:
            db_locks.acquire_exclusive_lock(table_name)
            step_ids.append(gen_tv_id())
    funct_kwargs = {
        "yaml_dir": yaml_dir,
        "table_names": table_names,
        "code_dir": code_dir,
        "execute": execute,
        "allow_multiple_tables": allow_multiple_tables,
        "step_ids": step_ids,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_restart_database(db_locks: DatabaseLock
                           ) -> SETUP_OUTPUT:
    db_locks.acquire_exclusive_lock(constants.RESTART_LOCK)
    return {}

SETUP_MAP = {
    constants.COPY_FILE_OP: setup_copy_files,
    constants.DELETE_TABLE_OP: setup_delete_table,
    constants.DELETE_INSTANCE_OP: setup_delete_instance,
    constants.EXECUTE_OP: setup_execute_instance,
    constants.SETUP_TEMP_OP: setup_setup_temp_instance,
    constants.SETUP_TABLE_OP: setup_setup_table,
    constants.COPY_DB_OP: setup_copy_database_files,
    constants.RESTART_OP: setup_restart_database,
}
