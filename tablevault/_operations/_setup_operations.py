from tablevault._helper.metadata_store import MetadataStore
from tablevault.defintions import tv_errors #import TVArgumentError, TVFileError
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper import file_operations
from tablevault.defintions.utils import gen_tv_id
from tablevault.defintions import constants, types # import ILLEGAL_TABLE_NAMES, RESTART_LOCK, CODE_FOLDER, PROMPT_FOLDER
from tablevault.defintions.types import SETUP_OUTPUT
from tablevault.prompts.base_ptype import TVPrompt, order_tables_by_prompts
from tablevault.prompts.load_prompt import load_prompt
from tablevault.prompts.utils.utils import topological_sort

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
    db_metadata.update_process_data(process_id, {"perm_instance_id": perm_instance_id, 
                                                 "instance_id": instance_id, 
                                                 "table_name": table_name})
    db_locks.make_lock_path(table_name, perm_instance_id)
    instance_id = "TEMP_" + version
    if allow_multiple:
        db_locks.acquire_exclusive_lock(table_name, instance_id)
        db_locks.acquire_exclusive_lock(table_name, perm_instance_id)
    else:
        db_locks.acquire_exclusive_lock(table_name)

    yaml_prompts = file_operations.get_yaml_prompts(instance_id, table_name, db_metadata.db_dir)
    prompts = {pname: load_prompt(yprompt) for pname, yprompt in yaml_prompts.items()}
    instance_exists = file_operations.check_temp_instance_existance(
        instance_id, table_name, db_metadata.db_dir
    )
    if not instance_exists:
        raise tv_errors.TVArgumentError(
            f"Temporary Instance {instance_id} Does not Exist For Table {table_name}"
        )
    if not force_execute:
        meta_ = file_operations.get_yaml_metadata(instance_id, table_name, db_metadata.db_dir)
        prev_instance_id = meta_[constants.INSTANCE_ORIGIN]
        if prev_instance_id == "":
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
    ) = parse_prompts(
        prompts,
        db_metadata,
        start_time,
        instance_id,
        table_name,
        prev_instance_id,
    )
    funct_kwargs = {
        "table_name": table_name,
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
    lock_id = db_locks.acquire_shared_lock(table_name)
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if version == "":
        version = constants.BASE_TABLE_VERSION
    if not allow_multiple and version != constants.BASE_TABLE_VERSION:
        raise tv_errors.TVArgumentError("Cannot Define Version for Table without Versioning")
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
    elif (isinstance(prompt_names, list) and len(prompt_names) != 0) or (isinstance(prompt_names, bool) and prompt_names):
        db_locks.acquire_shared_lock(table_name, constants.PROMPT_FOLDER)
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
        if not allow_multiple:
            db_locks.acquire_exclusive_lock(table_name)
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.release_lock(lock_id)
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
            # TODO: get prompts
            table_names = order_tables_by_prompts()
        except Exception as e:
            raise tv_errors.TVFileError(f"Error ordering {yaml_dir}: {e}")
        for table_name in table_names:
            db_locks.make_lock_path(table_name)
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
                internal_deps[pname].add(dep.column)
                for pn in prompts:
                    if dep.column in prompts[pn].changed_columns:
                        internal_prompt_deps[pname].add(pn)
                continue
            if dep.column != None:
                mat_time, _, instance = (
                         db_metadata.get_last_column_update(dep.table, dep.column, start_time)
                    )
            else:
                mat_time, _, instance = db_metadata.get_last_table_update(
                        dep.table, start_time= start_time, version=dep.version
                    )
            external_deps[pname].add((dep.table, dep.column, instance, mat_time))
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
) -> tuple[list[str], list[str], list[str], list[str], types.InternalDeps, types.ExternalDeps]:
    internal_prompt_deps, internal_deps, external_deps = _parse_dependencies(
        prompts, table_name, start_time, db_metadata
    )
    pnames = list(prompts.keys())
    top_pnames = topological_sort(pnames, internal_prompt_deps)
    all_columns = []
    to_change_columns = []
    for pname in top_pnames:
        all_columns += prompts[pname].changed_columns

    if origin_id != "":
        to_execute = []
        prev_mat_time, _ = db_metadata.get_table_times(origin_id, table_name)
        prev_prompts = file_operations.get_prompt_names(
            origin_id, table_name, db_metadata.db_dir
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
                    pname, instance_id, origin_id, table_name, db_metadata.db_dir
                ):
                    execute = True
            if execute:
                to_execute.append(pname)
                to_change_columns += prompts[pname].changed_columns
    else:
        for pname in top_pnames:
            to_change_columns += prompts[pname].changed_columns

    return (
        top_pnames,
        to_change_columns,
        all_columns,
        internal_deps,
        external_deps,
    )
