import time
from tablevault._metadata_store import MetadataStore
from tablevault import _file_operations
from tablevault._prompt_parsing import prompt_parser
from tablevault._prompt_execution.parse_code import (
    execute_code_from_prompt,
    execute_gen_table_from_prompt,
)
from tablevault._prompt_execution.parse_llm import execute_llm_from_prompt
from tablevault._database_lock import DatabaseLock
import pandas as pd
import random
import string

from tablevault._timing_helper.timing_helper import BasicTimer, StepsTimer


def _update_table_columns(
    to_change_columns: list,
    all_columns: list,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    df = _file_operations.get_table(instance_id, table_name, db_dir)
    columns = list(dict.fromkeys(df.columns).keys()) + [
        col for col in all_columns if col not in df.columns
    ]
    for col in columns:
        if col not in all_columns:
            df.drop(col, axis=1)
        elif len(df) == 0:
            df[col] = pd.Series(dtype="string")
        elif col in to_change_columns or col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype("string")
    _file_operations.write_table(df, instance_id, table_name, db_dir)


def _fetch_table_cache(
    external_dependencies: list,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> prompt_parser.Cache:
    cache = {}
    cache["self"] = _file_operations.get_table(instance_id, table_name, db_dir)

    for dep in external_dependencies:
        table, _, instance, _, latest = dep
        if latest:
            cache[table] = _file_operations.get_table(instance, table, db_dir)
        else:
            cache[(table, instance)] = _file_operations.get_table(
                instance, table, db_dir
            )
    return cache


def setup_database(db_dir: str, replace: bool = False):
    _file_operations.setup_database_folder(db_dir, replace)
    DatabaseLock(db_dir)


def execute_table(
    table_name: str,
    db_dir: str,
    author: str,
    instance_id: str = "",
    force: bool = False,
): 
    #test_timer = BasicTimer()
    start_time = time.time()
    rand_str = "".join(random.choices(string.ascii_letters, k=5))
    perm_instance_id = "_" + str(int(start_time)) + "_" + rand_str
    perm_instance_id = instance_id + perm_instance_id
    instance_id = "TEMP_" + instance_id

    db_metadata = MetadataStore(db_dir)

    instance_lock = DatabaseLock(db_dir, table_name, instance_id)

    instance_lock.acquire_exclusive_lock()

    perm_instance_lock = DatabaseLock(db_dir, table_name, perm_instance_id)

    perm_instance_lock.acquire_exclusive_lock()

    prompts = _file_operations.get_prompts(instance_id, table_name, db_dir)

    instance_exists = _file_operations.check_temp_instance_existance(
        instance_id, table_name, db_dir
    )
    if not instance_exists:
        raise ValueError(
            f"Temporary Instance {instance_id} Does not Exist For Table {table_name}"
        )

    if "origin" in prompts["description"]:
        origin = prompts["description"]["origin"]
        origin_lock = DatabaseLock(db_dir, table_name, origin)
        origin_lock.acquire_shared_lock()
        origin_exists = db_metadata.check_table_instance(table_name, origin)
        if not origin_exists:
            origin_lock.release_shared_lock(delete=True)
            origin_lock = None
    else:
        origin = None
        origin_lock = None
        origin_exists = False

    (
        top_pnames,
        to_execute,
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
        db_dir,
        force,
        origin_exists,
    )

    if origin_lock is not None:
        origin_lock.release_shared_lock()
    # execute prompts
    dep_locks = []
    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            lock = DatabaseLock(db_dir, table_name=table, instance_id=instance)
            lock.acquire_shared_lock()
            dep_locks.append(lock)

    data = {
        "origin": origin,
        "perm_instance_id": perm_instance_id,
        "to_execute": to_execute,
        "top_pnames": top_pnames,
        "to_change_columns": to_change_columns,
        "start_time": start_time,
        "all_columns": all_columns,
        "internal_prompt_deps": internal_prompt_deps,
        "external_deps": external_deps,
        "gen_columns": prompts[top_pnames[0]]["parsed_changed_columns"],
    }
    print("PARSED DEPENDENCIES AND TO EXECUTE PROMPTS")
    process_id = db_metadata.start_new_process(
        author, "execute_table", table_name, instance_id, start_time, data=data
    )

    _update_table_columns(
        to_change_columns, all_columns, instance_id, table_name, db_dir
    )

    to_change_columns = set(to_change_columns)
    #test_timer.end_time('Finish Beginning Operation')
    
    for i, pname in enumerate(top_pnames):
        prompt = prompt_parser.convert_reference(prompts[pname])

        cache = _fetch_table_cache(
            external_deps[pname],
            instance_id,
            table_name,
            db_dir,
        )

        if i == 0:
            rows_updated = execute_gen_table_from_prompt(
                prompt, cache, instance_id, table_name, db_dir
            )
            db_metadata.update_process_data(process_id, {"rows_updated": rows_updated})
            #test_timer.end_time("Finish Generation")
            if not rows_updated and len(to_execute) == 0:
                break
        else:
            if rows_updated or pname in to_execute:
                if prompt["type"] == "code":
                    execute_code_from_prompt(
                        prompt, cache, instance_id, table_name, db_dir
                    )
                elif prompt["type"] == "llm":
                    execute_llm_from_prompt(
                        prompt, cache, instance_id, table_name, db_dir
                    )
                #test_timer.end_time(f"Finish Prompt {pname}")
            else:
                continue
        db_metadata.update_process_step(process_id, pname)
        print("FINISHED STEP")
        print(pname)
    if not rows_updated and len(to_change_columns) == 0:
        db_metadata.update_process_step(process_id, "no_update")
        
        db_metadata.write_to_log(process_id, success=False)

        instance_lock.release_exclusive_lock()
        perm_instance_lock.release_exclusive_lock(delete=True)

        for lock in dep_locks:
            lock.release_shared_lock()

        print("NO UPDATES: NOTHING HAPPENS.")
    else:

        _file_operations.materialize_table_folder(
            perm_instance_id, instance_id, table_name, db_dir
        )

        db_metadata.update_process_step(process_id, "materalized")

        db_metadata.write_to_log(process_id)

        instance_lock.release_exclusive_lock(delete=True)

        perm_instance_lock.release_exclusive_lock()

        for lock in dep_locks:
            lock.release_shared_lock()
    #test_timer.end_time(f"Finish Operation")


def restart_execute_table(author: str, process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        top_pnames = process.data["top_pnames"]
        to_change_columns = process.data["to_change_columns"]
        all_columns = process.data["all_columns"]
        process.data["internal_prompt_deps"]
        external_deps = process.data["external_deps"]
        instance_id = process.instance_id
        start_time = process.data["start_time"]
        process.data["origin"]
        perm_instance_id = process.data["perm_instance_id"]
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f"Error Fetching Data for process {process_id}. Not executed.")
        raise e

    instance_lock = DatabaseLock(db_dir, table_name, instance_id, restart=True)
    instance_lock.acquire_exclusive_lock()
    if "stop_execute" in process.complete_steps:
        print("stop_execution")
        _file_operations.clear_table_instance(instance_id, table_name, db_dir)
        db_metadata.write_to_log(process_id, success=False)
        instance_lock.release_exclusive_lock()
        return
    perm_instance_lock = DatabaseLock(
        db_dir, table_name, perm_instance_id, restart=True
    )
    perm_instance_lock.acquire_exclusive_lock()
    if "no_update" in process.complete_steps:
        db_metadata.write_to_log(process_id, success=False)
        instance_lock.release_exclusive_lock()
        perm_instance_lock.release_exclusive_lock(delete=True)
        return
    elif "materalized" in process.complete_steps:
        db_metadata.write_to_log(process_id)
        instance_lock.release_exclusive_lock(delete=True)
        perm_instance_lock.release_exclusive_lock()
        return
    prompts = _file_operations.get_prompts(instance_id, table_name, db_dir)
    _, prompts = prompt_parser.parse_prompts_modified(prompts)
    dep_locks = []
    for pname in external_deps:
        for table, _, instance, _, _ in external_deps[pname]:
            lock = DatabaseLock(
                db_dir, table_name=table, instance_id=instance, restart=True
            )
            lock.acquire_shared_lock()
            dep_locks.append(lock)

    if "clear_table" not in process.complete_steps:
        _update_table_columns(
            to_change_columns, all_columns, instance_id, table_name, db_dir
        )
        db_metadata.update_process_step(process_id, "clear_table")

    for i, pname in enumerate(top_pnames):
        if pname in process.complete_steps and i == 0:
            rows_updated = process.data["rows_updated"]
            if not rows_updated and len(to_change_columns) == 0:
                break
        if pname in process.complete_steps:
            continue
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _fetch_table_cache(
            external_deps[pname],
            instance_id,
            table_name,
            db_dir,
        )
        if i == 0:
            rows_updated = execute_gen_table_from_prompt(
                prompt, cache, instance_id, table_name, db_dir
            )
            db_metadata.update_process_data(process_id, {"rows_updated": rows_updated})
            if not rows_updated and len(to_change_columns) == 0:
                break
        else:
            if rows_updated or set(prompt["parsed_changed_columns"]).issubset(
                to_change_columns
            ):
                if prompt["type"] == "code":
                    execute_code_from_prompt(
                        prompt, cache, instance_id, table_name, db_dir
                    )
                elif prompt["type"] == "llm":
                    execute_llm_from_prompt(
                        prompt, cache, instance_id, table_name, db_dir
                    )
        db_metadata.update_process_step(process_id, pname)

    if not rows_updated and len(to_change_columns) == 0:
        db_metadata.update_process_step(process_id, "no_update")
        db_metadata.write_to_log(process_id, success=False)
        perm_instance_lock.release_exclusive_lock(delete=True)
        instance_lock.release_exclusive_lock()
    else:
        _file_operations.materialize_table_folder(
            perm_instance_id, instance_id, table_name, db_dir
        )
        db_metadata.update_process_step(process_id, "materalized")
        db_metadata.write_to_log(process_id)
        perm_instance_lock.release_exclusive_lock()
        instance_lock.release_exclusive_lock(delete=True)
        for lock in dep_locks:
            lock.release_shared_lock()


def delete_table(table_name: str, db_dir: str, author: str):
    """
    TODO: Deletion Future Safety is up to users right now.
    i.e. I don't take into account if  I need a deleted table
    """
    db_metadata = MetadataStore(db_dir)
    lock = DatabaseLock(db_dir, table_name)
    lock.acquire_exclusive_lock()
    operation = "delete_table"
    process_id = db_metadata.start_new_process(author, operation, table_name)
    _file_operations.delete_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock(delete=True)


def restart_delete_table(author: str, process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f"Error Fetching Data for process {process_id}. Not executed.")
        raise e
    lock = DatabaseLock(db_dir, table_name, restart=True)
    lock.acquire_exclusive_lock()
    _file_operations.delete_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock(delete=True)


def delete_table_instance(instance_id: str, table_name: str, db_dir: str, author: str):
    db_metadata = MetadataStore(db_dir)
    operation = "delete_table_instance"
    lock = DatabaseLock(db_dir, table_name, instance_id)
    lock.acquire_exclusive_lock()
    process_id = db_metadata.start_new_process(
        author, operation, table_name, instance_id
    )
    _file_operations.delete_table_folder(table_name, db_dir, instance_id)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock(delete=True)


def restart_delete_table_instance(author: str, process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        instance_id = process.instance_id
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f"Error Fetching Data for process {process_id}. Not executed.")
        raise e
    lock = DatabaseLock(db_dir, table_name, instance_id, restart=True)
    lock.acquire_exclusive_lock()
    _file_operations.delete_table_folder(table_name, db_dir, instance_id)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock(delete=True)


def setup_table_instance(
    version: str,
    table_name: str,
    db_dir: str,
    author: str,
    origin_id: str = "",
    prompts: list[str] = [],
    gen_prompt: str = "",
):
    if len(prompts) != 0 and gen_prompt not in prompts:
        raise ValueError("Need to Define gen_prompt")
    db_metadata = MetadataStore(db_dir)
    table_lock = DatabaseLock(db_dir, table_name)
    table_lock.acquire_shared_lock()
    table_exists = db_metadata.check_table_instance(table_name)
    if not table_exists:
        table_lock.release_shared_lock(delete=True)
        raise ValueError("Table Does not Exist")
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if not allow_multiple and version == "":
        instance_id = "TEMP_"
    elif not allow_multiple and version != "":
        raise ValueError("Cannot Define Instance ID for Table without Versioning")
    elif allow_multiple and version == "":
        raise ValueError("Must Define Instance ID for Table with Versioning")
    else:
        instance_id = "TEMP_" + version
    start_time = time.time()
    lock = DatabaseLock(db_dir, table_name, instance_id)
    lock.acquire_shared_lock()
    table_lock.release_shared_lock()
    lock.acquire_exclusive_lock()
    data = {"gen_prompt": gen_prompt, "prompts": prompts, "origin_id": origin_id}
    process_id = db_metadata.start_new_process(
        author,
        "setup_table_instance",
        table_name,
        instance_id,
        data=data,
        start_time=start_time,
    )
    _file_operations.setup_table_instance_folder(
        instance_id, table_name, db_dir, origin_id, prompts, gen_prompt
    )
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()


def restart_setup_table_instance(author: str, process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        instance_id = process.instance_id
        gen_prompt = process.data["gen_prompt"]
        prompts = process.data["prompts"]
        origin = process.data["origin"]
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f"Error Fetching Data for process {process_id}. Not executed.")
        print(e)
    lock = DatabaseLock(db_dir, table_name, instance_id, restart=True)
    lock.acquire_exclusive_lock()
    _file_operations.setup_table_instance_folder(
        instance_id, table_name, db_dir, origin, prompts, gen_prompt
    )
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()


def setup_table(table_name: str, db_dir: str, author: str, allow_multiple: bool = True):
    db_metadata = MetadataStore(db_dir)
    lock = DatabaseLock(db_dir, table_name)
    lock.acquire_exclusive_lock()
    process_id = db_metadata.start_new_process(
        author, "setup_table", table_name, data={"allow_multiple": allow_multiple}
    )
    _file_operations.setup_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    # write to metadata about multiple
    lock.release_exclusive_lock()


def restart_setup_table(author: str, process_id: str, db_dir: str):
    db_metadata = MetadataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        process.data["allow_multiple"]
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f"Error Fetching Data for process {process_id}. Not executed.")
        print(e)
    lock = DatabaseLock(db_dir, table_name, restart=True)
    lock.acquire_exclusive_lock()

    _file_operations.setup_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()


def restart_database(author: str, db_dir: str, excluded_processes: list[str] = []):
    db_lock = DatabaseLock(db_dir, table_name="RESTART")
    db_lock.acquire_exclusive_lock()
    db_metadata = MetadataStore(db_dir)
    active_logs = db_metadata.get_active_processes(to_print=False)
    data = {
        "excluded_processes": excluded_processes,
        "active_ids": list(active_logs.keys()),
    }
    process_id = db_metadata.start_new_process(
        author, "restart_database", table_name="", data=data
    )

    for id in active_logs.keys():
        operation = active_logs[id].operation
        if id == process_id:
            continue
        if id in excluded_processes:
            if operation == "execute_table":
                print("hello")
                db_metadata.update_process_step(id, "stop_execute")
            else:
                raise ValueError(f"Can Only Stop Table Executions Right Now: {id}")

        if operation == "restart_database":
            db_metadata.write_to_log(id, success=False)
        elif "write_log" in active_logs[id].complete_steps:
            db_metadata.write_to_log(process_id, success=None)
        elif operation == "setup_table":
            restart_setup_table(author, id, db_dir)
        elif operation == "setup_table_instance":
            restart_setup_table_instance(author, id, db_dir)
        elif operation == "delete_table":
            restart_delete_table(author, id, db_dir)
        elif operation == "delete_table_instance":
            restart_delete_table_instance(author, id, db_dir)
        elif operation == "execute_table":
            print("HELLO")
            restart_execute_table(author, id, db_dir)
        db_metadata.update_process_step(process_id, (id, operation))

    db_metadata.write_to_log(process_id)
    db_lock.release_exclusive_lock()
