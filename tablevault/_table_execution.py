from tablevault._utils.metadata_store import MetadataStore
from tablevault._utils import file_operations
from tablevault._prompt_parsing.types import Prompt, Cache, ExternalDeps
from tablevault._prompt_parsing import prompt_parser
from tablevault._prompt_execution.parse_code import (
    execute_code_from_prompt,
    execute_gen_table_from_prompt,
)
from tablevault._prompt_execution.parse_llm import execute_llm_from_prompt
import pandas as pd 


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
    
    if not "clear_table" not in prev_completed_steps:
        if prev_instance_id != '':
            file_operations.copy_table(instance_id, prev_instance_id,table_name, db_metadata.db_dir)
        _update_table_columns(
            to_change_columns, all_columns, instance_id, table_name, db_metadata.db_dir
        )
        db_metadata.update_process_step(process_id, "clear_table")

    for i, pname in enumerate(top_pnames):
        if pname in prev_completed_steps:
            continue
        elif not update_rows and len(to_change_columns) == 0:
            db_metadata.update_process_step(process_id, pname)
            continue
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _fetch_table_cache(
            external_deps[pname],
            instance_id,
            table_name,
            db_metadata.db_dir,
        )
        if i == 0:
            update_rows = execute_gen_table_from_prompt(
                prompt, cache, instance_id, table_name, db_metadata.db_dir
            )
            db_metadata.update_process_data(process_id, {"update_rows": update_rows})
    
        else:
            if update_rows or set(prompt["parsed_changed_columns"]).issubset(
                to_change_columns
            ):
                if prompt["type"] == "code":
                    execute_code_from_prompt(
                        prompt, cache, instance_id, table_name, db_metadata.db_dir
                    )
                elif prompt["type"] == "llm":
                    execute_llm_from_prompt(
                        prompt, cache, instance_id, table_name, db_metadata.db_dir
                    )
        db_metadata.update_process_step(process_id, pname)
    
    if not update_rows and len(to_change_columns) == 0:
        if 'no_update' not in prev_completed_steps:
            db_metadata.update_process_step(process_id, "no_update")
    else:
        if "materalized" not in prev_completed_steps:
            file_operations.materialize_table_folder(
                perm_instance_id, instance_id, table_name, db_metadata.db_dir
            )
            db_metadata.update_process_step(process_id, "materalized")

def _update_table_columns(
    to_change_columns: list,
    all_columns: list,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    df = file_operations.get_table(instance_id, table_name, db_dir)
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
    file_operations.write_table(df, instance_id, table_name, db_dir)


def _fetch_table_cache(
    external_dependencies: list,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> Cache:
    cache = {}
    cache["self"] = file_operations.get_table(instance_id, table_name, db_dir)

    for dep in external_dependencies:
        table, _, instance, _, latest = dep
        if latest:
            cache[table] = file_operations.get_table(instance, table, db_dir)
        else:
            cache[(table, instance)] = file_operations.get_table(
                instance, table, db_dir
            )
    return cache