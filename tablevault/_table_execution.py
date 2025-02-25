from tablevault._utils.metadata_store import MetadataStore
from tablevault._utils import file_operations
from tablevault._prompt_parsing.types import Prompt, ExternalDeps
from tablevault._prompt_parsing import prompt_parser
from tablevault._prompt_execution.parse_code import (
    execute_code_from_prompt,
    execute_gen_table_from_prompt,
)
from tablevault._prompt_execution.parse_llm import execute_llm_from_prompt
from tablevault._utils import _table_operations


def execute_instance(
    table_name: str,
    instance_id: str,
    perm_instance_id: str,
    prompts: dict[Prompt],
    top_pnames: list[str],
    to_change_columns: list[str],
    all_columns: list[str],
    column_dtypes: dict[str, str],
    external_deps: ExternalDeps,
    prev_instance_id: str,
    prev_completed_steps: list[str],
    update_rows: bool,
    process_id: str,
    db_metadata: MetadataStore,
):

    if not "clear_table" not in prev_completed_steps:
        if prev_instance_id != "":
            file_operations.copy_table(
                instance_id, prev_instance_id, table_name, db_metadata.db_dir
            )
        _table_operations.update_table_columns(
            to_change_columns,
            all_columns,
            column_dtypes,
            instance_id,
            table_name,
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, "clear_table")

    for i, pname in enumerate(top_pnames):
        if pname in prev_completed_steps:
            continue
        elif not update_rows and len(to_change_columns) == 0:
            db_metadata.update_process_step(process_id, pname)
            continue
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _table_operations.fetch_table_cache(
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
        if "no_update" not in prev_completed_steps:
            db_metadata.update_process_step(process_id, "no_update")
    else:
        if "materalized" not in prev_completed_steps:
            file_operations.materialize_table_folder(
                perm_instance_id, instance_id, table_name, db_metadata.db_dir
            )
            db_metadata.update_process_step(process_id, "materalized")
