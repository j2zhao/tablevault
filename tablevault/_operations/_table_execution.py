from tablevault._helper.metadata_store import MetadataStore
from tablevault._helper import file_operations
from tablevault.defintions.types import ExternalDeps
from tablevault.prompts.utils import table_operations
from tablevault.defintions import constants
from tablevault.prompts.load_prompt import load_prompt

def execute_instance(
    table_name: str,
    instance_id: str,
    perm_instance_id: str,
    top_pnames: list[str],
    to_change_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    prev_instance_id: str,
    prev_completed_steps: list[str],
    update_rows: bool,
    process_id: str,
    db_metadata: MetadataStore,
):
    yaml_prompts = file_operations.get_yaml_prompts(instance_id, table_name, db_metadata.db_dir)
    prompts = {pname: load_prompt(yprompt) for pname, yprompt in yaml_prompts.items()}
    column_dtypes = {}
    for pname in top_pnames:
        column_dtypes.update(prompts[pname].dtypes)
    
    if not constants.EX_CLEAR_TABLE not in prev_completed_steps:
        if prev_instance_id != "":
            file_operations.copy_table(
                instance_id, prev_instance_id, table_name, db_metadata.db_dir
            )
        table_operations.update_table_columns(
            to_change_columns,
            all_columns,
            column_dtypes,
            instance_id,
            table_name,
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, constants.EX_CLEAR_TABLE)

    for i, pname in enumerate(top_pnames):
        if pname in prev_completed_steps:
            continue
        elif not update_rows and len(to_change_columns) == 0:
            db_metadata.update_process_step(process_id, pname)
            continue

        cache = table_operations.fetch_table_cache(
            external_deps[pname],
            instance_id,
            table_name,
            db_metadata.db_dir,
        )
        if i == 0:
            update_rows = prompts[pname].execute(cache, instance_id, table_name, db_metadata.db_dir)
            db_metadata.update_process_data(process_id, {"update_rows": update_rows})

        else:
            if update_rows or set(prompts[pname].changed_columns).issubset(
                to_change_columns
            ):
                prompts[pname].execute(cache, instance_id, table_name, db_metadata.db_dir)
        db_metadata.update_process_step(process_id, pname)

    if not update_rows and len(to_change_columns) == 0:
        if constants.EX_NO_UPDATE not in prev_completed_steps:
            db_metadata.update_process_step(process_id, constants.EX_NO_UPDATE)
    else:
        if constants.EX_MAT not in prev_completed_steps:
            file_operations.materialize_table_folder(
                perm_instance_id, instance_id, table_name, db_metadata.db_dir
            )
            db_metadata.update_process_step(process_id, constants.EX_MAT)
