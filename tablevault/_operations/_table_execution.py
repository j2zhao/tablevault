from tablevault.helper.metadata_store import MetadataStore
from tablevault.helper import file_operations
from tablevault.defintions.types import ExternalDeps
from tablevault.prompts.utils import table_operations, table_string
from tablevault.defintions import constants
from tablevault.prompts.load_prompt import load_prompt

def execute_instance(
    table_name: str,
    instance_id: str,
    top_pnames: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id:str,
    origin_table:str,
    process_id: str,
    db_metadata: MetadataStore,
):  
    
    # db_metadata.start_execute_operation(table_name)
    log = db_metadata.get_active_processes()[process_id]
    prev_completed_steps = log.complete_steps
    update_rows = log.data['update_rows']
    yaml_prompts = file_operations.get_yaml_prompts(instance_id, table_name, db_metadata.db_dir)
    prompts = {pname: load_prompt(yprompt) for pname, yprompt in yaml_prompts.items()}
    column_dtypes = {}

    yaml_descript = file_operations.get_description(instance_id, table_name, db_metadata.db_dir)
    yaml_descript[constants.DESCRIPTION_PROMPT_DEPENDENCIES] = external_deps
    file_operations.write_description(yaml_descript,instance_id, table_name, db_metadata.db_dir)

    for pname in top_pnames:
        column_dtypes.update(prompts[pname].dtypes)
    if constants.EX_CLEAR_TABLE not in prev_completed_steps:
        if origin_id != "": 
            file_operations.copy_table(
                instance_id, table_name, origin_id, origin_table, db_metadata.db_dir
            )
        
        table_operations.update_table_columns(
            changed_columns,
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
        cache = table_operations.fetch_table_cache(
            external_deps[pname],
            instance_id,
            table_name,
            db_metadata,
        )
        # print(cache[constants.OUTPUT_SELF])
        # print(cache[constants.OUTPUT_SELF].dtypes)
        # raise ValueError()
        if i == 0:
            update_rows = prompts[pname].execute(cache, instance_id, table_name, db_metadata.db_dir)
            db_metadata.update_process_data(process_id, {"update_rows": update_rows})
        elif not update_rows and len(changed_columns) == 0:
            db_metadata.update_process_step(process_id, pname)
            continue
        else:
            if update_rows or set(prompts[pname].changed_columns).issubset(
                changed_columns
            ):
                prompts[pname].execute(cache, instance_id, table_name, db_metadata.db_dir)
        db_metadata.update_process_step(process_id, pname)
    