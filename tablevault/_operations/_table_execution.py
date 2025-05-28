from tablevault.helper.metadata_store import MetadataStore
from tablevault.helper import file_operations
from tablevault.defintions.types import ExternalDeps
from tablevault.dataframe_helper import table_operations
from tablevault.defintions import constants
from tablevault.builders.load_builder import load_builder

def execute_instance(
    table_name: str,
    instance_id: str,
    top_builder_names: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id: str,
    origin_table: str,
    process_id: str,
    db_metadata: MetadataStore,
):
    log = db_metadata.get_active_processes()[process_id]
    prev_completed_steps = log.complete_steps
    update_rows = log.data["update_rows"]
    yaml_builders = file_operations.get_yaml_builders(
        instance_id, table_name, db_metadata.db_dir
    )
    builders = {
        builder_name: load_builder(ybuilder)
        for builder_name, ybuilder in yaml_builders.items()
    }
    column_dtypes = {}

    yaml_descript = file_operations.get_description(
        instance_id, table_name, db_metadata.db_dir
    )
    yaml_descript[constants.DESCRIPTION_BUILDER_DEPENDENCIES] = external_deps
    file_operations.write_description(
        yaml_descript, instance_id, table_name, db_metadata.db_dir
    )

    for builder_name in top_builder_names:
        column_dtypes.update(builders[builder_name].dtypes)
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
    cache = {}
    for i, builder_name in enumerate(top_builder_names):
        if builder_name in prev_completed_steps:
            continue
        cache = table_operations.fetch_table_cache(
            external_deps[builder_name],
            instance_id,
            table_name,
            db_metadata,
            cache,
        )
        if i == 0:
            update_rows = builders[builder_name].execute(
                cache, instance_id, table_name, db_metadata.db_dir, process_id
            )
            db_metadata.update_process_data(process_id, {"update_rows": update_rows})
        elif not update_rows and len(changed_columns) == 0:
            db_metadata.update_process_step(process_id, builder_name)
            continue
        else:
            if update_rows or set(builders[builder_name].changed_columns).issubset(
                changed_columns
            ):
                builders[builder_name].execute(
                    cache, instance_id, table_name, db_metadata.db_dir, process_id
                )
        db_metadata.update_process_step(process_id, builder_name)
