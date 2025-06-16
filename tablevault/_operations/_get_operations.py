from tablevault._helper import file_operations
from tablevault._helper.metadata_store import MetadataStore
from tablevault._dataframe_helper import table_operations
from tablevault._defintions import constants
from tablevault._dataframe_helper import artifact
import pandas as pd
from typing import Optional
from rich.tree import Tree


def get_process_completion(
    process_id: str,
    db_dir: str,
) -> bool:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.check_written(process_id)


def get_artifact_folder(
    table_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
    db_dir: str,
):
    if instance_id == "":
        if is_temp:
            instance_id = constants.TEMP_INSTANCE + version
        else:
            db_metadata = MetadataStore(db_dir)
            _, _, instance_id = db_metadata.get_last_table_update(table_name, version)

    return artifact.get_artifact_folder(instance_id, table_name, db_dir)


def get_active_processes(db_dir: str):
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_active_processes()


def get_table_instances(
    table_name: str,
    version: str,
    db_dir: str,
    include_temp: bool,
):
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version, include_temp)


def get_descriptions(
    instance_id: str,
    table_name: str,
    db_dir: str,
):
    return file_operations.get_description(instance_id, table_name, db_dir)


def get_file_tree(
    instance_id: str,
    table_name: str,
    code_files: bool,
    builder_files: bool,
    metadata_files: bool,
    artifact_files: bool,
    db_dir: str,
) -> Tree:
    tree = file_operations.get_file_tree(
        instance_id,
        table_name,
        code_files,
        builder_files,
        metadata_files,
        artifact_files,
        db_dir,
    )
    return tree


def get_code_modules_list(
    db_dir: str,
):
    return file_operations.get_code_module_names(db_dir)


def get_builders_list(
    instance_id: str,
    table_name: str,
    version: str,
    is_temp: bool,
    db_dir: str,
):
    if instance_id == "":
        if is_temp:
            instance_id = constants.TEMP_INSTANCE + version
        else:
            db_metadata = MetadataStore(db_dir)
            _, _, instance_id = db_metadata.get_last_table_update(table_name, version)
    return file_operations.get_builder_names(instance_id, table_name, db_dir)


def get_builder_str(
    table_name: str,
    builder_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
    db_dir: str,
) -> str:
    if builder_name == "":
        builder_name = builder_name + constants.INDEX_BUILDER_SUFFIX
    if instance_id == "":
        if is_temp:
            instance_id = constants.TEMP_INSTANCE + version
        else:
            db_metadata = MetadataStore(db_dir)
            _, _, instance_id = db_metadata.get_last_table_update(table_name, version)
    return file_operations.get_builder_str(
        builder_name, instance_id, table_name, db_dir
    )


def get_code_module_str(module_name: str, db_dir: str) -> str:
    return file_operations.get_code_module_str(module_name, db_dir)


def get_dataframe(
    instance_id: str,
    table_name: str,
    version: str,
    active_only: bool,
    successful_only: bool,
    rows: Optional[int],
    full_artifact_path: bool,
    db_dir,
) -> tuple[pd.DataFrame, str]:
    db_metadata = MetadataStore(db_dir)
    if instance_id == "":
        _, _, instance_id = db_metadata.get_last_table_update(
            table_name, version, active_only=active_only, success_only=successful_only
        )
    df = table_operations.get_table(
        instance_id,
        table_name,
        db_dir,
        rows,
        artifact_dir=full_artifact_path,
        get_index=False,
    )
    return df, instance_id
