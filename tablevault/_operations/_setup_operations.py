from tablevault._helper.metadata_store import MetadataStore, check_top_process
from tablevault._defintions import tv_errors
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper import file_operations
from tablevault._helper.utils import gen_tv_id
from tablevault._defintions import constants, types
from tablevault._defintions.types import SETUP_OUTPUT, ExternalDeps, InternalDeps
from tablevault._builders.base_builder_type import TVBuilder
from tablevault._builders.load_builder import load_builder
from tablevault._helper.utils import topological_sort
from tablevault._dataframe_helper import table_operations
from tablevault._helper.copy_write_file import CopyOnWriteFile
import pandas as pd
from typing import Optional
from tablevault._operations._takedown_operations import TAKEDOWN_MAP


def _process_lock_results(
    results: list[str],
    process_id: str,
    db_locks: DatabaseLock,
    db_metadata: MetadataStore,
):
    for pid in results:
        logs = db_metadata.get_active_processes()
        active_pids = logs.keys()
        if pid in logs:
            _, parent_pid = check_top_process(pid, active_pids)
            if parent_pid in logs and logs[parent_pid].force_takedown:
                try:
                    db_metadata.stop_operation(parent_pid, force=False)
                    error = (
                        tv_errors.TVLockError.__name__,
                        str(f"stopped by {process_id}"),
                    )
                    if logs[parent_pid].start_success is None:
                        db_metadata.update_process_start_status(
                            parent_pid, False, error
                        )
                    elif logs[parent_pid].execution_success is None:
                        db_metadata.update_process_execution_status(
                            parent_pid, False, error
                        )
                    TAKEDOWN_MAP[logs[parent_pid].operation](
                        parent_pid, db_metadata, db_locks, parent_pid
                    )
                except Exception:
                    raise tv_errors.TVLockError(
                        f"Unable to acquire lock because of {pid}."
                    )


def _acquire_lock(
    acquire_type: str,
    process_id: str,
    db_locks: DatabaseLock,
    db_metadata: MetadataStore,
    table_name: str = "",
    instance_id: str = "",
) -> tuple[str, str, str]:
    if acquire_type == "exclusive":
        output, success = db_locks.acquire_exclusive_lock(table_name, instance_id)
    elif acquire_type == "shared":
        output, success = db_locks.acquire_shared_lock(table_name, instance_id)
    if success:
        return output
    else:
        _process_lock_results(output, process_id, db_locks, db_metadata)
        if acquire_type == "exclusive":
            output, success = db_locks.acquire_exclusive_lock(table_name, instance_id)
        elif acquire_type == "shared":
            output, success = db_locks.acquire_shared_lock(table_name, instance_id)
        if not success:
            raise tv_errors.TVLockError(
                f"Unable to acquire lock {table_name} {instance_id}."
            )
        return output


def setup_create_code_module(
    module_name: str,
    copy_dir: str,
    text: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if module_name == "" and copy_dir == "":
        raise tv_errors.TVArgumentError(
            "One of module_name and copy_dir needs to be filled"
        )
    funct_kwargs = {"module_name": module_name, "copy_dir": copy_dir, "text": text}
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name=constants.CODE_FOLDER
    )
    file_operations.copy_folder_to_temp(
        process_id,
        db_metadata.db_dir,
        file_writer,
        subfolder=constants.CODE_FOLDER,
    )
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_delete_code_module(
    module_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    funct_kwargs = {"module_name": module_name}
    _acquire_lock("exclusive", process_id, db_locks, db_metadata, constants.CODE_FOLDER)
    file_operations.copy_folder_to_temp(
        process_id,
        db_metadata.db_dir,
        file_writer,
        subfolder=constants.CODE_FOLDER,
    )
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_create_builder_file(
    builder_name: str,
    table_name: str,
    version: str,
    copy_dir: str,
    text: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    instance_id = constants.TEMP_INSTANCE + version
    if builder_name == "" and copy_dir == "":
        builder_name = f"{table_name}{constants.INDEX_BUILDER_SUFFIX}"

    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    file_operations.copy_folder_to_temp(
        process_id,
        db_metadata.db_dir,
        file_writer,
        instance_id=instance_id,
        table_name=table_name,
        subfolder=constants.BUILDER_FOLDER,
    )
    funct_kwargs = {
        "builder_name": builder_name,
        "instance_id": instance_id,
        "table_name": table_name,
        "copy_dir": copy_dir,
        "text": text,
    }

    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_delete_builder_file(
    builder_name: str,
    table_name: str,
    version: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    instance_id = constants.TEMP_INSTANCE + version
    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    file_operations.copy_folder_to_temp(
        process_id,
        db_metadata.db_dir,
        file_writer,
        table_name=table_name,
        instance_id=instance_id,
        subfolder=constants.BUILDER_FOLDER,
    )
    funct_kwargs = {
        "builder_name": builder_name,
        "instance_id": instance_id,
        "table_name": table_name,
    }

    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_rename_table(
    new_table_name: str,
    table_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if new_table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {new_table_name}")
    existance = db_metadata.get_table_instances(table_name, "")
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    existance = db_metadata.get_table_instances(new_table_name, "")
    if existance is not None:
        raise tv_errors.TVArgumentError(f"{new_table_name} exist")
    _acquire_lock("exclusive", process_id, db_locks, db_metadata, table_name)
    db_locks.make_lock_path(new_table_name)
    db_locks.make_lock_path(new_table_name, constants.ARTIFACT_FOLDER)
    instance_ids = db_metadata.get_table_instances(table_name, "")
    for id in instance_ids:
        db_locks.make_lock_path(new_table_name, id)
    _acquire_lock("exclusive", process_id, db_locks, db_metadata, new_table_name)
    funct_kwargs = {"new_table_name": new_table_name, "table_name": table_name}
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_delete_table(
    table_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    existance = db_metadata.get_table_instances(table_name, "")
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    _acquire_lock("exclusive", process_id, db_locks, db_metadata, table_name)
    file_operations.copy_folder_to_temp(
        process_id, db_metadata.db_dir, file_writer, table_name=table_name
    )
    funct_kwargs = {"table_name": table_name}
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_delete_instance(
    table_name: str,
    instance_id: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    file_operations.copy_folder_to_temp(
        process_id,
        db_metadata.db_dir,
        file_writer,
        instance_id=instance_id,
        table_name=table_name,
    )
    funct_kwargs = {"table_name": table_name, "instance_id": instance_id}
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_materialize_instance(
    instance_id: str,
    table_name: str,
    version: str,
    perm_instance_id: str,
    origin_id: str,
    origin_table: str,
    changed_columns: list[str],
    all_columns: list[str],
    dtypes: list[str],
    dependencies: list[tuple[str, str]],
    success: bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")

    if "_" not in process_id:
        instance_id = constants.TEMP_INSTANCE + version
        if perm_instance_id == "":
            start_time = db_metadata.get_active_processes()[process_id].start_time
            perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
            perm_instance_id = version + perm_instance_id
    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    # db_metadata.update_process_data(process_id, funct_kwargs)
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    db_locks.make_lock_path(table_name, perm_instance_id)
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, perm_instance_id
    )
    table_data = file_operations.get_description(
        instance_id="", table_name=table_name, db_dir=db_metadata.db_dir
    )

    if "_" not in process_id:
        instance_data = file_operations.get_description(
            instance_id, table_name, db_metadata.db_dir
        )
        if not instance_data[constants.DESCRIPTION_EDIT]:
            raise tv_errors.TVArgumentError(
                "Instance cannot be externally materialized."
            )
        if constants.DESCRIPTION_ORIGIN in instance_data:
            origin_id, origin_table = instance_data[constants.DESCRIPTION_ORIGIN]
        table_df = table_operations.get_table(
            instance_id, table_name, db_metadata.db_dir, file_writer=file_writer
        )

        all_columns = list(table_df.columns)
        if constants.TABLE_INDEX in all_columns:
            all_columns.remove(constants.TABLE_INDEX)
        changed_columns = all_columns
        if origin_id != "":
            try:
                temp_lock = _acquire_lock(
                    "shared", process_id, db_locks, db_metadata, origin_table, origin_id
                )
                changed_columns = table_operations.check_changed_columns(
                    table_df,
                    origin_id,
                    origin_table,
                    db_metadata.db_dir,
                    file_writer,
                )
                db_locks.release_lock(temp_lock)
            except tv_errors.TVLockError:
                pass

    if not table_data[constants.TABLE_ALLOW_MARTIFACT] and success:
        _acquire_lock(
            "exclusive",
            process_id,
            db_locks,
            db_metadata,
            table_name,
            constants.ARTIFACT_FOLDER,
        )
        file_operations.copy_folder_to_temp(
            process_id,
            db_metadata.db_dir,
            file_writer,
            table_name=table_name,
            subfolder=constants.ARTIFACT_FOLDER,
        )
    funct_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "perm_instance_id": perm_instance_id,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "dtypes": dtypes,
        "all_columns": all_columns,
        "changed_columns": changed_columns,
        "dependencies": dependencies,
        "success": success,
    }

    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_stop_process(
    to_stop_process_id: str,
    force: bool,
    materialize: bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
):
    logs, process_ids_ = db_metadata.stop_operation(to_stop_process_id, force)
    step_ids = []
    step_ids.append(process_id + "_" + gen_tv_id())
    materialize_ops = {}
    if materialize:
        prev_materialized = False
        for process_id_ in process_ids_:
            if (
                logs[process_id_].operation == constants.MAT_OP
                and logs[process_id_].execution_success
            ):
                prev_materialized = True
        if (
            not prev_materialized
            and logs[to_stop_process_id].start_success
            and not logs[to_stop_process_id].execution_success
        ):
            if logs[to_stop_process_id].operation == constants.EXECUTE_OP:
                step_ids.append(process_id + "_" + gen_tv_id())
                materialize_ops["instance_id"] = logs[to_stop_process_id].data[
                    "instance_id"
                ]
                materialize_ops["table_name"] = logs[to_stop_process_id].data[
                    "table_name"
                ]
                materialize_ops["perm_instance_id"] = logs[to_stop_process_id].data[
                    "perm_instance_id"
                ]
                materialize_ops["origin_id"] = logs[to_stop_process_id].data[
                    "origin_id"
                ]
                materialize_ops["origin_table"] = logs[to_stop_process_id].data[
                    "origin_table"
                ]
                materialize_ops["dtypes"] = []
                materialize_ops["all_columns"] = logs[to_stop_process_id].data[
                    "all_columns"
                ]
                materialize_ops["changed_columns"] = logs[to_stop_process_id].data[
                    "changed_columns"
                ]
                dependencies = []
                external_deps = logs[to_stop_process_id].data["external_deps"]
                for builder_name in external_deps:
                    for tname, _, id, _, _ in external_deps[builder_name]:
                        dependencies.append([id, tname])
                materialize_ops["dependencies"] = dependencies
            elif logs[to_stop_process_id].operation == constants.WRITE_INSTANCE_OP:
                step_ids.append(process_id + "_" + gen_tv_id())
                materialize_ops["instance_id"] = logs[to_stop_process_id].data[
                    "instance_id"
                ]
                materialize_ops["table_name"] = logs[to_stop_process_id].data[
                    "table_name"
                ]
                materialize_ops["perm_instance_id"] = logs[to_stop_process_id].data[
                    "perm_instance_id"
                ]
                materialize_ops["origin_id"] = logs[to_stop_process_id].data[
                    "origin_id"
                ]
                materialize_ops["origin_table"] = logs[to_stop_process_id].data[
                    "origin_table"
                ]
                materialize_ops["dtypes"] = logs[to_stop_process_id].data["dtypes"]
                materialize_ops["all_columns"] = logs[to_stop_process_id].data[
                    "all_columns"
                ]
                materialize_ops["changed_columns"] = logs[to_stop_process_id].data[
                    "changed_columns"
                ]
                materialize_ops["dependencies"] = logs[to_stop_process_id].data[
                    "dependencies"
                ]
            elif logs[to_stop_process_id].operation == constants.MAT_OP:
                step_ids.append(process_id + "_" + gen_tv_id())
                materialize_ops["instance_id"] = logs[to_stop_process_id].data[
                    "instance_id"
                ]
                materialize_ops["table_name"] = logs[to_stop_process_id].data[
                    "table_name"
                ]
                materialize_ops["perm_instance_id"] = logs[to_stop_process_id].data[
                    "perm_instance_id"
                ]
                materialize_ops["origin_id"] = logs[to_stop_process_id].data[
                    "origin_id"
                ]
                materialize_ops["origin_table"] = logs[to_stop_process_id].data[
                    "origin_table"
                ]
                materialize_ops["dtypes"] = logs[to_stop_process_id].data["dtypes"]
                materialize_ops["all_columns"] = logs[to_stop_process_id].data[
                    "all_columns"
                ]
                materialize_ops["changed_columns"] = logs[to_stop_process_id].data[
                    "changed_columns"
                ]
                materialize_ops["dependencies"] = logs[to_stop_process_id].data[
                    "dependencies"
                ]

    funct_kwargs = {
        "process_ids": process_ids_,
        "materialize_args": materialize_ops,
        "step_ids": step_ids,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_write_instance_inner(
    table_df: Optional[pd.DataFrame],
    instance_id: str,
    table_name: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    funct_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "table_df": None,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    funct_kwargs["table_df"] = table_df
    return funct_kwargs


def setup_write_instance(
    table_df: Optional[pd.DataFrame],
    table_name: str,
    version: str,
    dtypes: dict[str, str],
    dependencies: list[tuple[str, str]],
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    step_ids = []
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if table_df is None:
        raise tv_errors.TVProcessError("Cannot Re-execute Write Table")
    if len(table_df.columns) == 0 or len(table_df) == 0:
        raise tv_errors.TVArgumentError("Empty Table")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    instance_id = constants.TEMP_INSTANCE + version
    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    instance_data = file_operations.get_description(
        instance_id, table_name, db_metadata.db_dir
    )
    if not instance_data[constants.DESCRIPTION_EDIT]:
        raise tv_errors.TVArgumentError(
            "External edit table cannot be executed for this instance."
        )

    step_ids.append(process_id + "_" + gen_tv_id())
    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    start_time = db_metadata.get_active_processes()[process_id].start_time
    perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
    perm_instance_id = version + perm_instance_id
    db_locks.make_lock_path(table_name, perm_instance_id)
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, perm_instance_id
    )
    if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
        _acquire_lock(
            "exclusive",
            process_id,
            db_locks,
            db_metadata,
            table_name,
            constants.ARTIFACT_FOLDER,
        )
    step_ids.append(process_id + "_" + gen_tv_id())

    all_columns = list(table_df.columns)
    if constants.TABLE_INDEX in all_columns:
        all_columns.remove(constants.TABLE_INDEX)
    changed_columns = all_columns
    origin_id, origin_table = instance_data[constants.DESCRIPTION_ORIGIN]
    if origin_id != "":
        try:
            temp_lock = _acquire_lock(
                "shared", process_id, db_locks, db_metadata, origin_table, origin_id
            )
            changed_columns = table_operations.check_changed_columns(
                table_df, origin_id, origin_table, db_metadata.db_dir, file_writer
            )
            db_locks.release_lock(temp_lock)
        except tv_errors.TVLockError:
            pass

    dependencies_ = []
    for id, tname in dependencies:
        if id is None or id == "":
            _, _, id = db_metadata.get_last_table_update(table_name)
        dependencies_.append([id, tname])

    funct_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "dtypes": dtypes,
        "dependencies": dependencies_,
        "perm_instance_id": perm_instance_id,
        "step_ids": step_ids,
        "table_df": None,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "all_columns": all_columns,
        "changed_columns": changed_columns,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    for col, dtype in dtypes.items():
        if col in table_df.columns:
            table_df[col] = table_df[col].astype(dtype)
        else:
            raise tv_errors.TVArgumentError("Artifact column not in Dataframe")
    funct_kwargs["table_df"] = table_df
    return funct_kwargs


def setup_execute_instance_inner(
    instance_id: str,
    table_name: str,
    top_builder_names: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    internal_deps: InternalDeps,
    external_deps: ExternalDeps,
    origin_id: str,
    origin_table: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    funct_kwargs = {
        "table_name": table_name,
        "instance_id": instance_id,
        "top_builder_names": top_builder_names,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "internal_deps": internal_deps,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "update_rows": True,
    }
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    if origin_id != "":
        _acquire_lock(
            "shared", process_id, db_locks, db_metadata, origin_table, origin_id
        )
    for builder_name in external_deps:
        for table, _, instance, _, _ in external_deps[builder_name]:
            _acquire_lock("shared", process_id, db_locks, db_metadata, table, instance)
            table_data = file_operations.get_description("", table, db_metadata.db_dir)
            if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
                db_locks.acquire_shared_lock(table, constants.ARTIFACT_FOLDER)
    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_execute_instance(
    table_name: str,
    version: str,
    force_execute: bool,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    start_time = db_metadata.get_active_processes()[process_id].start_time
    perm_instance_id = "_" + str(int(start_time)) + "_" + gen_tv_id()
    perm_instance_id = version + perm_instance_id
    instance_id = constants.TEMP_INSTANCE + version
    existance = db_metadata.get_table_instances(table_name, "", include_temp=True)
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    if instance_id not in existance:
        raise tv_errors.TVArgumentError("instance doesn't exist")
    instance_data = file_operations.get_description(
        instance_id, table_name, db_metadata.db_dir
    )
    if instance_data[constants.DESCRIPTION_EDIT]:
        raise tv_errors.TVArgumentError("External edit table cannot be executed.")
    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    db_locks.make_lock_path(table_name, perm_instance_id)
    if not table_data[constants.TABLE_SIDE_EFFECTS]:
        _acquire_lock(
            "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
        )
        _acquire_lock(
            "exclusive", process_id, db_locks, db_metadata, table_name, perm_instance_id
        )
        if not table_data[constants.TABLE_ALLOW_MARTIFACT]:
            db_locks.make_lock_path(table_name, perm_instance_id)
            _acquire_lock(
                "exclusive",
                process_id,
                db_locks,
                db_metadata,
                table_name,
                constants.ARTIFACT_FOLDER,
            )
    else:
        _acquire_lock("exclusive", process_id, db_locks, db_metadata, table_name)

    yaml_builders = file_operations.get_yaml_builders(
        instance_id, table_name, db_metadata.db_dir
    )
    builders = {
        builder_name: load_builder(ybuilder)
        for builder_name, ybuilder in yaml_builders.items()
    }

    if not force_execute:
        origin_table = ""
        origin_id = ""

        if constants.DESCRIPTION_ORIGIN in instance_data:
            origin_id, origin_table = instance_data[constants.DESCRIPTION_ORIGIN]
        if origin_id == "":
            _, _, origin_id = db_metadata.get_last_table_update(
                table_name, version, before_time=start_time
            )
            origin_table = table_name
        if origin_id == "":
            _, _, origin_id = db_metadata.get_last_table_update(
                table_name, "", before_time=start_time
            )
            origin_table = table_name

        if origin_id != "":
            _acquire_lock(
                "shared", process_id, db_locks, db_metadata, origin_table, origin_id
            )
    else:
        origin_id = ""
        origin_table = ""
    (
        top_builder_names,
        changed_columns,
        all_columns,
        internal_deps,
        external_deps,
        code_dependencies,
        custom_code,
    ) = parse_builders(
        builders,
        db_metadata,
        start_time,
        instance_id,
        table_name,
        origin_id,
        origin_table,
        file_writer=file_writer,
    )
    if custom_code:
        _acquire_lock(
            "shared", process_id, db_locks, db_metadata, constants.CODE_FOLDER
        )
    funct_kwargs = {
        "table_name": table_name,
        "instance_id": instance_id,
        "perm_instance_id": perm_instance_id,
        "top_builder_names": top_builder_names,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "update_rows": True,
        "internal_deps": internal_deps,
        "code_dependencies": code_dependencies,
    }
    funct_kwargs["step_ids"] = [process_id + "_" + gen_tv_id()]
    funct_kwargs["step_ids"].append(process_id + "_" + gen_tv_id())

    for builder_name in external_deps:
        for table, _, instance, _, _ in external_deps[builder_name]:
            _acquire_lock("shared", process_id, db_locks, db_metadata, table, instance)
            try:
                _acquire_lock(
                    "shared",
                    process_id,
                    db_locks,
                    db_metadata,
                    table,
                    constants.ARTIFACT_FOLDER,
                )
            except tv_errors.TVLockError:
                pass

    db_metadata.update_process_data(process_id, funct_kwargs)
    return funct_kwargs


def setup_create_instance(
    table_name: str,
    version: str,
    description: str,
    origin_id: str,
    origin_table: str,
    external_edit: bool,
    copy: bool,
    builder_names: list[str] | dict[str, str],
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES:
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    if version == "":
        version = constants.BASE_TABLE_VERSION
    elif "_" in version:
        raise tv_errors.TVArgumentError("version cannot contain '_' char")
    instance_id = constants.TEMP_INSTANCE + version
    existance = db_metadata.get_table_instances(table_name, "")
    if existance is None:
        raise tv_errors.TVArgumentError(f"{table_name} doesn't exist")
    start_time = db_metadata.get_active_processes()[process_id].start_time
    if origin_id != "":
        if origin_table == "":
            origin_table = table_name
        _acquire_lock(
            "shared", process_id, db_locks, db_metadata, origin_table, origin_id
        )
    elif copy:
        try:
            _, _, origin_id = db_metadata.get_last_table_update(
                table_name, "", before_time=start_time
            )
            origin_table = table_name
            _acquire_lock(
                "shared", process_id, db_locks, db_metadata, origin_table, origin_id
            )
        except tv_errors.TVArgumentError:
            origin_id = ""
            origin_table = ""
    index_builder = table_name + constants.INDEX_BUILDER_SUFFIX
    if origin_id == "" and index_builder not in builder_names and not external_edit:
        builder_names.append(index_builder)
    funct_kwargs = {
        "version": version,
        "instance_id": instance_id,
        "table_name": table_name,
        "description": description,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "external_edit": external_edit,
        "builder_names": builder_names,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.make_lock_path(table_name, instance_id)
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, table_name, instance_id
    )
    return funct_kwargs


def setup_create_table(
    table_name: str,
    allow_multiple_artifacts: bool,
    has_side_effects: bool,
    description: str,
    process_id: str,
    db_metadata: MetadataStore,
    db_locks: DatabaseLock,
    file_writer: CopyOnWriteFile,
) -> SETUP_OUTPUT:
    if table_name in constants.ILLEGAL_TABLE_NAMES or table_name.startswith("."):
        raise tv_errors.TVArgumentError("Forbidden Table Name: {table_name}")
    existance = db_metadata.get_table_instances(table_name, "")
    if existance is not None:
        raise tv_errors.TVArgumentError(f"{table_name} exists")
    funct_kwargs = {
        "table_name": table_name,
        "allow_multiple_artifacts": allow_multiple_artifacts,
        "has_side_effects": has_side_effects,
        "description": description,
    }
    db_metadata.update_process_data(process_id, funct_kwargs)
    db_locks.make_lock_path(table_name)
    db_locks.make_lock_path(table_name, constants.ARTIFACT_FOLDER)
    _acquire_lock("exclusive", process_id, db_locks, db_metadata, table_name)
    return funct_kwargs


def setup_restart_database(
    process_id: str,
    db_locks: DatabaseLock,
    db_metadata: MetadataStore,
) -> SETUP_OUTPUT:
    _acquire_lock(
        "exclusive", process_id, db_locks, db_metadata, constants.RESTART_LOCK
    )
    return {}


# TODO
SETUP_MAP = {
    constants.CREATE_CODE_MODULE_OP: setup_create_code_module,
    constants.DELTE_CODE_MODULE_OP: setup_delete_code_module,
    constants.CREATE_BUILDER_FILE_OP: setup_create_builder_file,
    constants.DELETE_BUILDER_FILE_OP: setup_delete_builder_file,
    constants.RENAME_TABLE_OP: setup_rename_table,
    constants.DELETE_TABLE_OP: setup_delete_table,
    constants.DELETE_INSTANCE_OP: setup_delete_instance,
    constants.MAT_OP: setup_materialize_instance,
    constants.STOP_PROCESS_OP: setup_stop_process,
    constants.WRITE_INSTANCE_OP: setup_write_instance,
    constants.WRITE_INSTANCE_INNER_OP: setup_write_instance_inner,
    constants.EXECUTE_INNER_OP: setup_execute_instance_inner,
    constants.EXECUTE_OP: setup_execute_instance,
    constants.CREATE_INSTANCE_OP: setup_create_instance,
    constants.CREATE_TABLE_OP: setup_create_table,
    constants.RESTART_OP: setup_restart_database,
}


def _parse_dependencies(
    builders: dict[str, TVBuilder],
    table_name: str,
    start_time: float,
    db_metadata: MetadataStore,
) -> tuple[types.BuilderDeps, types.InternalDeps, types.ExternalDeps, types.CodeDeps]:
    table_generator = ""
    for builder_name in builders:
        if (
            builder_name == (f"{table_name}{constants.INDEX_BUILDER_SUFFIX}")
            and table_generator == ""
        ):
            table_generator = builder_name
        elif (
            builder_name == (f"{table_name}{constants.INDEX_BUILDER_SUFFIX}")
            and table_generator != ""
        ):
            raise tv_errors.TVBuilderError(
                f"""Can only have one index builder :
                {table_name}{constants.INDEX_BUILDER_SUFFIX}"""
            )
    if table_generator == "":
        raise tv_errors.TVBuilderError(
            f"""Needs one IndexBuilder that is
            {table_name}{constants.INDEX_BUILDER_SUFFIX}"""
        )
    external_deps = {}
    internal_builder_deps = {}
    internal_deps = {}
    for builder_name in builders:
        external_deps[builder_name] = set()
        if builder_name != table_generator:
            internal_builder_deps[builder_name] = {table_generator}
            internal_deps[builder_name] = set()
        else:
            internal_deps[builder_name] = set()
            internal_builder_deps[builder_name] = set()
        for dep in builders[builder_name].dependencies:
            if dep.table == constants.TABLE_SELF:
                for bn in builders:
                    if dep.columns is not None:
                        for col in dep.columns:
                            if col in builders[bn].changed_columns:
                                internal_builder_deps[builder_name].add(bn)
                continue
            if dep.table == table_name:
                active_only = False
            else:
                active_only = True
            if dep.version is not None and "_" in dep.version:
                (
                    mat_time,
                    _,
                    _,
                    _,
                ) = db_metadata.get_table_times(dep.version, dep.table)
                external_deps[builder_name].add(
                    (dep.table, dep.columns, dep.version, mat_time, dep.version)
                )
            if dep.columns is not None:
                for col in dep.columns:
                    if col != constants.TABLE_INDEX:
                        mat_time, _, instance = db_metadata.get_last_column_update(
                            dep.table,
                            col,
                            start_time,
                            version=dep.version,
                            active_only=active_only,
                        )
                        external_deps[builder_name].add(
                            (dep.table, col, instance, mat_time, dep.version)
                        )
                    else:
                        mat_time, _, instance = db_metadata.get_last_table_update(
                            dep.table,
                            version=dep.version,
                            before_time=start_time,
                            active_only=active_only,
                        )
                        external_deps[builder_name].add(
                            (dep.table, None, instance, mat_time, dep.version)
                        )

            else:
                mat_time, _, instance = db_metadata.get_last_table_update(
                    dep.table,
                    version=dep.version,
                    before_time=start_time,
                    active_only=active_only,
                )
                external_deps[builder_name].add(
                    (dep.table, None, instance, mat_time, dep.version)
                )
        external_deps[builder_name] = list(external_deps[builder_name])
        internal_deps[builder_name] = list(internal_deps[builder_name])
        internal_builder_deps[builder_name] = list(internal_builder_deps[builder_name])
    # get code dependencies
    code_dependencies = {}
    for builder_name in builders:
        if (
            hasattr(builders[builder_name], "code_module")
            and hasattr(builders[builder_name], "is_custom")
            and hasattr(builders[builder_name], "python_function")
        ):
            if isinstance(builders[builder_name].is_custom, bool):
                if builders[builder_name].is_custom:
                    if isinstance(builders[builder_name].code_module, str):
                        if isinstance(builders[builder_name].python_function, str):
                            code_func = (
                                builders[builder_name].code_module,
                                builders[builder_name].python_function,
                            )
                        else:
                            code_func = (builders[builder_name].code_module, None)
                        code_dependencies[builder_name] = code_func
                    else:
                        code_dependencies[builder_name] = None
            else:
                code_dependencies[builder_name] = None

    return internal_builder_deps, internal_deps, external_deps, code_dependencies


def parse_builders(
    builders: dict[str, TVBuilder],
    db_metadata: MetadataStore,
    start_time: float,
    instance_id: str,
    table_name: str,
    origin_id: str,
    origin_table: str,
    file_writer: CopyOnWriteFile,
) -> tuple[
    list[str], list[str], list[str], types.InternalDeps, types.ExternalDeps, bool
]:
    internal_builder_deps, internal_deps, external_deps, code_dependencies = (
        _parse_dependencies(builders, table_name, start_time, db_metadata)
    )
    builder_names = list(builders.keys())
    top_builder_names = topological_sort(builder_names, internal_builder_deps)
    all_columns = []
    changed_columns = []
    for builder_name in top_builder_names:
        all_columns += builders[builder_name].changed_columns

    custom_code = False
    if origin_id != "":
        to_execute = []
        prev_mat_time, _, _, _ = db_metadata.get_table_times(origin_id, table_name)
        prev_builders = file_operations.get_builder_names(
            origin_id, origin_table, db_metadata.db_dir
        )

        for builder_name in top_builder_names:
            execute = False
            if builder_name in code_dependencies:
                custom_code = True
                if code_dependencies[builder_name] is None:
                    execute = True
                else:
                    code_function_eq = file_operations.check_code_function_equality(
                        code_dependencies[builder_name][1],
                        code_dependencies[builder_name][0],
                        origin_id,
                        origin_table,
                        db_metadata.db_dir,
                        file_writer,
                    )
                    if not code_function_eq:
                        execute = True
            if not execute:
                for dep in internal_builder_deps[builder_name]:
                    if dep in to_execute and dep != top_builder_names[0]:
                        execute = True
                        break
            if not execute:
                for dep in external_deps[builder_name]:
                    if dep[3] >= prev_mat_time:
                        execute = True
                        break
            if not execute:
                if builder_name not in prev_builders:
                    execute = True
                elif not file_operations.check_builder_equality(
                    builder_name,
                    instance_id,
                    table_name,
                    origin_id,
                    origin_table,
                    db_metadata.db_dir,
                ):
                    execute = True
            if execute:
                to_execute.append(builder_name)
                changed_columns += builders[builder_name].changed_columns
    else:
        for builder_name in top_builder_names:
            if builder_name in code_dependencies:
                if builder_name in code_dependencies:
                    custom_code = True
            changed_columns += builders[builder_name].changed_columns
    return (
        top_builder_names,
        changed_columns,
        all_columns,
        internal_deps,
        external_deps,
        code_dependencies,
        custom_code,
    )
