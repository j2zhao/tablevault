from tablevault.helper import file_operations, database_lock, utils
from tablevault.helper.metadata_store import MetadataStore, ActiveProcessDict
from tablevault._operations._meta_operations import tablevault_operation
from tablevault._operations import _table_execution
from tablevault.defintions.types import ExternalDeps
from tablevault.dataframe_helper import table_operations
from tablevault.defintions import constants
from tablevault.defintions import tv_errors
from tablevault.helper.database_lock import DatabaseLock
from tablevault._operations._takedown_operations import TAKEDOWN_MAP
from tablevault.dataframe_helper import artifact
import os
import pandas as pd
from typing import Optional, Any


def get_artifact_folder(
    instance_id: str, table_name: str, version: str, is_temp: bool, db_dir: str
):
    if instance_id == "":
        if is_temp:
            instance_id = constants.TEMP_INSTANCE + version
        else:
            db_metadata = MetadataStore(db_dir)
            _, _, instance_id = db_metadata.get_last_table_update(table_name, version)
    return artifact.get_artifact_folder(instance_id, table_name, db_dir)


def setup_database(db_dir: str, description: str, replace: bool = False) -> None:
    file_operations.setup_database_folder(db_dir, description, replace)

def active_processes(db_dir: str) -> ActiveProcessDict:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_active_processes()

def complete_process(process_id: str, db_dir: str) -> bool:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.check_written(process_id)

def get_instances(table_name: str, db_dir: str, version: str) -> list[str] | None:
    db_metadata = MetadataStore(db_dir)
    return db_metadata.get_table_instances(table_name, version)

def get_table(
    instance_id: str,
    table_name: str,
    version: str,
    db_dir: str,
    active_only: bool,
    safe_locking: bool,
    rows: Optional[int],
):

    db_metadata = MetadataStore(db_dir)
    if instance_id == "":
        _, _, instance_id = db_metadata.get_last_table_update(
            table_name, version, active_only=active_only
        )
    process_id = utils.gen_tv_id()
    db_lock = database_lock.DatabaseLock(process_id, db_dir)
    if safe_locking:
        db_lock.acquire_shared_lock(table_name, instance_id)
    try:
        df = table_operations.get_table(instance_id, table_name, db_dir, rows, artifact_dir=True, get_index=False)
    finally:
        if safe_locking:
            db_lock.release_all_locks()
    return df

def _create_code_module(module_name:str,
                  copy_dir:str,
                  db_metadata: MetadataStore):
    file_operations.create_copy_code_file(db_metadata.db_dir, module_name, copy_dir)

def create_code_module(author: str,
                 module_name:str,
                 copy_dir:str,
                 process_id: str,
                 db_dir: str):
    setup_kwargs = {"module_name": module_name, "copy_dir": copy_dir}
    return tablevault_operation(
        author,
        constants.CREATE_CODE_MODULE_OP,
        _create_code_module,
        db_dir,
        process_id,
        setup_kwargs,
    )

def _delete_code_module(module_name:str,
                  db_metadata: MetadataStore):
    file_operations.delete_code_file(module_name, db_metadata.db_dir)

def delete_code_module(author: str,
                 module_name:str,
                 process_id: str,
                 db_dir: str):
    setup_kwargs = {"module_name": module_name }
    return tablevault_operation(
        author,
        constants.DELTE_CODE_MODULE_OP,
        _delete_code_module,
        db_dir,
        process_id,
        setup_kwargs,
    )

def _create_builder_file(builder_name:str,
                         instance_id:str,
                         table_name:str,
                         copy_dir:str,
                         builder_type:str,
                           db_metadata: MetadataStore
                           ):
    file_operations.create_copy_builder_file(instance_id, table_name, db_metadata.db_dir, 
                                             builder_name, copy_dir, builder_type)
def create_builder_file(author: str,
                 builder_name:str,
                 table_name:str,
                 version: str,
                 copy_dir:str,
                 builder_type:str,
                 process_id: str,
                 db_dir: str):
    setup_kwargs = {"builder_name": builder_name, "table_name": table_name,
                     "version":version, "copy_dir": copy_dir,
                     "builder_type": builder_type}
    return tablevault_operation(
        author,
        constants.CREATE_BUILDER_FILE_OP,
        _create_builder_file,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _delete_builder_file(builder_name:str,
                         instance_id:str,
                         table_name:str,
                  db_metadata: MetadataStore):
    file_operations.delete_builder_file(builder_name, instance_id, table_name, db_metadata.db_dir)

def delete_builder_file(author: str,
                 builder_name:str,
                 table_name:str, 
                 version: str,
                 process_id: str,
                 db_dir: str):
    setup_kwargs = {"builder_name": builder_name, "table_name": table_name,
                     "version":version}
    return tablevault_operation(
        author,
        constants.DELETE_BUILDER_FILE_OP,
        _delete_builder_file,
        db_dir,
        process_id,
        setup_kwargs,
    )

def _rename_table(new_table_name:str,
                  table_name:str,
                  db_metadata: MetadataStore):
    file_operations.rename_table(new_table_name, table_name,db_metadata.db_dir)

def rename_table(author: str,
                 new_table_name:str,
                 table_name:str,
                 process_id: str,
                 db_dir: str):
    setup_kwargs = {"new_table_name": new_table_name, "table_name": table_name, }
    return tablevault_operation(
        author,
        constants.RENAME_TABLE_OP,
        _rename_table,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _delete_table(table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir)

def delete_table(author: str, table_name: str, process_id: str, db_dir: str):
    setup_kwargs = {"table_name": table_name}
    return tablevault_operation(
        author,
        constants.DELETE_TABLE_OP,
        _delete_table,
        db_dir,
        process_id,
        setup_kwargs,
    )



def _delete_instance(instance_id: str, table_name: str, db_metadata: MetadataStore):
    file_operations.delete_table_folder(table_name, db_metadata.db_dir, instance_id)

def delete_instance(
    author: str, table_name: str, instance_id: str, process_id: str, db_dir: str
):
    setup_kwargs = {"table_name": table_name, "instance_id": instance_id}
    return tablevault_operation(
        author,
        constants.DELETE_INSTANCE_OP,
        _delete_instance,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _materialize_instance(
    instance_id: str,
    table_name: str,
    perm_instance_id: str,
    origin_id: str,
    origin_table: str,
    dtypes: dict[str, str],
    success: bool,
    dependencies: list[tuple[str, str]],
    db_metadata: MetadataStore,
):  
    table_data = file_operations.get_description("", table_name, db_metadata.db_dir)
    table_operations.update_dtypes(
            dtypes, instance_id, table_name, db_metadata.db_dir
        )

    if success:

        table_operations.check_table(instance_id, table_name, db_metadata.db_dir)

    if not table_data[constants.TABLE_ALLOW_MARTIFACT] and success:
        file_operations.move_artifacts_to_table(
            db_metadata.db_dir, table_name, instance_id
        )

    file_operations.rename_table_instance(
        perm_instance_id, instance_id, table_name, db_metadata.db_dir
    )

    instance_descript = file_operations.get_description(
        perm_instance_id, table_name, db_metadata.db_dir
    )
    instance_descript[constants.DESCRIPTION_DEPENDENCIES] = dependencies
    instance_descript[constants.DESCRIPTION_SUCCESS] = success
    file_operations.write_description(
        instance_descript, perm_instance_id, table_name, db_metadata.db_dir
    )

    if origin_id != "":
        parent_descript = file_operations.get_description(
            origin_id, origin_table, db_metadata.db_dir
        )
        if constants.DESCRIPTION_FUTURE not in parent_descript:
            parent_descript[constants.DESCRIPTION_FUTURE] = []
        recorded = False
        for id, name in parent_descript[constants.DESCRIPTION_FUTURE]:
            if id == perm_instance_id or name == table_name:
                recorded = True
        if not recorded:
            parent_descript[constants.DESCRIPTION_FUTURE].append(
                [perm_instance_id, table_name]
            )
            file_operations.write_description(
                parent_descript, origin_id, origin_table, db_metadata.db_dir
            )

    for dep_table, dep_instance in dependencies:
        parent_descript = file_operations.get_description(
            dep_instance, dep_table, db_metadata.db_dir
        )
        if constants.DESCRIPTION_CHILDREN not in parent_descript:
            parent_descript[constants.DESCRIPTION_CHILDREN] = []
        recorded = False
        for id, name in parent_descript[constants.DESCRIPTION_CHILDREN]:
            if id == perm_instance_id or name == table_name:
                recorded = True
        if not recorded:
            parent_descript[constants.DESCRIPTION_CHILDREN].append(
                [perm_instance_id, table_name]
            )
            file_operations.write_description(
                parent_descript, dep_instance, dep_table, db_metadata.db_dir
            )

def materialize_instance(
    author: str,
    instance_id: str,
    table_name: str,
    version: str,
    perm_instance_id: str,
    origin_id: str,
    origin_table: str,
    dtypes: dict[str, str],
    changed_columns: list[str],
    all_columns: list[str],
    dependencies: list[tuple[str, str]],
    process_id: str,
    db_dir: str,
    success: bool = True,
):
    setup_kwargs = {
        "table_name": table_name,
        "instance_id": instance_id,
        "version": version,
        "perm_instance_id": perm_instance_id,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "dtypes": dtypes,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "dependencies": dependencies,
        "success": success,
    }
    return tablevault_operation(
        author,
        constants.MAT_OP,
        _materialize_instance,
        db_dir,
        process_id,
        setup_kwargs,
    )

def _stop_process(
    process_ids: list[str],
    materialize_args: dict[str, Any],
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
):
    logs = db_metadata.get_active_processes()
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        error = (tv_errors.TVForcedError.__class__.__name__, "User Stopped")
        for process_id_ in process_ids:
            if process_id_ not in logs:
                continue
            if logs[process_id_].start_success is None:
                db_metadata.update_process_start_status(process_id_, False, error)
            elif logs[process_id_].execution_success is None:
                db_metadata.update_process_execution_status(process_id_, False, error)
            db_locks = DatabaseLock(process_id_, db_metadata.db_dir)
            TAKEDOWN_MAP[logs[process_id_].operation](
                process_id_, db_metadata, db_locks
            )
            db_metadata.write_process(process_id_)
        db_metadata.update_process_step(process_id, step_ids[0])
    if len(step_ids) > 1:
        try:
            materialize_instance(
                process_id,
                materialize_args["instance_id"],
                materialize_args["table_name"],
                "",
                materialize_args["perm_instance_id"],
                materialize_args["origin_id"],
                materialize_args["origin_table"],
                materialize_args["dtypes"],
                materialize_args["changed_columns"],
                materialize_args["all_columns"],
                materialize_args["dependencies"],
                step_ids[1],
                db_metadata.db_dir,
            )
        finally:
            db_metadata.update_process_step(process_id, step_ids[1])

def stop_process(
    author: str,
    to_stop_process_id: str,
    force: bool,
    materialize: bool,
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "to_stop_process_id": to_stop_process_id,
        "materialize": materialize,
        "force": force,
    }
    return tablevault_operation(
        author,
        constants.STOP_PROCESS_OP,
        _stop_process,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _write_table_inner(
    table_df: Optional[pd.DataFrame],
    instance_id: str,
    table_name: str,
    db_metadata: MetadataStore,
):
    if table_df is None:
        raise tv_errors.TVProcessError("Cannot Restart Write Table")
    table_operations.write_table(table_df, instance_id, table_name, db_metadata.db_dir)
    table_operations.write_dtype(
        table_df.dtypes, instance_id, table_name, db_metadata.db_dir
    )

def write_table_inner(
    author: str,
    table_df: Optional[pd.DataFrame],
    instance_id: str,
    table_name: str,
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "table_df": table_df,
        "instance_id": instance_id,
        "table_name": table_name,
    }
    return tablevault_operation(
        author,
        constants.WRITE_TABLE_INNER_OP,
        _write_table_inner,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _write_table(
    table_df: Optional[pd.DataFrame],
    instance_id: str,
    table_name: str,
    perm_instance_id: str,
    dtypes: dict[str, str],
    all_columns: list[str],
    changed_columns: list[str],
    dependencies: list[tuple[str, str]],
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        write_table_inner(
            process_id,
            table_df,
            instance_id,
            table_name,
            step_ids[0],
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[0])

    if step_ids[1] not in complete_steps:
        materialize_instance(
            process_id,
            instance_id,
            table_name,
            "",
            perm_instance_id,
            "",
            "",
            dtypes,
            all_columns,
            changed_columns,
            dependencies,
            step_ids[1],
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[1])

def write_table(
    author: str,
    table_df: Optional[pd.DataFrame],
    table_name: str,
    version: str,
    dtypes: dict[str, str],
    dependencies: list[tuple[str, str]],
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "table_df": table_df,
        "table_name": table_name,
        "version": version,
        "dtypes": dtypes,
        "dependencies": dependencies,
    }
    return tablevault_operation(
        author, constants.WRITE_TABLE_OP, _write_table, db_dir, process_id, setup_kwargs
    )


def _execute_instance_inner(
    instance_id: str,
    table_name: str,
    top_builder_names: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id: str,
    origin_table: str,
    process_id: str,
    db_metadata: MetadataStore,
):
    _table_execution.execute_instance(
        table_name,
        instance_id,
        top_builder_names,
        changed_columns,
        all_columns,
        external_deps,
        origin_id,
        origin_table,
        process_id,
        db_metadata,
    )
    #

def execute_instance_inner(
    author: str,
    instance_id: str,
    table_name: str,
    top_builder_names: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id: str,
    origin_table: str,
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "instance_id": instance_id,
        "table_name": table_name,
        "top_builder_names": top_builder_names,
        "changed_columns": changed_columns,
        "all_columns": all_columns,
        "external_deps": external_deps,
        "origin_id": origin_id,
        "origin_table": origin_table,
    }
    return tablevault_operation(
        author,
        constants.EXECUTE_INNER_OP,
        _execute_instance_inner,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _execute_instance(
    instance_id: str,
    perm_instance_id: str,
    table_name: str,
    top_builder_names: list[str],
    changed_columns: list[str],
    all_columns: list[str],
    external_deps: ExternalDeps,
    origin_id: str,
    origin_table: str,
    step_ids: list[str],
    process_id: str,
    db_metadata: MetadataStore,
):
    complete_steps = db_metadata.get_active_processes()[process_id].complete_steps
    if step_ids[0] not in complete_steps:
        execute_instance_inner(
            process_id,
            instance_id,
            table_name,
            top_builder_names,
            changed_columns,
            all_columns,
            external_deps,
            origin_id,
            origin_table,
            step_ids[0],
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[0])
    if step_ids[1] not in complete_steps:
        dependencies = []
        for builder_name in external_deps:
            for tname, _, id, _, _ in external_deps[builder_name]:
                dependencies.append([tname, id])

        materialize_instance(
            process_id,
            instance_id,
            table_name,
            "",
            perm_instance_id,
            origin_id,
            origin_table,
            [],
            changed_columns,
            all_columns,
            dependencies,
            step_ids[1],
            db_metadata.db_dir,
        )
        db_metadata.update_process_step(process_id, step_ids[1])

def execute_instance(
    author: str,
    table_name: str,
    version: str,
    force_restart: bool,
    force_execute: bool,
    process_id: str,
    db_dir: str,
    background: bool,
):

    if force_restart and process_id == "":
        instance_id = constants.TEMP_INSTANCE + version
        db_metadata = MetadataStore(db_dir)
        active_procceses = db_metadata.get_active_processes()
        for id in active_procceses:
            if (
                id != process_id
                and active_procceses[id].operation == constants.EXECUTE_OP
            ):  
                if (
                    "table_name" in active_procceses[id].data
                    and "instance_id" in active_procceses[id].data
                ):  
                    if (
                        active_procceses[id].data["table_name"] == table_name
                        and active_procceses[id].data["instance_id"] == instance_id
                    ):
                        stop_process(
                            author,
                            id,
                            force=False,
                            materialize=False,
                            process_id="",
                            db_dir=db_dir,
                        )

    setup_kwargs = {
        "table_name": table_name,
        "version": version,
        "force_execute": force_execute,
    }
    return tablevault_operation(
        author,
        constants.EXECUTE_OP,
        _execute_instance,
        db_dir,
        process_id,
        setup_kwargs,
        background,
    )


def _create_instance(
    instance_id: str,
    table_name: str,
    origin_id: str,
    origin_table: str,
    external_edit: bool,
    builder_names: dict[str, str],
    description: str,
    db_metadata: MetadataStore,
):
    file_operations.setup_table_instance_folder(
        instance_id,
        table_name,
        db_metadata.db_dir,
        external_edit,
        origin_id,
        origin_table,
    )
    for bn, bt in builder_names.items():
        if bt.endswith(".yaml"):
            file_operations.create_copy_builder_file(instance_id, table_name, db_metadata.db_dir,
                                                     builder_name=bn, copy_dir=bt)
        else:
            file_operations.create_copy_builder_file(instance_id, table_name, db_metadata.db_dir,
                                                     builder_name=bn, builder_type=bt)
    descript_yaml = {
        constants.DESCRIPTION_SUMMARY: description,
        constants.DESCRIPTION_ORIGIN: [origin_id, origin_table],
        constants.DESCRIPTION_EDIT: external_edit,
    }
    file_operations.write_description(
        descript_yaml, instance_id, table_name, db_metadata.db_dir
    )


def create_instance(
    author: str,
    table_name: str,
    version: str,
    description: str,
    origin_id: str,
    origin_table: str,
    external_edit: bool,
    copy: bool,
    builder_names: list[str] | dict[str, str],
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "table_name": table_name,
        "version": version,
        "description": description,
        "origin_id": origin_id,
        "origin_table": origin_table,
        "external_edit": external_edit,
        "copy": copy,
        "builder_names": builder_names,
    }
    return tablevault_operation(
        author,
        constants.CREATE_INSTANCE_OP,
        _create_instance,
        db_dir,
        process_id,
        setup_kwargs,
    )


def _create_table(
    table_name: str,
    description: str,
    allow_multiple_artifacts: bool,
    has_side_effects: bool,
    db_metadata: MetadataStore,
):
    make_artifacts = not allow_multiple_artifacts
    file_operations.setup_table_folder(table_name, db_metadata.db_dir, make_artifacts)
    descript_yaml = {
        constants.DESCRIPTION_SUMMARY: description,
        constants.TABLE_ALLOW_MARTIFACT: allow_multiple_artifacts,
        constants.TABLE_SIDE_EFFECTS: has_side_effects,
    }
    file_operations.write_description(
        descript_yaml=descript_yaml,
        instance_id="",
        table_name=table_name,
        db_dir=db_metadata.db_dir,
    )

def create_table(
    author: str,
    table_name: str,
    allow_multiple_artifacts: bool,
    has_side_effects: bool,
    description: str,
    yaml_dir: str,
    process_id: str,
    db_dir: str,
):
    setup_kwargs = {
        "table_name": table_name,
        "allow_multiple_artifacts": allow_multiple_artifacts,
        "has_side_effects": has_side_effects,
        "yaml_dir": yaml_dir,
        "description": description,
    }
    return tablevault_operation(
        author, constants.CREATE_TABLE_OP, _create_table, db_dir, process_id, setup_kwargs
    )


def _restart_database(
    process_id: str,
    db_metadata: MetadataStore,
):
    active_processes = db_metadata.get_active_processes()
    for prid in active_processes:
        if active_processes[prid].operation == constants.STOP_PROCESS_OP:
            stop_process(
                author=process_id,
                to_stop_process_id="",
                force=False,
                materialize=False,
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )

    active_processes = db_metadata.get_active_processes()
    for prid in active_processes:
        if active_processes[prid].start_success is None:
            error = ("TVProcessError", "Restart Failure")
            db_metadata.update_process_start_status(prid, False, error)
    for prid in active_processes:
        if "_" in prid:
            continue
        if active_processes[prid].operation == constants.CREATE_CODE_MODULE_OP:
            create_code_module(
                author=process_id,
                module_name="",
                copy_dir="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        if active_processes[prid].operation == constants.DELTE_CODE_MODULE_OP:
            delete_code_module(
                author=process_id,
                module_name="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        if active_processes[prid].operation == constants.CREATE_BUILDER_FILE_OP:
            create_builder_file(
                author=process_id,
                builder_name="",
                table_name="",
                version="",
                copy_dir="",
                builder_type="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        if active_processes[prid].operation == constants.DELETE_BUILDER_FILE_OP:
            delete_builder_file(
                author=process_id,
                builder_name="",
                table_name="",
                version="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.RENAME_TABLE_OP:
            delete_table(
                author=process_id,
                new_table_name="",
                table_name="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.DELETE_TABLE_OP:
            delete_table(
                author=process_id,
                table_name="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.DELETE_INSTANCE_OP:
            delete_instance(
                author=process_id,
                table_name="",
                instance_id="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.MAT_OP:
            materialize_instance(
                author=process_id,
                instance_id="",
                table_name="",
                version="",
                perm_instance_id="",
                origin_id="",
                origin_table="",
                dtypes={},
                changed_columns=[],
                all_columns=[],
                dependencies=[],
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.WRITE_TABLE_OP:
            write_table(
                author=process_id,
                table_df=None,
                table_name="",
                version="",
                dtypes={},
                dependencies=[],
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.EXECUTE_OP:
            execute_instance(
                author=process_id,
                table_name="",
                version="",
                force_restart=False,
                force_execute=False,
                process_id=prid,
                db_dir=db_metadata.db_dir,
                background=False,
            )
        elif active_processes[prid].operation == constants.CREATE_INSTANCE_OP:
            create_instance(
                author=process_id,
                table_name="",
                version="",
                description="",
                origin_id="",
                origin_table="",
                external_edit=False,
                copy=False,
                builder_names=[],
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )
        elif active_processes[prid].operation == constants.CREATE_TABLE_OP:
            create_table(
                author=process_id,
                table_name="",
                allow_multiple_artifacts=False,
                has_side_effects=False,
                description="",
                yaml_dir="",
                process_id=prid,
                db_dir=db_metadata.db_dir,
            )

        db_metadata.update_process_step(process_id, step=prid)

def restart_database(
    author: str,
    process_id: str,
    db_dir: str,
):
    return tablevault_operation(
        author,
        constants.RESTART_OP,
        _restart_database,
        db_dir,
        process_id,
        setup_kwargs={},
    )
