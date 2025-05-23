import os
import shutil
import pandas as pd
import json
import yaml
from tablevault.defintions.tv_errors import TVFileError
from tablevault.helper.database_lock import DatabaseLock
from tablevault.defintions import constants
from typing import Any, Optional
import filecmp


def delete_database_folder(db_dir) -> None:
    shutil.rmtree(db_dir)


def setup_database_folder(db_dir: str, description: str, replace: bool = False) -> None:
    if not replace and os.path.exists(db_dir):
        raise TVFileError(f"database path {db_dir} already taken")
    elif replace and os.path.isdir(db_dir):
        shutil.rmtree(db_dir)
    elif replace and os.path.isfile(db_dir):
        os.remove(db_dir)

    os.makedirs(db_dir)
    os.makedirs(os.path.join(db_dir, constants.CODE_FOLDER))
    os.makedirs(os.path.join(db_dir, constants.TEMP_FOLDER))
    meta_dir = os.path.join(db_dir, constants.METADATA_FOLDER)
    deletion_dir = os.path.join(meta_dir, constants.DELETION_FOLDER)
    os.makedirs(meta_dir)
    os.makedirs(deletion_dir)
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    os.makedirs(lock_dir)

    with open(os.path.join(meta_dir, constants.META_LOG_FILE), "w") as file:
        pass
    with open(os.path.join(meta_dir, constants.META_CLOG_FILE), "w") as file:
        pass

    with open(os.path.join(meta_dir, constants.META_ALOG_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_CHIST_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_THIST_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_INSTANCE_FILE), "w") as file:
        json.dump({}, file)
    with open(os.path.join(meta_dir, constants.META_TABLE_FILE), "w") as file:
        json.dump({}, file)
    db_lock = DatabaseLock("", db_dir)
    db_lock.make_lock_path(constants.RESTART_LOCK)
    db_lock.make_lock_path(constants.CODE_FOLDER)
    meta_lock = os.path.join(meta_dir, "LOG.lock")
    with open(meta_lock, "w"):
        pass
    meta_file = os.path.join(db_dir, constants.META_DESCRIPTION_FILE)
    with open(meta_file, "w") as f:
        descript_yaml = {constants.DESCRIPTION_SUMMARY: description}
        yaml.safe_dump(descript_yaml, f)


def setup_table_instance_folder(
    instance_id: str,
    table_name: str,
    db_dir: str,
    external_edit: bool,
    origin_id: str = "",
    origin_table: str = "",
    builders: list[str] = [],
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    instance_dir = os.path.join(table_dir, instance_id)
    if os.path.exists(instance_dir):
        shutil.rmtree(instance_dir)
    os.makedirs(instance_dir)

    builder_dir = os.path.join(instance_dir, constants.BUILDER_FOLDER)
    artifact_dir = os.path.join(instance_dir, constants.ARTIFACT_FOLDER)
    current_table_path = os.path.join(instance_dir, constants.TABLE_FILE)
    archive_dir = os.path.join(instance_dir, constants.ARCHIVE_FOLDER)
    os.makedirs(archive_dir)
    description_path = os.path.join(
        table_dir, instance_id, constants.META_DESCRIPTION_FILE
    )
    if not external_edit:
        if origin_id != "":
            prev_dir = os.path.join(db_dir, origin_table, str(origin_id))
            prev_builder_dir = os.path.join(prev_dir, constants.BUILDER_FOLDER)
            shutil.copytree(prev_builder_dir, builder_dir, copy_function=shutil.copy2)

            prev_artifact_dir = os.path.join(prev_dir, constants.ARTIFACT_FOLDER)
            if os.path.isdir(prev_artifact_dir):
                shutil.copytree(
                    prev_artifact_dir, artifact_dir, copy_function=shutil.copy2
                )
            else:
                os.makedirs(artifact_dir)

            prev_code_dir = os.path.join(prev_dir, constants.CODE_FOLDER)
            # if os.path.isdir(prev_code_dir):
            #     shutil.copytree(prev_code_dir, code_dir, copy_function=shutil.copy2)
            # else:
            #     os.makedirs(code_dir)
        elif len(builders) != 0:
            os.makedirs(builder_dir)
            builder_dir_ = os.path.join(table_dir, constants.BUILDER_FOLDER)
            for builder_name in builders:
                builder_path_ = os.path.join(builder_dir_, builder_name + ".yaml")
                builder_path = os.path.join(builder_dir, builder_name + ".yaml")
                shutil.copy2(builder_path_, builder_path)
            os.makedirs(artifact_dir)
            # os.makedirs(code_dir)
        else:
            os.makedirs(builder_dir)
            os.makedirs(artifact_dir)
            # os.makedirs(code_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        type_path = os.path.join(instance_dir, constants.DTYPE_FILE)
        with open(type_path, "w") as f:
            json.dump({}, f)

        with open(description_path, "w") as f:
            pass
    else:
        if origin_id != "":
            copy_table(instance_id, table_name, origin_id, origin_table, db_dir)
            prev_artifact_dir_instance = os.path.join(
                db_dir, origin_table, str(origin_id), constants.ARTIFACT_FOLDER
            )
            prev_artifact_dir_table = os.path.join(
                db_dir, origin_table, constants.ARTIFACT_FOLDER
            )
            if os.path.isdir(prev_artifact_dir_instance):
                shutil.copytree(
                    prev_artifact_dir_instance, artifact_dir, copy_function=shutil.copy2
                )
            elif os.path.isdir(prev_artifact_dir_table):
                shutil.copytree(
                    prev_artifact_dir_table, artifact_dir, copy_function=shutil.copy2
                )
            else:
                os.makedirs(artifact_dir)
        else:
            df = pd.DataFrame()
            df.to_csv(current_table_path, index=False)
            type_path = os.path.join(instance_dir, constants.DTYPE_FILE)
            with open(type_path, "w") as f:
                json.dump({}, f)
            os.makedirs(artifact_dir)
        with open(description_path, "w") as f:
            pass


def get_description(instance_id: str, table_name: str, db_dir: str) -> Any:
    meta_file = db_dir
    if table_name != "":
        meta_file = os.path.join(meta_file, table_name)
        if instance_id != "":
            meta_file = os.path.join(meta_file, instance_id)
    elif instance_id != "":
        raise TVFileError("Need table_name with instance_id")
    meta_file = os.path.join(meta_file, constants.META_DESCRIPTION_FILE)
    with open(meta_file, "r") as f:
        descript_yaml = yaml.safe_load(f)
        return descript_yaml


def write_description(
    descript_yaml: dict, instance_id: str, table_name: str, db_dir: str
):
    meta_file = db_dir
    if table_name != "":
        meta_file = os.path.join(meta_file, table_name)
        if instance_id != "":
            meta_file = os.path.join(meta_file, instance_id)
    elif instance_id != "":
        raise TVFileError("Need table_name with instance_id")
    meta_file = os.path.join(meta_file, constants.META_DESCRIPTION_FILE)
    with open(meta_file, "w") as f:
        yaml.safe_dump(descript_yaml, f)


def setup_table_folder(
    table_name: str, db_dir: str, make_artifacts: bool = True
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        raise TVFileError("table folder already exists.")
    if os.path.isfile(table_dir):
        raise TVFileError("table folder already exists as file.")
    os.makedirs(table_dir)
    builder_dir_ = os.path.join(table_dir, constants.BUILDER_FOLDER)
    os.makedirs(builder_dir_)
    if make_artifacts:
        os.makedirs(os.path.join(table_dir, constants.ARTIFACT_FOLDER))
    description_dir = os.path.join(table_dir, constants.META_DESCRIPTION_FILE)
    with open(description_dir, "w") as _:
        pass


def rename_table_instance(
    instance_id: str, prev_instance_id: str, table_name: str, db_dir: str
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, prev_instance_id)
    new_dir = os.path.join(table_dir, instance_id)
    if not os.path.exists(temp_dir) and os.path.exists(new_dir):
        return
    elif not os.path.exists(temp_dir) or os.path.exists(new_dir):
        raise TVFileError("Could Not Rename Instance")
    os.rename(temp_dir, new_dir)


def delete_table_folder_2(table_name: str, db_dir: str, instance_id: str = "") -> None:
    instance_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        instance_dir = os.path.join(instance_dir, str(instance_id))
    if os.path.exists(instance_dir):
        shutil.rmtree(instance_dir)


def delete_table_folder(table_name: str, db_dir: str, instance_id: str = "") -> None:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        instance_dir = os.path.join(table_dir, str(instance_id))
        df_dir = os.path.join(instance_dir, constants.TABLE_FILE)
        if os.path.exists(df_dir):
            os.remove(df_dir)
    else:
        for dire in os.listdir(table_dir):
            instance_dir = os.path.join(table_dir, dire)
            if os.path.isdir(instance_dir):
                df_dir = os.path.join(instance_dir, constants.TABLE_FILE)
                if os.path.exists(df_dir):
                    os.remove(df_dir)
        instance_dir = table_dir
    dest_dir = os.path.join(db_dir, constants.METADATA_FOLDER, constants.DELETION_FOLDER, table_name)
    if instance_id != "":
        dest_dir = os.path.join(dest_dir, instance_id)
    dest_dir_ = dest_dir
    i = 1
    while os.path.exists(dest_dir_):
        dest_dir_ = dest_dir  + "_" + str(i)
        i +=1
    shutil.move(instance_dir, dest_dir_)


def get_yaml_builders(
    instance_id: str, table_name: str, db_dir: str, yaml_name: str = ""
) -> dict[str, dict]:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, instance_id)
    builder_dir = os.path.join(table_dir, constants.BUILDER_FOLDER)
    if not os.path.isdir(builder_dir):
        return {}
    if yaml_name == "":
        builders = {}
        for item in os.listdir(builder_dir):
            if item.endswith(".yaml"):
                name = item.split(".")[0]
                builder_path = os.path.join(builder_dir, item)
                with open(builder_path, "r") as file:
                    builder = yaml.safe_load(file)
                    builder[constants.BUILDER_NAME] = name
                builders[name] = builder
        return builders
    else:
        builder_path = os.path.join(builder_dir, yaml_name)
        with open(builder_path, "r") as file:
            builders = {}
            name = yaml_name.split(".")[0]
            builder = yaml.safe_load(file)
            builder[constants.BUILDER_NAME] = name
            builders[name] = builder
        return builders


def save_yaml_builder(
    builder: Any, instance_id: str, table_name: str, db_dir: str
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, instance_id)
    builder_dir = os.path.join(table_dir, constants.BUILDER_FOLDER)
    yaml_name = builder[constants.BUILDER_NAME] + ".yaml"
    builder_path = os.path.join(builder_dir, yaml_name)
    with open(builder_path, "w") as file:
        yaml.safe_dump(builder, file)


def get_external_yaml_builders(external_dir: str) -> dict[str, dict[str, Any]]:
    builders = {}
    for table_name in os.listdir(external_dir):
        builder_dir = os.path.join(external_dir, table_name)
        builders[table_name] = {}
        for item in os.listdir(builder_dir):
            if item.endswith(".yaml"):
                name = item.split(".")[0]
                builder_path = os.path.join(builder_dir, item)
                with open(builder_path, "r") as file:
                    builder = yaml.safe_load(file)
                    builder[constants.BUILDER_NAME] = name
                builders[table_name][name] = builder
    return builders


def get_builder_names(instance_id: str, table_name: str, db_dir: str) -> list[str]:
    if instance_id != "":
        builder_dir = os.path.join(
            db_dir, table_name, instance_id, constants.BUILDER_FOLDER
        )
        if not os.path.isdir(builder_dir):
            return []
    else:
        builder_dir = os.path.join(db_dir, table_name, constants.BUILDER_FOLDER)
    builder_names = []
    for file in os.listdir(builder_dir):
        if file.endswith(".yaml"):
            builder_name = file.split(".")[0]
            builder_names.append(builder_name)
    return builder_names


def check_builder_equality(
    builder_name: str,
    instance_id_1: str,
    table_name_1: str,
    instance_id_2: str,
    table_name_2: str,
    db_dir: str,
) -> bool:
    builder_dir_1 = os.path.join(
        db_dir,
        table_name_1,
        instance_id_1,
        constants.BUILDER_FOLDER,
        f"{builder_name}.yaml",
    )
    builder_dir_2 = os.path.join(
        db_dir,
        table_name_2,
        instance_id_2,
        constants.BUILDER_FOLDER,
        f"{builder_name}.yaml",
    )
    with open(builder_dir_1, "r") as file:
        builder1 = yaml.safe_load(file)
        if constants.BUILDER_NAME in builder1:
            del builder1[constants.BUILDER_NAME]
    with open(builder_dir_2, "r") as file:
        builder2 = yaml.safe_load(file)
        if constants.BUILDER_NAME in builder2:
            del builder2[constants.BUILDER_NAME]
    return builder1 == builder2


def copy_files(
    file_dir: str,
    sub_folder: str,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    if sub_folder not in [constants.CODE_FOLDER, constants.BUILDER_FOLDER]:
        raise TVFileError(f"subfolder {sub_folder} not supported.")
    try:
        new_dir = db_dir
        if table_name != "":
            new_dir = os.path.join(db_dir, table_name)
        if instance_id != "":
            new_dir = os.path.join(new_dir, instance_id)
        new_dir = os.path.join(new_dir, sub_folder)
        if file_dir.endswith(".yaml") and sub_folder == constants.BUILDER_FOLDER:
            shutil.copy2(file_dir, new_dir)
        elif file_dir.endswith(".py") and sub_folder == constants.BUILDER_FOLDER:
            shutil.copy2(file_dir, new_dir)
        else:
            for f in os.listdir(file_dir):
                if f.endswith(".yaml") and sub_folder == constants.BUILDER_FOLDER:
                    p_path = os.path.join(file_dir, f)
                    shutil.copy2(p_path, new_dir)
                if f.endswith(".py") and sub_folder == constants.CODE_FOLDER:
                    p_path = os.path.join(file_dir, f)
                    shutil.copy2(p_path, new_dir)
    except Exception as e:
        raise TVFileError(f"Error copying from {file_dir}: {e}")


def move_artifacts_to_table(db_dir: str, table_name: str = "", instance_id: str = ""):
    new_artifact_dir = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if not os.path.exists(new_artifact_dir):
        return
    old_artifact_dir = os.path.join(
        db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER
    )
    new_artifact_dir = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if os.path.exists(new_artifact_dir):
        shutil.rmtree(new_artifact_dir)
    shutil.move(old_artifact_dir, new_artifact_dir)


def upload_artifact(
    artifact_name: str,
    path_name: str,
    db_dir: str,
    table_name: str,
    instance_id: str,
) -> tuple[str, str]:
    artifact_dir = os.path.join(
        db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER, artifact_name
    )
    table_dir = os.path.join(
        db_dir, table_name, constants.ARTIFACT_FOLDER, artifact_name
    )
    shutil.copy2(path_name, artifact_dir)
    return artifact_dir, table_dir


def has_artifact(instance_id: str, table_name: str, db_dir: str) -> bool:
    # TODO: deal with external edit and artifact case
    artifact_dir = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if os.path.isdir(artifact_dir):
        for name in os.listdir(artifact_dir):
            if not name.startswith("."):
                return True
    artifact_dir = os.path.join(
        db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER
    )
    if os.path.isdir(artifact_dir):
        for name in os.listdir(artifact_dir):
            if not name.startswith("."):
                return True
    return False


def copy_folder_to_temp(
    process_id: str,
    db_dir: str,
    instance_id: str = "",
    table_name: str = "",
    subfolder: str = "",
):
    folder_dir = db_dir
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER, process_id)
    if table_name != "":
        folder_dir = os.path.join(folder_dir, table_name)
        temp_dir = os.path.join(temp_dir, table_name)
    if instance_id != "":
        folder_dir = os.path.join(folder_dir, instance_id)
        temp_dir = os.path.join(temp_dir, instance_id)
    if subfolder != "":
        folder_dir = os.path.join(folder_dir, subfolder)
        temp_dir = os.path.join(temp_dir, subfolder)
    os.makedirs(temp_dir, exist_ok=True)
    shutil.copytree(folder_dir, temp_dir, dirs_exist_ok=True)


def copy_temp_to_db(
    process_id: str,
    db_dir: str,
):
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER, process_id)
    if os.path.isdir(temp_dir):
        shutil.copytree(temp_dir, db_dir, dirs_exist_ok=True)


def delete_from_temp(process_id: str, db_dir: str):
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER)
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder.startswith(process_id):
            shutil.rmtree(sub_dir)


def cleanup_temp(active_ids: list[str], db_dir: str):
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER)
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder not in active_ids:
            shutil.rmtree(sub_dir)


def copy_table(
    temp_id: str,
    table_name: str,
    prev_instance_id: str,
    prev_table_name: str,
    db_dir: str,
):
    prev_table_path = os.path.join(
        db_dir, prev_table_name, prev_instance_id, constants.TABLE_FILE
    )
    current_table_path = os.path.join(db_dir, table_name, temp_id, constants.TABLE_FILE)
    shutil.copy2(prev_table_path, current_table_path)
    prev_dtype_path = os.path.join(
        db_dir, prev_table_name, prev_instance_id, constants.DTYPE_FILE
    )
    current_dtype_path = os.path.join(db_dir, table_name, temp_id, constants.DTYPE_FILE)
    shutil.copy2(prev_dtype_path, current_dtype_path)


def load_code_function(
    python_function: str,
    module_name: str,
    db_dir: str,
    instance_id: str = "",
    table_name: str = "",
):
    if table_name != "":
        file_path_ = os.path.join(db_dir, table_name)
    if instance_id != "":
        file_path_ = os.path.join(file_path_, instance_id)

    file_path_ = os.path.join(file_path_, constants.ARCHIVE_FOLDER, module_name + ".py")
    try:
        namespace = {}
        with open(file_path_, "r") as file:
            exec(file.read(), namespace)
        if python_function in namespace:
            return namespace[python_function], namespace
        else:
            raise TVFileError(
                f"Function '{python_function}' not found in '{file_path_}'"
            )
    except Exception as e:
        raise TVFileError(
            f"Function '{python_function}' not found in '{file_path_}': {e}"
        )


def move_code_to_instance(
    module_name: str, instance_id: str, table_name: str, db_dir: str
):
    file_path = os.path.join(db_dir, constants.CODE_FOLDER, module_name + ".py")
    if not os.path.exists(file_path):
        raise TVFileError(f"Function '{module_name}' not found")
    file_path_ = os.path.join(
        db_dir, table_name, instance_id, constants.ARCHIVE_FOLDER, module_name + ".py"
    ) #TODO: edit
    shutil.copy2(file_path, file_path_)


def check_code_function_equality(
    python_function: Optional[str],
    module_name: str,
    instance_id: str,
    table_name: str,
    db_dir: str,
):
    if python_function is None:
        file_path = os.path.join(db_dir, constants.CODE_FOLDER, module_name + ".py")
        if not os.path.exists(file_path):
            raise TVFileError(f"Function '{module_name}' not found")
        file_path_ = os.path.join(
            db_dir, table_name, instance_id, constants.CODE_FOLDER, module_name + ".py"
        )
        if not os.path.exists(file_path):
            return False
        return filecmp.cmp(file_path, file_path_, shallow=False)
    else:
        origin_func, _ = load_code_function(
            python_function, module_name, db_dir, instance_id, table_name
        )
        base_func = load_code_function(python_function, module_name, db_dir)
        return origin_func.__code__ == base_func.__code__
