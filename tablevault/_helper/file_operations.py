import os
import shutil
import pandas as pd
import json
import yaml
from tablevault._defintions.tv_errors import TVFileError
from tablevault._helper.database_lock import DatabaseLock
from tablevault._defintions import constants
from typing import Any, Optional
import filecmp
from importlib import resources
from tablevault._builders.examples.mapping import BUILDER_EXAMPLE_MAPPING
from tablevault._helper import user_lock
from tablevault._builders import builder_constants
from tablevault._helper.copy_write_file import CopyOnWriteFile
from rich.tree import Tree


def delete_database_folder(db_dir) -> None:
    shutil.rmtree(db_dir)


def setup_database_folder(db_dir: str, description: str, replace: bool = False) -> None:
    if not replace and os.path.exists(db_dir):
        raise TVFileError(f"database path {db_dir} already taken")
    elif replace and os.path.isdir(db_dir):
        user_lock.set_writable(db_dir)
        shutil.rmtree(db_dir)
    elif replace and os.path.isfile(db_dir):
        user_lock.set_writable(db_dir)
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

    with open(os.path.join(meta_dir, constants.META_TEMP_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_ALOG_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_CHIST_FILE), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, constants.META_THIST_FILE), "w") as file:
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
    user_lock.set_tv_lock("", "", db_dir)

    with open(os.path.join(db_dir, constants.TABLEVAULT_IDENTIFIER), "w") as file:
        pass


def setup_table_instance_folder(
    instance_id: str,
    table_name: str,
    db_dir: str,
    external_edit: bool,
    origin_id: str = "",
    origin_table: str = "",
) -> None:
    filewriter = CopyOnWriteFile(db_dir)

    table_dir = os.path.join(db_dir, table_name)
    instance_dir = os.path.join(table_dir, instance_id)
    if os.path.exists(instance_dir):
        filewriter.rmtree(instance_dir)
    filewriter.makedirs(instance_dir)

    builder_dir = os.path.join(instance_dir, constants.BUILDER_FOLDER)
    artifact_dir = os.path.join(instance_dir, constants.ARTIFACT_FOLDER)
    current_table_path = os.path.join(instance_dir, constants.TABLE_FILE)
    archive_dir = os.path.join(instance_dir, constants.ARCHIVE_FOLDER)
    filewriter.makedirs(archive_dir)
    description_path = os.path.join(
        table_dir, instance_id, constants.META_DESCRIPTION_FILE
    )
    if not external_edit:
        if origin_id != "":
            prev_dir = os.path.join(db_dir, origin_table, str(origin_id))
            prev_builder_dir = os.path.join(prev_dir, constants.BUILDER_FOLDER)
            filewriter.copytree(prev_builder_dir, builder_dir)
            # prev_artifact_dir = os.path.join(prev_dir, constants.ARTIFACT_FOLDER)
            # if os.path.isdir(prev_artifact_dir):
            #     filewriter.copytree(
            #         prev_artifact_dir, artifact_dir)
            # else:
            #     filewriter.makedirs(artifact_dir)
            filewriter.makedirs(artifact_dir)
        else:
            filewriter.makedirs(builder_dir)
            filewriter.makedirs(artifact_dir)
        df = pd.DataFrame()
        filewriter.write_csv(current_table_path, df)
        type_path = os.path.join(instance_dir, constants.DTYPE_FILE)
        with filewriter.open(type_path, "w") as f:
            json.dump({}, f)

        with filewriter.open(description_path, "w") as f:
            pass
    else:
        if origin_id != "":
            copy_table(instance_id, table_name, origin_id, origin_table, db_dir)
            # prev_artifact_dir_instance = os.path.join(
            #     db_dir, origin_table, str(origin_id), constants.ARTIFACT_FOLDER
            # )
            # prev_artifact_dir_table = os.path.join(
            #     db_dir, origin_table, constants.ARTIFACT_FOLDER
            # )
            # if os.path.isdir(prev_artifact_dir_instance):
            #     filewriter.copytree(prev_artifact_dir_instance, artifact_dir)
            # elif os.path.isdir(prev_artifact_dir_table):
            #     filewriter.copytree(prev_artifact_dir_table, artifact_dir)
            # else:
            filewriter.makedirs(artifact_dir)
        else:
            df = pd.DataFrame()
            filewriter.write_csv(current_table_path, df)
            type_path = os.path.join(instance_dir, constants.DTYPE_FILE)
            with filewriter.open(type_path, "w") as f:
                json.dump({}, f)
            filewriter.makedirs(artifact_dir)
        with filewriter.open(description_path, "w") as f:
            pass
    user_lock.set_tv_lock(instance_id, table_name, db_dir)


def get_description(instance_id: str, table_name: str, db_dir: str) -> Any:
    filewriter = CopyOnWriteFile(db_dir)
    meta_file = db_dir
    if table_name != "":
        meta_file = os.path.join(meta_file, table_name)
        if instance_id != "":
            meta_file = os.path.join(meta_file, instance_id)
    elif instance_id != "":
        raise TVFileError("Need table_name with instance_id")
    meta_file = os.path.join(meta_file, constants.META_DESCRIPTION_FILE)
    with filewriter.open(meta_file, "r") as f:
        descript_yaml = yaml.safe_load(f)
        return descript_yaml


def write_description(
    descript_yaml: dict, instance_id: str, table_name: str, db_dir: str
):
    filewriter = CopyOnWriteFile(db_dir)
    meta_file = db_dir
    if table_name != "":
        meta_file = os.path.join(meta_file, table_name)
        if instance_id != "":
            meta_file = os.path.join(meta_file, instance_id)
    elif instance_id != "":
        raise TVFileError("Need table_name with instance_id")
    meta_file = os.path.join(meta_file, constants.META_DESCRIPTION_FILE)
    with filewriter.open(meta_file, "w") as f:
        yaml.safe_dump(descript_yaml, f)


def setup_table_folder(
    table_name: str, db_dir: str, make_artifacts: bool = True
) -> None:
    filewriter = CopyOnWriteFile(db_dir)
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        raise TVFileError("table folder already exists.")
    if os.path.isfile(table_dir):
        raise TVFileError("table folder already exists as file.")
    filewriter.makedirs(table_dir)
    if make_artifacts:
        filewriter.makedirs(os.path.join(table_dir, constants.ARTIFACT_FOLDER))
    description_dir = os.path.join(table_dir, constants.META_DESCRIPTION_FILE)
    with filewriter.open(description_dir, "w") as _:
        pass


def rename_table_instance(
    instance_id: str, prev_instance_id: str, table_name: str, db_dir: str
) -> None:
    filewriter = CopyOnWriteFile(db_dir)
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, prev_instance_id)
    new_dir = os.path.join(table_dir, instance_id)
    if not os.path.exists(temp_dir) and os.path.exists(new_dir):
        return
    elif not os.path.exists(temp_dir) or os.path.exists(new_dir):
        raise TVFileError("Could Not Rename Instance")
    filewriter.rename(temp_dir, new_dir)


def rename_table(new_table_name: str, table_name: str, db_dir: str) -> None:
    filewriter = CopyOnWriteFile(db_dir)
    new_dir = os.path.join(db_dir, new_table_name)
    old_dir = os.path.join(db_dir, table_name)
    if os.path.exists(new_dir) or not os.path.exists(old_dir):
        raise TVFileError("Could Not Rename Table")
    filewriter.rename(old_dir, new_dir)


def delete_table_folder_2(table_name: str, db_dir: str, instance_id: str = "") -> None:
    filewriter = CopyOnWriteFile(db_dir)
    instance_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        instance_dir = os.path.join(instance_dir, str(instance_id))
    if os.path.exists(instance_dir):
        filewriter.rmtree(instance_dir)


def delete_table_folder(table_name: str, db_dir: str, instance_id: str = "") -> None:
    filewriter = CopyOnWriteFile(db_dir)
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        instance_dir = os.path.join(table_dir, str(instance_id))
        df_dir = os.path.join(instance_dir, constants.TABLE_FILE)
        if os.path.exists(df_dir):
            filewriter.remove(df_dir)
    else:
        for dire in os.listdir(table_dir):
            instance_dir = os.path.join(table_dir, dire)
            if os.path.isdir(instance_dir):
                df_dir = os.path.join(instance_dir, constants.TABLE_FILE)
                if os.path.exists(df_dir):
                    filewriter.remove(df_dir)
        instance_dir = table_dir
    dest_dir = os.path.join(
        db_dir, constants.METADATA_FOLDER, constants.DELETION_FOLDER, table_name
    )
    if instance_id != "":
        dest_dir = os.path.join(dest_dir, instance_id)
    dest_dir_ = dest_dir
    i = 1
    while os.path.exists(dest_dir_):
        dest_dir_ = dest_dir + "_" + str(i)
        i += 1
    filewriter.move(instance_dir, dest_dir_)


def get_yaml_builders(
    instance_id: str, table_name: str, db_dir: str, yaml_name: str = ""
) -> dict[str, dict] | dict:
    filewriter = CopyOnWriteFile(db_dir)
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
                with filewriter.open(builder_path, "r") as file:
                    builder = yaml.safe_load(file)
                    builder[constants.BUILDER_NAME] = name
                builders[name] = builder
        return builders
    else:
        builder_path = os.path.join(builder_dir, f"{yaml_name}.yaml")
        with filewriter.open(builder_path, "r") as file:
            builder = yaml.safe_load(file)
            builder[constants.BUILDER_NAME] = name
        return builder


def save_yaml_builder(
    builder: Any, instance_id: str, table_name: str, db_dir: str
) -> None:
    filewriter = CopyOnWriteFile(db_dir)
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, instance_id)
    builder_dir = os.path.join(table_dir, constants.BUILDER_FOLDER)
    yaml_name = builder[constants.BUILDER_NAME] + ".yaml"
    builder_path = os.path.join(builder_dir, yaml_name)
    with filewriter.open(builder_path, "w") as file:
        yaml.safe_dump(builder, file)


def get_builder_names(instance_id: str, table_name: str, db_dir: str) -> list[str]:
    builder_dir = os.path.join(
        db_dir, table_name, instance_id, constants.BUILDER_FOLDER
    )
    if not os.path.isdir(builder_dir):
        return []
    builder_names = []
    for file in os.listdir(builder_dir):
        if file.endswith(".yaml"):
            builder_name = file.split(".")[0]
            builder_names.append(builder_name)
    return builder_names


def get_builder_str(
    builder_name: str, instance_id: str, table_name: str, db_dir: str
) -> str:
    builder_path = os.path.join(
        db_dir,
        table_name,
        instance_id,
        constants.BUILDER_FOLDER,
        f"{builder_name}.yaml",
    )
    filewriter = CopyOnWriteFile(db_dir)
    with filewriter.open(builder_path, "r") as f:
        return f.read()


def get_code_module_names(db_dir: str) -> list[str]:
    module_dir = os.path.join(db_dir, constants.CODE_FOLDER)
    module_names = []
    for file in os.listdir(module_dir):
        if file.endswith(".py"):
            module_name = file.split(".")[0]
            module_names.append(module_name)
    return module_names


def get_code_module_str(module_name: str, db_dir: str) -> str:
    module_path = os.path.join(db_dir, constants.CODE_FOLDER, f"{module_name}.py")
    filewriter = CopyOnWriteFile(db_dir)
    with filewriter.open(module_path, "r") as f:
        return f.read()


def check_builder_equality(
    builder_name: str,
    instance_id_1: str,
    table_name_1: str,
    instance_id_2: str,
    table_name_2: str,
    db_dir: str,
) -> bool:
    filewriter = CopyOnWriteFile(db_dir)
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
    with filewriter.open(builder_dir_1, "r") as file:
        builder1 = yaml.safe_load(file)
        if constants.BUILDER_NAME in builder1:
            del builder1[constants.BUILDER_NAME]
    with filewriter.open(builder_dir_2, "r") as file:
        builder2 = yaml.safe_load(file)
        if constants.BUILDER_NAME in builder2:
            del builder2[constants.BUILDER_NAME]
    return builder1 == builder2


def create_copy_code_file(
    db_dir: str, module_name: str = "", copy_dir: str = "", text: str = ""
):
    filewriter = CopyOnWriteFile(db_dir)
    code_dir = os.path.join(db_dir, constants.CODE_FOLDER)
    if copy_dir != "":
        if os.path.isdir(copy_dir):
            for f in os.listdir(copy_dir):
                if f.endswith(".py"):
                    file_path = os.path.join(copy_dir, f)
                    if os.path.exists(file_path):
                        filewriter.remove(file_path)
                    try:
                        filewriter.copy2(file_path, code_dir)
                    except Exception as e:
                        raise TVFileError(str(e))
        elif os.path.exists(copy_dir) and copy_dir.endswith(".py"):
            if module_name != "":
                code_dir = os.path.join(code_dir, f"{module_name}.py")
            else:
                file_name = os.path.basename(copy_dir)
                code_dir = os.path.join(code_dir, file_name)
            try:
                if os.path.exists(code_dir):
                    filewriter.remove(code_dir)
                filewriter.copy2(copy_dir, code_dir)
            except Exception as e:
                raise TVFileError(str(e))
        else:
            raise TVFileError("could not copy file path")
    else:
        code_path = os.path.join(code_dir, f"{module_name}.py")
        if text == "":
            data = resources.read_binary("tablevault._helper.examples", "example.py")
            try:
                with filewriter.open(code_path, "wb") as f:
                    f.write(data)
            except Exception as e:
                raise TVFileError(f"could not create code file: {e}")
        else:
            try:
                with filewriter.open(code_path, "w") as f:
                    f.write(text)
            except Exception as e:
                raise TVFileError(f"could not create code file: {e}")


def delete_code_file(module_name: str, db_dir: str):
    filewriter = CopyOnWriteFile(db_dir)
    file_path = os.path.join(db_dir, constants.CODE_FOLDER, f"{module_name}.py")
    if not os.path.exists(file_path):
        raise TVFileError("code file doesn't exist")
    else:
        filewriter.remove(file_path)


def delete_builder_file(
    builder_name: str, instance_id: str, table_name: str, db_dir: str
):
    filewriter = CopyOnWriteFile(db_dir)
    file_path = os.path.join(
        db_dir,
        table_name,
        instance_id,
        constants.BUILDER_FOLDER,
        f"{builder_name}.yaml",
    )
    if not os.path.exists(file_path):
        raise TVFileError("code file doesn't exist")
    else:
        filewriter.remove(file_path)


def create_copy_builder_file(
    instance_id: str,
    table_name: str,
    db_dir: str,
    builder_name: str = "",
    copy_dir: str = "",
    text: str = "",
):
    filewriter = CopyOnWriteFile(db_dir)
    builder_dir = os.path.join(
        db_dir, table_name, instance_id, constants.BUILDER_FOLDER
    )
    if copy_dir != "":
        if os.path.isdir(copy_dir):
            for file_name in os.listdir(copy_dir):
                if file_name.endswith(".yaml"):
                    file_path = os.path.join(copy_dir, file_name)
                    try:
                        builder_path = os.path.join(builder_dir, file_name)
                        if os.path.exists(builder_path):
                            filewriter.remove(builder_path)
                        filewriter.copy2(file_path, builder_path)
                    except Exception as e:
                        raise TVFileError(str(e))
        elif os.path.exists(copy_dir) and copy_dir.endswith(".yaml"):
            if builder_name != "":
                builder_path = os.path.join(builder_dir, f"{builder_name}.yaml")
            else:
                builder_name = os.path.basename(copy_dir)
                builder_path = os.path.join(builder_dir, builder_name)
            if os.path.exists(builder_path):
                filewriter.remove(builder_path)
            try:
                filewriter.copy2(copy_dir, builder_path)
            except Exception as e:
                raise TVFileError(str(e))
        else:
            raise TVFileError("could not copy builder path")
    else:
        index_name = table_name + constants.INDEX_BUILDER_SUFFIX
        if builder_name == index_name:
            example_builder = BUILDER_EXAMPLE_MAPPING[builder_constants.INDEX_BUILDER]
        elif builder_name.endswith(constants.INDEX_BUILDER_SUFFIX):
            raise TVFileError(
                f"Only index builder name {index_name} allowed with index suffix. Instead of {builder_name}"
            )
        else:
            example_builder = BUILDER_EXAMPLE_MAPPING[builder_constants.COLUMN_BUILDER]
        builder_path = os.path.join(builder_dir, f"{builder_name}.yaml")
        if text == "":
            data = resources.read_binary(example_builder[0], example_builder[1])
            try:
                with filewriter.open(builder_path, "wb") as f:
                    f.write(data)
            except Exception as e:
                raise TVFileError(f"could not create builder file: {e}")
        else:
            try:
                with filewriter.open(builder_path, "w") as f:
                    f.write(text)
            except Exception as e:
                raise TVFileError(f"could not create builder file: {e}")


def move_artifacts_to_table(db_dir: str, table_name: str = "", instance_id: str = ""):
    filewriter = CopyOnWriteFile(db_dir)
    new_artifact_dir = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if not os.path.exists(new_artifact_dir):
        return
    old_artifact_dir = os.path.join(
        db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER
    )
    new_artifact_dir = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if os.path.exists(new_artifact_dir):
        filewriter.rmtree(new_artifact_dir)
    filewriter.move(
        old_artifact_dir,
        new_artifact_dir,
    )


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
    filewriter = CopyOnWriteFile(db_dir)
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
    filewriter.makedirs(temp_dir, exist_ok=True)
    filewriter.linktree(folder_dir, temp_dir, dirs_exist_ok=True)


def copy_temp_to_db(
    process_id: str,
    db_dir: str,
):
    filewriter = CopyOnWriteFile(db_dir)
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER, process_id)
    if os.path.isdir(temp_dir):
        filewriter.linktree(temp_dir, db_dir, dirs_exist_ok=True)


def delete_from_temp(process_id: str, db_dir: str):
    filewriter = CopyOnWriteFile(db_dir)
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER)
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder.startswith(process_id):
            filewriter.rmtree(sub_dir)


def cleanup_temp(active_ids: list[str], db_dir: str):
    filewriter = CopyOnWriteFile(db_dir)
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER)
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder not in active_ids:
            filewriter.rmtree(sub_dir)


def copy_table(
    temp_id: str,
    table_name: str,
    prev_instance_id: str,
    prev_table_name: str,
    db_dir: str,
):
    filewriter = CopyOnWriteFile(db_dir)
    prev_table_path = os.path.join(
        db_dir, prev_table_name, prev_instance_id, constants.TABLE_FILE
    )
    current_table_path = os.path.join(db_dir, table_name, temp_id, constants.TABLE_FILE)
    filewriter.copy2(prev_table_path, current_table_path)
    prev_dtype_path = os.path.join(
        db_dir, prev_table_name, prev_instance_id, constants.DTYPE_FILE
    )
    current_dtype_path = os.path.join(db_dir, table_name, temp_id, constants.DTYPE_FILE)
    filewriter.copy2(prev_dtype_path, current_dtype_path)
    user_lock.set_tv_lock(temp_id, table_name, db_dir)


def load_code_function(
    python_function: str,
    module_name: str,
    db_dir: str,
    instance_id: str = "",
    table_name: str = "",
):
    filewriter = CopyOnWriteFile(db_dir)
    if instance_id != "":
        file_path_ = os.path.join(
            db_dir,
            table_name,
            instance_id,
            constants.ARCHIVE_FOLDER,
            module_name + ".py",
        )
    else:
        file_path_ = os.path.join(db_dir, constants.CODE_FOLDER, module_name + ".py")
    try:
        namespace = {}
        with filewriter.open(file_path_, "r") as file:
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
    filewriter = CopyOnWriteFile(db_dir)
    file_path = os.path.join(db_dir, constants.CODE_FOLDER, module_name + ".py")
    if not os.path.exists(file_path):
        raise TVFileError(f"Function '{module_name}' not found")
    file_path_ = os.path.join(
        db_dir, table_name, instance_id, constants.ARCHIVE_FOLDER, module_name + ".py"
    )  # TODO: edit
    filewriter.copy2(file_path, file_path_)


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
        if not os.path.exists(file_path_):
            return False
        return filecmp.cmp(file_path, file_path_, shallow=False)
    else:
        try:
            origin_func, _ = load_code_function(
                python_function, module_name, db_dir, instance_id, table_name
            )
        except Exception:
            return False
        base_func, _ = load_code_function(python_function, module_name, db_dir)
        return origin_func.__code__ == base_func.__code__


def check_folder_existance(instance_id: str, table_name: str, db_dir: str):
    file_path = db_dir
    if table_name != "":
        file_path = os.path.join(file_path, table_name)
    if instance_id != "":
        file_path = os.path.join(file_path, instance_id)
    return os.path.isdir(file_path)


def sort_with_key(lst) -> list:
    tail_set = set(constants.ILLEGAL_TABLE_NAMES)
    return sorted(lst, key=lambda x: (x in tail_set, x))


def _get_file_tree_all(path: str, tree: Tree, extension: Optional[str] = None) -> True:
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)

        if os.path.isdir(full):
            branch = tree.add(f"[bold magenta]{name}/")
            _get_file_tree_all(full, branch, extension)
        elif extension is None or name.endswith(extension):
            tree.add(f"[red]{name}")
    return tree


def _get_file_tree(
    path: str,
    tree: Tree,
    code_files: bool,
    builder_files: bool,
    metadata_files: bool,
    artifact_files: bool,
    db_dir: str,
) -> Tree:
    try:
        file_names = list(os.listdir(path))
    except Exception:
        return
    file_names = sort_with_key(file_names)
    for name in file_names:
        full = os.path.join(path, name)
        if name == constants.TABLEVAULT_IDENTIFIER or name.endswith(".lock"):
            continue
        elif not metadata_files and name in [
            constants.LOCK_FOLDER,
            constants.TEMP_FOLDER,
            constants.METADATA_FOLDER,
            constants.META_DESCRIPTION_FILE,
            constants.ARCHIVE_FOLDER,
            constants.DELETION_FOLDER,
            constants.TABLE_FILE,
        ]:
            continue
        elif name == constants.BUILDER_FOLDER and not builder_files:
            continue
        elif name == constants.BUILDER_FOLDER:
            branch = tree.add(f"[bold blue]{name}/")
            _get_file_tree_all(full, branch, ".yaml")
        elif name == constants.ARTIFACT_FOLDER and not artifact_files:
            continue
        elif name == constants.ARTIFACT_FOLDER:
            branch = tree.add(f"[bold blue]{name}/")
            _get_file_tree_all(full, branch)
        elif name == constants.CODE_FOLDER and not code_files:
            continue
        elif name == constants.CODE_FOLDER:
            branch = tree.add(f"[bold blue]{name}/")
            _get_file_tree_all(full, branch, ".py")
        elif name == constants.METADATA_FOLDER:
            branch = tree.add(f"[bold blue]{name}/")
            _get_file_tree(
                full,
                branch,
                code_files,
                builder_files,
                metadata_files,
                artifact_files,
                db_dir,
            )
        elif name == constants.META_DESCRIPTION_FILE:
            tree.add(f"[cyan]{name}")
        elif name in [
            constants.LOCK_FOLDER,
            constants.TEMP_FOLDER,
            constants.ARCHIVE_FOLDER,
            constants.DELETION_FOLDER,
        ]:
            branch = tree.add(f"[bold blue]{name}/")
        elif os.path.isdir(full):
            branch = tree.add(f"[bold magenta]{name}/")
            _get_file_tree(
                full,
                branch,
                code_files,
                builder_files,
                metadata_files,
                artifact_files,
                db_dir,
            )
        elif name == constants.DTYPE_FILE:
            try:
                filewriter = CopyOnWriteFile(db_dir)
                with filewriter.open(full, "r") as f:
                    dtypes = json.load(f)
                for column, type_ in dtypes.items():
                    branch = tree.add(f"[bright_magenta]{column}({type_})")
                if metadata_files:
                    branch = tree.add(f"[green]{name}/")
            except Exception:
                pass
        else:
            tree.add(f"[green]{name}")
    return tree


def get_file_tree(
    instance_id: str,
    table_name: str,
    code_files: bool,
    builder_files: bool,
    metadata_files: bool,
    artifact_files: bool,
    db_dir: str,
) -> Tree:
    full = db_dir
    if table_name != "":
        full = os.path.join(full, table_name)
    if instance_id != "":
        full = os.path.join(full, instance_id)
    tree = Tree(f"[bold magenta]{full}/")
    return _get_file_tree(
        full, tree, code_files, builder_files, metadata_files, artifact_files, db_dir
    )
