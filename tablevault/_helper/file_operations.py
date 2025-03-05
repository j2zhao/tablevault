import os
import shutil
import pandas as pd
import json
import yaml
import filecmp
from tablevault._defintions.types import Prompt
from tablevault._defintions.tv_errors import TVFileError
from tablevault._helper.database_lock import DatabaseLock
from tablevault._defintions import constants, prompt_constants

def delete_database_folder(db_dir) -> None:
    shutil.rmtree(db_dir)


def setup_database_folder(db_dir: str, replace: bool = False) -> None:
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
    os.makedirs(meta_dir)

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

    with open(os.path.join(meta_dir, constants.META_MULT_FILE), "w") as file:
        json.dump({}, file)
    db_lock = DatabaseLock("", db_dir)
    db_lock.make_lock_path(constants.RESTART_LOCK)
    db_lock.make_lock_path(constants.CODE_FOLDER)
    

def setup_table_instance_folder(
    instance_id: str,
    table_name: str,
    db_dir: str,
    origin: str = "",
    prompts: list[str] = [],
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, instance_id)
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    # create or copy promtpts
    prompt_dir = os.path.join(temp_dir, constants.PROMPT_FOLDER)
    current_table_path = os.path.join(temp_dir, constants.TABLE_FILE)

    if origin != "":
        prev_dir = os.path.join(table_dir, str(origin))
        prev_prompt_dir = os.path.join(prev_dir, constants.PROMPT_FOLDER)
        shutil.copytree(prev_prompt_dir, prompt_dir, copy_function=shutil.copy2)

    elif len(prompts) != 0:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        prompt_dir_ = os.path.join(table_dir, constants.PROMPT_FOLDER)
        for prompt in prompts:
            prompt_path_ = os.path.join(prompt_dir_, prompt + ".yaml")
            prompt_path = os.path.join(prompt_dir, prompt + ".yaml")
            shutil.copy2(prompt_path_, prompt_path)
    else:
        os.makedirs(prompt_dir)
    df = pd.DataFrame()
    df.to_csv(current_table_path, index=False)


def setup_table_folder(table_name: str, db_dir: str) -> None:
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)
    if os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    prompt_dir_ = os.path.join(table_dir, constants.PROMPT_FOLDER)
    os.makedirs(prompt_dir_)


def materialize_table_folder(
    instance_id: str, temp_instance_id: str, table_name: str, db_dir: str
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, temp_instance_id)
    if not os.path.exists(temp_dir):
        raise TVFileError("No Temp Table Found")
    new_dir = os.path.join(table_dir, instance_id)
    if os.path.exists(new_dir):
        return
    os.rename(temp_dir, new_dir)


def delete_table_folder(table_name: str, db_dir: str, instance_id: str = "") -> None:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, str(instance_id))
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)


def get_prompts(
    instance_id: str, table_name: str, db_dir: str
) -> dict[str, Prompt]:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, instance_id)
    prompt_dir = os.path.join(table_dir, constants.PROMPT_FOLDER)
    prompts = {}
    for item in os.listdir(prompt_dir):
        if item.endswith(".yaml"):
            name = item.split(".")[0]
            prompt_path = os.path.join(prompt_dir, item)
            with open(prompt_path, "r") as file:
                prompt = yaml.safe_load(file)
                prompt[prompt_constants.NAME] = name
            prompts[name] = prompt
    return prompts


def get_db_prompts(db_dir: str) -> dict[str, Prompt]:
    prompts = {}
    for table_name in os.listdir(db_dir):
        prompt_dir = os.path.join(db_dir, table_name, constants.PROMPT_FOLDER)
        if os.path.exists(prompt_dir):
            prompts[table_name] = get_prompts("", table_name, db_dir)
    return prompts


def get_prompt_names(instance_id: str, table_name: str, db_dir: str) -> list[str]:
    if instance_id != "":
        prompt_dir = os.path.join(db_dir, table_name, instance_id, constants.PROMPT_FOLDER)
    else:
        prompt_dir = os.path.join(db_dir, table_name, constants.PROMPT_FOLDER)
    prompt_names = []
    for file in os.listdir(prompt_dir):
        if file.endswith(".yaml"):
            prompt = file.split(".")[0]
            prompt_names.append(prompt)
    return prompt_names


def check_prompt_equality(
    pname: str, instance_id_1: str, instance_id_2: str, table_name: str, db_dir: str
) -> bool:
    table_dir = os.path.join(db_dir, table_name)
    prompt_dir_1 = os.path.join(table_dir, instance_id_1, constants.PROMPT_FOLDER, f"{pname}.yaml")
    prompt_dir_2 = os.path.join(table_dir, instance_id_2, constants.PROMPT_FOLDER, f"{pname}.yaml")
    return filecmp.cmp(prompt_dir_1, prompt_dir_2, shallow=False)


def check_temp_instance_existance(
    instance_id: str, table_name: str, db_dir: str
) -> str:
    instance_dir = os.path.join(db_dir, table_name, instance_id)
    return os.path.exists(instance_dir)


def copy_files(file_dir: str, 
               table_name: str, 
               db_dir: str) -> None:
    try:
        if table_name != "":
            new_dir = os.path.join(db_dir, table_name, constants.PROMPT_FOLDER)
            if file_dir.endswith(".yaml"):
                shutil.copy2(file_dir, new_dir)
            else:
                for f in os.listdir(file_dir):
                    if f.endswith(".yaml"):
                        p_path = os.path.join(file_dir, f)
                        shutil.copy2(p_path, new_dir)
        else:
            new_dir = os.path.join(db_dir, constants.CODE_FOLDER)
            if file_dir.endswith(".py"):
                shutil.copy2(file_dir, new_dir)
            else:
                for f in os.listdir(file_dir):
                    if f.endswith(".py"):
                        p_path = os.path.join(file_dir, f)
                        shutil.copy2(p_path, new_dir)
    except Exception as e:
        raise TVFileError(f"Error copying from {file_dir}: {e}")


def copy_table(temp_id: str, prev_instance_id: str, table_name: str, db_dir: str):
    table_dir = os.path.join(db_dir, table_name)
    prev_table_path = os.path.join(table_dir, prev_instance_id, constants.TABLE_FILE)
    current_table_path = os.path.join(table_dir, temp_id, constants.TABLE_FILE)
    shutil.copy2(prev_table_path, current_table_path)


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
    os.makedirs(temp_dir)
    if os.path.isdir(folder_dir):
        temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER, process_id)
        shutil.copy2(folder_dir, temp_dir)


def copy_temp_to_db(
    process_id: str,
    db_dir: str,
):
    temp_dir = os.path.join(db_dir, constants.TEMP_FOLDER, process_id)
    if os.path.isdir(temp_dir):
        shutil.copytree(temp_dir, db_dir,dirs_exist_ok=True)


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
