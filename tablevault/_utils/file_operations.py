import os
import shutil
import pandas as pd
import json
import yaml
import filecmp
from tablevault._prompt_parsing.types import Prompt
from tablevault._utils.errors import TVFileError


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
    os.makedirs(os.path.join(db_dir, "code_functions"))
    os.makedirs(os.path.join(db_dir, "_temp"))
    meta_dir = os.path.join(db_dir, "metadata")
    os.makedirs(meta_dir)

    with open(os.path.join(meta_dir, "logs.txt"), "w") as file:
        pass
    with open(os.path.join(meta_dir, "completed_logs.txt"), "w") as file:
        pass

    with open(os.path.join(meta_dir, "active_logs.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "columns_history.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "tables_history.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "tables_multiple.json"), "w") as file:
        json.dump({}, file)


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
    prompt_dir = os.path.join(temp_dir, "prompts")
    current_table_path = os.path.join(temp_dir, "table.csv")
    metadata_path = os.path.join(prompt_dir, "description.yaml")

    if origin != "":
        prev_dir = os.path.join(table_dir, str(origin))
        prev_prompt_dir = os.path.join(prev_dir, "prompts")
        shutil.copytree(prev_prompt_dir, prompt_dir, copy_function=shutil.copy2)
        with open(metadata_path, "r") as file:
            metadata = yaml.safe_load(file)
        if "copied_prompts" in metadata:
            del metadata["copied_prompts"]
        metadata["origin"] = origin
        with open(metadata_path, "w") as file:
            yaml.safe_dump(metadata, file)

    elif len(prompts) != 0:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        prompt_dir_ = os.path.join(table_dir, "prompts")
        for prompt in prompts:
            prompt_path_ = os.path.join(prompt_dir_, prompt + ".yaml")
            prompt_path = os.path.join(prompt_dir, prompt + ".yaml")
            shutil.copy2(prompt_path_, prompt_path)
        metadata["copied_prompts"] = prompts
        with open(metadata_path, "w") as file:
            yaml.safe_dump(metadata, file)
    else:
        os.makedirs(prompt_dir)
        with open(metadata_path, "w") as file:
            pass
    df = pd.DataFrame()
    df.to_csv(current_table_path, index=False)


def setup_table_folder(table_name: str, db_dir: str) -> None:
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)
    if os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    prompt_dir_ = os.path.join(table_dir, "prompts")
    os.makedirs(prompt_dir_)


def materialize_table_folder(
    instance_id: str, temp_instance_id: str, table_name: str, db_dir: str
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, temp_instance_id)
    if not os.path.exists(temp_dir):
        raise TVFileError("No Table In Progress")
    new_dir = os.path.join(table_dir, instance_id)
    if os.path.exists(new_dir):
        print("Table already materialized")
        return
    os.rename(temp_dir, new_dir)


def delete_table_folder(table_name: str, db_dir: str, instance_id: str = "") -> None:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, str(instance_id))
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)


def get_prompts(
    instance_id: str, table_name: str, db_dir: str, get_metadata: bool = False
) -> dict[str, Prompt]:
    table_dir = os.path.join(db_dir, table_name)
    if instance_id != "":
        table_dir = os.path.join(table_dir, instance_id)
    prompt_dir = os.path.join(table_dir, "prompts")
    prompts = {}
    for item in os.listdir(prompt_dir):
        if item.endswith(".yaml"):
            name = item.split(".")[0]
            prompt_path = os.path.join(prompt_dir, item)
            with open(prompt_path, "r") as file:
                prompt = yaml.safe_load(file)
                prompt["name"] = name
            prompts[name] = prompt
    if not get_metadata:
        if "description" in prompts:
            del prompts["description"]
    return prompts


def get_db_prompts(db_dir: str, get_metadata: bool = False) -> dict[str, Prompt]:
    prompts = {}
    for table_name in os.listdir(db_dir):
        prompt_dir = os.path.join(db_dir, table_name, "prompts")
        if os.path.exists(prompt_dir):
            prompts[table_name] = get_prompts("", table_name, db_dir, get_metadata)
    return prompts


def get_prompt_names(instance_id: str, table_name: str, db_dir: str) -> list[str]:
    if instance_id != "":
        prompt_dir = os.path.join(db_dir, table_name, instance_id, "prompts")
    else:
        prompt_dir = os.path.join(db_dir, table_name, "prompts")
    prompt_names = []
    for file in os.listdir(prompt_dir):
        if file.endswith(".yaml") and file != "description.yaml":
            prompt = file.split(".")[0]
            prompt_names.append(prompt)
    return prompt_names


def check_prompt_equality(
    pname: str, instance_id_1: str, instance_id_2: str, table_name: str, db_dir: str
) -> bool:
    table_dir = os.path.join(db_dir, table_name)
    prompt_dir_1 = os.path.join(table_dir, instance_id_1, "prompts", f"{pname}.yaml")
    prompt_dir_2 = os.path.join(table_dir, instance_id_2, "prompts", f"{pname}.yaml")
    return filecmp.cmp(prompt_dir_1, prompt_dir_2, shallow=False)


def check_temp_instance_existance(
    instance_id: str, table_name: str, db_dir: str
) -> str:
    instance_dir = os.path.join(db_dir, table_name, instance_id)
    return os.path.exists(instance_dir)


def copy_files(file_dir: str, table_name: str, db_dir: str) -> None:
    try:
        if table_name != "":
            new_dir = os.path.join(db_dir, table_name, "prompts")
            if file_dir.endswith(".yaml"):
                shutil.copy2(file_dir, new_dir)
            else:
                for f in os.listdir(file_dir):
                    if f.endswith(".yaml"):
                        p_path = os.path.join(file_dir, f)
                        shutil.copy2(p_path, new_dir)
        else:
            new_dir = os.path.join(db_dir, "code_functions")
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
    prev_table_path = os.path.join(table_dir, prev_instance_id, "table.csv")
    current_table_path = os.path.join(table_dir, temp_id, "table.csv")
    shutil.copy2(prev_table_path, current_table_path)


def copy_folder_to_temp(
    process_id: str,
    db_dir: str,
    instance_id: str = "",
    table_name: str = "",
    subfolder: str = "",
):
    folder_dir = db_dir
    if table_name != "":
        folder_dir = os.path.join(folder_dir, table_name)
    if instance_id != "":
        folder_dir = os.path.join(folder_dir, instance_id)
    if subfolder != "":
        folder_dir = os.path.join(folder_dir, subfolder)
    if os.path.isdir(folder_dir):
        temp_dir = os.path.join(db_dir, "_temp", process_id)
        shutil.copy2(folder_dir, temp_dir)


def copy_temp_to_folder(
    process_id: str,
    db_dir: str,
    instance_id: str = "",
    table_name: str = "",
    subfolder: str = "",
):
    folder_dir = db_dir
    if table_name != "":
        folder_dir = os.path.join(folder_dir, table_name)
    if instance_id != "":
        folder_dir = os.path.join(folder_dir, instance_id)
    if subfolder != "":
        folder_dir = os.path.join(folder_dir, subfolder)

    temp_dir = os.path.join(db_dir, "_temp", process_id)
    if os.path.isdir(temp_dir):
        shutil.copy2(temp_dir, folder_dir)


def delete_from_temp(process_id: str, db_dir: str):
    temp_dir = os.path.join(db_dir, "_temp")
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder.startswith(process_id):
            shutil.rmtree(sub_dir)


def cleanup_temp(active_ids: list[str], db_dir: str):
    temp_dir = os.path.join(db_dir, "_temp")
    for sub_folder in os.listdir(temp_dir):
        sub_dir = os.path.join(temp_dir, sub_folder)
        if os.path.isdir(sub_dir) and sub_folder not in active_ids:
            shutil.rmtree(sub_dir)
