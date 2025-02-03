import os
import shutil
import pandas as pd
import json
from typing import Optional
import yaml
import filecmp
from tablevault._prompt_parsing.prompt_types import Prompt


def setup_database_folder(db_dir: str, replace: bool = False) -> None:
    if not replace and os.path.exists(db_dir):
        raise FileExistsError("path already taken")
    elif replace and os.path.isdir(db_dir):
        shutil.rmtree(db_dir)
    elif replace and os.path.isfile(db_dir):
        os.remove(db_dir)

    os.makedirs(db_dir)
    os.makedirs(os.path.join(db_dir, "code_functions"))
    meta_dir = os.path.join(db_dir, "metadata")
    os.makedirs(meta_dir)

    with open(os.path.join(meta_dir, "log.txt"), "w") as file:
        pass

    with open(os.path.join(meta_dir, "active_log.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "columns_history.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "tables_history.json"), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, "tables_multiple.json"), "w") as file:
        json.dump({}, file)


def clear_table_instance(instance_id: str, table_name: str, db_dir: str) -> None:
    if not instance_id.startswith("TEMP"):
        raise ValueError('Temp folder name has to start with "TEMP"')
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, instance_id)
    prompt_dir = os.path.join(temp_dir, "prompts")
    metadata_path = os.path.join(prompt_dir, "description.yaml")
    with open(metadata_path, "r") as file:
        metadata = yaml.safe_load(file)
    if "origin" in metadata:
        prev_dir = os.path.join(table_dir, metadata["origin"])
        prev_table_path = os.path.join(prev_dir, "table.csv")
        current_table_path = os.path.join(temp_dir, "table.csv")
        shutil.copy2(prev_table_path, current_table_path)
    else:
        df = pd.DataFrame()
        current_table_path = os.path.join(temp_dir, "table.csv")
        df.to_csv(current_table_path, index=False)


def setup_table_instance_folder(
    instance_id: str,
    table_name: str,
    db_dir: str,
    origin: str = "",
    prompts: list[str] = [],
    gen_prompt: str = "",
) -> None:
    if not instance_id.startswith("TEMP"):
        raise ValueError('Temp folder name has to start with "TEMP"')
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
        prev_table_path = os.path.join(prev_dir, "table.csv")
        shutil.copy2(prev_table_path, current_table_path)
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
        metadata = {"table_generator": gen_prompt}
        metadata["copied_prompts"] = prompts
        with open(metadata_path, "w") as file:
            yaml.safe_dump(metadata, file)
    else:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        with open(metadata_path, "w") as file:
            pass


def setup_table_folder(table_name: str, db_dir: str) -> None:
    if table_name == "DATABASE" or table_name == "TABLE" or table_name == "RESTART":
        raise ValueError(f"Special Name Taken: {table_name}.")
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)
    if os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    prompt_dir = os.path.join(table_dir, "prompts")
    os.makedirs(prompt_dir)


def materialize_table_folder(
    instance_id: str, temp_instance_id: str, table_name: str, db_dir: str
) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, temp_instance_id)
    if not os.path.exists(temp_dir):
        raise ValueError("No Table In Progress")
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


# get table
def get_table(
    instance_id: str, table_name: str, db_dir: str, rows: Optional[int] = None
) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_dir = os.path.join(table_dir, "table.csv")
    try:
        df = pd.read_csv(table_dir, nrows=rows, dtype=str)
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def get_prompts(instance_id: str, table_name: str, db_dir: str) -> list[Prompt]:
    table_dir = os.path.join(db_dir, table_name)
    instance_dir = os.path.join(table_dir, instance_id)
    prompt_dir = os.path.join(instance_dir, "prompts")
    prompts = {}
    for item in os.listdir(prompt_dir):
        if item.endswith(".yaml"):
            name = item.split(".")[0]
            prompt_path = os.path.join(prompt_dir, item)
            with open(prompt_path, "r") as file:
                prompt = yaml.safe_load(file)
                prompt["name"] = name
            prompts[name] = prompt
    return prompts


def write_table(
    df: pd.DataFrame, instance_id: str, table_name: str, db_dir: str
) -> None:
    if "pos_index" in df.columns:
        df.drop(columns="pos_index", inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_dir = os.path.join(table_dir, "table.csv")
    df = df.to_csv(table_dir, index=False)


def get_prompt_names(instance_id: str, table_name: str, db_dir: str) -> list[str]:
    prompt_dir = os.path.join(db_dir, table_name, instance_id, "prompts")
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


def check_temp_instance_existance(instance_id: str, table_name: str, db_dir: str):
    instance_dir = os.path.join(db_dir, table_name, instance_id)
    return os.path.exists(instance_dir)
