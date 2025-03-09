import os
from typing import Any, Callable
from concurrent.futures import ThreadPoolExecutor
import re
from tablevault._prompt_parsing import prompt_parser
from importlib import import_module
from tablevault.prompts.utils import table_operations
from tablevault.defintions import constants, prompt_constants

def load_function_from_file(file_path: str, function_name: str) -> tuple[Callable, Any]:
    namespace = {}
    with open(file_path, "r") as file:
        exec(file.read(), namespace)
    if function_name in namespace:
        return namespace[function_name], namespace
    else:
        raise AttributeError(f"Function '{function_name}' not found in '{file_path}'")


def get_function_from_module(module_name: str, function_name: str) -> Callable:

    # Import the module by its absolute name
    module = import_module(module_name)
    # Retrieve the attribute (which should be a function)
    func = getattr(module, function_name, None)

    # Validate it's actually callable
    if not callable(func):
        print(type(func))
        raise TypeError(f"'{function_name}' in '{module_name}' is not a callable.")

    return func


def _execute_code_from_prompt(
    index: int,
    prompt: prompt_parser.Prompt,
    funct: Callable,
    cache: prompt_parser.Cache,
) -> tuple[Any]:
    is_filled, entry = table_operations.check_entry(
        index, prompt[prompt_constants.CHANGED_COLUMNS], cache[prompt_constants.TABLE_SELF]
    )
    if is_filled:
        return entry

    args = prompt_parser.get_table_value(prompt["arguments"], index, cache)
    table_args = {}
    if "table_arguments" in prompt:
        for tname, table in prompt["table_arguments"].items():
            match = re.match(r"^(\w+)(?:\((\w+)\))?$", table)
            table_name = match.group(1)
            instance_id = match.group(2)
            if instance_id is not None:
                table_key = table_name
            else:
                table_key = (table_name, instance_id)
            table_args[tname] = cache[table_key]
    args = args | table_args
    results = funct(**args)
    return tuple(results)


def _execute_single_code_from_prompt(
    prompt: prompt_parser.Prompt, funct: Callable, cache: prompt_parser.Cache
) -> Any:
    args = prompt_parser.get_table_value(prompt["arguments"], None, cache)
    table_args = {}
    if "table_arguments" in prompt:
        for tname, table in prompt["table_arguments"].items():
            match = re.match(r"^(\w+)(?:\((\w+)\))?$", table)
            table_name = match.group(1)
            instance_id = match.group(2)
            if instance_id is None:
                table_key = table_name
            else:
                table_key = (table_name, instance_id)
            table_args[tname] = cache[table_key]
    args = args | table_args

    results = funct(**args)
    return results


def execute_code_from_prompt(
    prompt: prompt_parser.Prompt,
    cache: prompt_parser.Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    is_udf = prompt["is_udf"]
    is_global = prompt["is_global"]
    code_file = prompt["code_file"]
    prompt_function = prompt["function"]
    if "n_threads" in prompt:
        n_threads = prompt["n_threads"]
    else:
        n_threads = 1
    if is_global:
        code_file = code_file.split(".")[0]
        funct = get_function_from_module(
            f"tablevault.code_functions.{code_file}", prompt_function
        )
    else:
        code_dir = os.path.join(db_dir, constants.CODE_FOLDER)
        code_file = os.path.join(code_dir, code_file)
        funct, _ = load_function_from_file(code_file, prompt_function)

    df = cache["self"]
    if is_udf:
        indices = list(range(len(df)))
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            results = list(
                executor.map(
                    lambda i: _execute_code_from_prompt(i, prompt, funct, cache),
                    indices,
                )
            )
            for col, values in zip(prompt[prompt_constants.CHANGED_COLUMNS], zip(*results)):
                table_operations.update_column(values, cache[prompt_constants.TABLE_SELF], col)
    else:
        results = _execute_single_code_from_prompt(prompt, funct, cache)
        for col, values in zip(prompt[prompt_constants.CHANGED_COLUMNS], results):
            table_operations.update_column(values, cache[prompt_constants.TABLE_SELF], col)
    table_operations.write_table(df, instance_id, table_name, db_dir)


def execute_gen_table_from_prompt(
    prompt: prompt_parser.Prompt,
    cache: prompt_parser.Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> bool:
    is_global = prompt["is_global"]
    code_file = prompt["code_file"]
    prompt_function = prompt["function"]

    if is_global:
        code_file = code_file.split(".")[0]
        funct = get_function_from_module(
            f"tablevault.code_functions.{code_file}", prompt_function
        )
    else:
        code_dir = os.path.join(db_dir, ƒ)
        code_file = os.path.join(code_dir, code_file)
        funct, _ = load_function_from_file(code_file, prompt_function)

    results = _execute_single_code_from_prompt(prompt, funct, cache)
    merged_df, changed = table_operations.merge_columns(
        prompt[prompt_constants.CHANGED_COLUMNS], results, cache[prompt_constants.TABLE_SELF]
    )
    if changed:
        cache[prompt_constants.TABLE_SELF] = merged_df
        table_operations.write_table(merged_df, instance_id, table_name, db_dir)
        return True
    else:
        return False
