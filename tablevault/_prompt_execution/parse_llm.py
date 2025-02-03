import string
import random
import threading
from typing import Optional
import openai
import ast
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import re

from tablevault import _file_operations
from tablevault._llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
from tablevault._prompt_parsing import prompt_parser
from tablevault._prompt_execution import llm_prompts


def _execute_llm(
    index: int,
    prompt: dict,
    client: Optional[openai.OpenAI],
    lock: threading.Lock,
    cache: prompt_parser.Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    df = cache["self"]
    empty = False
    for col in prompt["changed_columns"]:
        if pd.isna(df.at[index, col]):
            empty = True
            break
    if not empty:
        return
    # get open_ai file keys
    name = (
        prompt["name"] + str(index) + "".join(random.choices(string.ascii_letters, k=5))
    )
    if "context_files" in prompt:
        context_files = prompt_parser.get_table_value(
            prompt["context_files"], index, cache
        )
    else:
        context_files = []
    if "context_msgs" in prompt:
        context_msgs = prompt_parser.get_table_value(
            prompt["context_msgs"], index, cache
        )
    else:
        context_msgs = ""
    if "instructions" in prompt:
        instructions = prompt_parser.get_table_value(
            prompt["instructions"], index, cache
        )
    else:
        instructions = None
    questions = prompt_parser.get_table_value(prompt["questions"], index, cache)

    uses_files = len(context_files) > 0
    thread = Open_AI_Thread(
        name,
        prompt["model"],
        prompt["temperature"],
        prompt["retry"],
        instructions,
        client=client,
        uses_files=uses_files,
    )
    if thread.success is False:
        return
    if isinstance(context_msgs, list):
        for i, cfile in enumerate(context_files):
            thread.add_message(message=context_msgs[i], file_ids=[cfile])
    else:
        thread.add_message(context_msgs, file_ids=context_files)
    if prompt["output_type"] == "category" and "category_definition" in prompt:
        thread.add_message(prompt["category_definition"])
    # parse and add questions
    results = []
    for question in questions:
        if prompt["output_type"] == "category":
            question = question.replace("CATEGORIES", str(prompt["category_names"]))
        thread.add_message(question)
        result = thread.run_query()
        results.append(result)
    # deal with output_types:
    if prompt["output_type"] == "freeform":
        pass
    elif prompt["output_type"] == "entity":
        msg = llm_prompts.ENTITY_MSG
        msg = msg.replace("ENTITY_NAME", prompt["entity_name"])
        thread.add_message(message=msg)
        result = thread.run_query()
        results.append(result)
    elif prompt["output_type"] == "entity_list":
        msg = llm_prompts.ENTITY_LIST_MSG
        msg = msg.replace("ENTITY_NAME", prompt["entity_name"])
        thread.add_message(message=msg)
        for i in range(prompt["retry"]):
            result = thread.run_query()
            cleaned_text = result.replace("\n", "")
            cleaned_text = re.sub(r"\s+", " ", result).strip()
            match = re.search(r"(\[.*?\])", cleaned_text)
            result = match.group(1)
            try:
                result = ast.literal_eval(result)
                break
            except Exception as e:
                print("Could not convert to Python list")
                print(e)
        results.append(result)

    elif prompt["output_type"] == "category":
        msg = llm_prompts.CATEGORY_MSG
        msg = msg.replace("CATEGORIES", str(prompt["category_names"]))
        thread.add_message(message=msg)
        result = thread.run_query()
        results.append(result)
    else:
        raise ValueError("Output type not supported")
    with lock:
        for i, column in enumerate(prompt["parsed_changed_columns"]):
            df.at[index, column] = results[i]
        _file_operations.write_table(df, instance_id, table_name, db_dir)


def execute_llm_from_prompt(
    prompt: dict,
    cache: prompt_parser.Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    """Only support OpenAI Thread prompts for now"""
    key_file = prompt["open_ai_key"]
    n_threads = prompt["n_threads"]

    with open(key_file, "r") as f:
        secret = f.read()
        add_open_ai_secret(secret)
    client = openai.OpenAI()
    indices = list(range(len(cache["self"])))
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        executor.map(
            lambda i: _execute_llm(
                i, prompt, client, lock, cache, instance_id, table_name, db_dir
            ),
            indices,
        )
