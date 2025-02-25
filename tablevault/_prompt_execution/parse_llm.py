import string
import random
import threading
from typing import Optional
import openai
import ast
from concurrent.futures import ThreadPoolExecutor
import re

from tablevault._llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
from tablevault._prompt_parsing import prompt_parser
from tablevault._prompt_execution import llm_prompts
from tablevault._utils import _table_operations

# from tablevault._timing_helper.timing_helper import StepsTimer
from tablevault._utils.errors import TVPromptError

# timer = StepsTimer()


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
    is_filled, _ = _table_operations.check_entry(
        index, prompt["changed_columns"], cache["self"]
    )
    if is_filled:
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
    # timer.stop_step('ParseArgs', it)
    # it = timer.start_step('AddArgs')

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
    # timer.stop_step('AddArgs', it)
    for question in questions:
        # it = timer.start_step('AddArgs')
        if prompt["output_type"] == "category":
            question = question.replace("CATEGORIES", str(prompt["category_names"]))
        thread.add_message(question)
        # timer.stop_step('AddArgs', it)
        # it = timer.start_step("RunQuery")
        result = thread.run_query()
        # timer.stop_step("RunQuery", it)
        results.append(result)
    # deal with output_types:
    if prompt["output_type"] == "freeform":
        pass
    elif prompt["output_type"] == "entity":
        # it = timer.start_step("AddArgs")
        msg = llm_prompts.ENTITY_MSG
        msg = msg.replace("ENTITY_NAME", prompt["entity_name"])
        thread.add_message(message=msg)
        # timer.stop_step("AddArgs", it)
        # it = timer.start_step("RunQuery")
        result = thread.run_query()
        # timer.stop_step("RunQuery", it)
        results.append(result)
    elif prompt["output_type"] == "entity_list":
        # it = timer.start_step("AddArgs")
        msg = llm_prompts.ENTITY_LIST_MSG
        msg = msg.replace("ENTITY_NAME", prompt["entity_name"])
        thread.add_message(message=msg)
        # timer.stop_step("AddArgs", it)
        for i in range(prompt["retry"]):
            # it = timer.start_step("RunQuery")
            result = thread.run_query()
            # timer.stop_step("RunQuery", it)
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
        # it = timer.start_step("AddArgs")
        msg = llm_prompts.CATEGORY_MSG
        msg = msg.replace("CATEGORIES", str(prompt["category_names"]))
        thread.add_message(message=msg)
        # timer.stop_step("AddArgs", it)
        # it = timer.start_step("RunQuery")
        result = thread.run_query()
        # timer.stop_step("RunQuery", it)
        results.append(result)
    else:
        raise TVPromptError("Output type not supported")

    with lock:
        for i, column in enumerate(prompt["parsed_changed_columns"]):
            _table_operations.update_entry(results[i], index, column, cache["self"])
        # # it = timer.start_step("WriteTable")
        # # timer.stop_step("WriteTable", it)
        # # it = timer.start_step("WriteTable2")
        _table_operations.write_table(cache["self"], instance_id, table_name, db_dir)
        # # timer.stop_step("WriteTable2", it)


def execute_llm_from_prompt(
    prompt: dict,
    cache: prompt_parser.Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    """Only support OpenAI Thread prompts for now"""
    # it = timer.start_step('PreThread')
    key_file = prompt["open_ai_key"]
    n_threads = prompt["n_threads"]

    with open(key_file, "r") as f:
        secret = f.read()
        add_open_ai_secret(secret)
    client = openai.OpenAI()
    indices = list(range(len(cache["self"])))
    lock = threading.Lock()
    # timer.stop_step('PreThread', it)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        executor.map(
            lambda i: _execute_llm(
                i, prompt, client, lock, cache, instance_id, table_name, db_dir
            ),
            indices,
        )
    # timer.print_results()
