from pydantic import Field, BaseModel
from dataclasses import dataclass
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault._llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
import openai
from tablevault.defintions import constants
import threading
from concurrent.futures import ThreadPoolExecutor
from tablevault.prompts.utils import table_operations
from tablevault.prompts.table_string import TableString, apply_table_string
from tablevault.helper.utils import gen_tv_id
import re
from typing import Any

class Message(BaseModel):
    text: TableString = Field(description='Message to model')
    regex: list[str] = Field(default=[], description='regex of output')

# CATEGORY_MSG = """Based on the previous messages and your analysis,
# respond with the option(s) that the paper matches out of: ENTITIES.
# If there are multiple options, separate with commas.
# Do not include any additional text."""

# ENTITY_LIST_MSG = """Based on the previous message,
# return a list of all ENTITY_NAME in python formatting
# (with square brackets, separated by commons, text in quotations).
# If there is no ENTITY_NAME, return an empty list."""

# ENTITY_MSG = """Based on the previous message,
# return a succint phase that captures ENTITY_NAME.
# If there is no ENTITY_NAME, return a message that says "NONE" only."""

class OAIThreadPrompt(TVPrompt):
    n_threads: int = Field(default=1, description="Number of Threads to run.")
    upload_files: list[TableString] = Field(default=[], description="List of files to upload.")
    file_msgs: list[TableString] = Field(default=[], description= "Context message for files.")
    context_msgs: list[TableString] = Field(default=[], description="Context message for thread.")
    instructions: TableString = Field(default='', description="Instructions for model.")
    questions: list[Message] = Field(description="List of Messages (output recorded).")
    model: str = Field(description="Model name.")
    temperature: float = Field(description="Model temperature.")
    retry: int = Field(default=5, description="Retry on fail.")
    key_file: str = Field(default=5, description="File location of OpenAI secret.")
    keywords: dict[str, Any] = Field(default={}, description="Keywords to replace message.")
    
    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str
    ) -> None:
        with open(self.key_file, "r") as f:
            secret = f.read()
            add_open_ai_secret(secret)
        client = openai.OpenAI()
        indices = list(range(len(cache[constants.TABLE_SELF])))
        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            futures = [
                executor.submit(
                    _execute_llm, i, self, client, lock, cache, instance_id, table_name, db_dir
                )
                for i in indices
            ]
            # Iterate over futures to force evaluation and raise any exceptions
            for future in futures:
                future.result()

def _execute_llm(
    index: int,
    prompt: OAIThreadPrompt,
    client: openai.OpenAI,
    lock: threading.Lock,
    cache: Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    is_filled, _ = table_operations.check_entry(
        index, prompt.changed_columns, cache[constants.TABLE_SELF]
    )
    if is_filled:
        return

    name = '_'.join([prompt.name, str(index), gen_tv_id()])
    uses_files = len(prompt.upload_files) > 0
    thread = Open_AI_Thread(
        name,
        prompt.model,
        prompt.temperature,
        prompt.retry,
        apply_table_string(prompt.instructions, cache, index),
        client=client,
        uses_files=uses_files,
    )
    if thread.success is False:
        return
    
    file_msgs = apply_table_string(prompt.file_msgs, cache, index)
    cfiles = apply_table_string(prompt.upload_files, cache, index)
    if len(file_msgs) == len(cfiles):
        for i, cfile in enumerate(cfiles):
            thread.add_message(message=file_msgs[i], file_ids=[cfile])
    elif len(prompt.upload_files) > 0:
        if len(file_msgs) > 0:
            file_msg = '\n'.join(file_msgs)
        else:
            file_msg = ''
        thread.add_message(file_msg, file_ids=cfiles)

    results = []
    for question in prompt.questions:
        question_ = apply_table_string(question.text, cache, index)
        for key, word in prompt.keywords.items():
            word = str(apply_table_string(word, cache, index))
            question_ = question_.replace(key, word)
        thread.add_message(question_)
        result = thread.run_query()
        for pattern in  question.regex:
            cleaned_text = result.replace("\n", "")
            match = re.search(pattern, cleaned_text)
            if match == None:
                continue
            str_match = match.group(0)
            result = str_match
            break
        results.append(result)

    with lock:
        for i, column in enumerate(prompt.changed_columns):
            table_operations.update_entry(results[i], index, column, cache[constants.TABLE_SELF])
        table_operations.write_table(cache[constants.TABLE_SELF], instance_id, table_name, db_dir)

