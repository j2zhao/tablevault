from pydantic import Field
from pydantic.dataclasses import dataclass

from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault._llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
import openai
from tablevault.defintions import constants
import threading
from concurrent.futures import ThreadPoolExecutor
from tablevault.prompts.utils import utils, table_operations
from tablevault.defintions.utils import gen_tv_id
import re
from typing import Any

@dataclass
class Message:
    text: utils.TableString
    regex: list[str] = Field(default=[])
    keywords: dict[str, Any] = Field(default={})

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
    open_ai_key: str
    n_threads: int = Field(default=1)
    context_files: list[utils.TableString] = Field(default=[])
    file_msgs: list[utils.TableString] = Field(default=[])
    context_msgs: list[utils.TableString] = Field(default=[])
    instructions: utils.TableString = Field(default='')
    questions: list[Message]
    model: str
    temperature: float
    retry: int = 10
    key_file: str
    
    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
    ) -> None:
        with open(self.key_file, "r") as f:
            secret = f.read()
            add_open_ai_secret(secret)
        client = openai.OpenAI()
        indices = list(range(len(cache[constants.TABLE_SELF])))
        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            executor.map(
                lambda i: _execute_llm(
                    i, self, client, lock, cache, instance_id, table_name, db_dir
                ),
                indices,
            )

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
    uses_files = len(prompt.context_files) > 0
    thread = Open_AI_Thread(
        name,
        prompt.model,
        prompt.temperature,
        prompt.retry,
        utils.parse_table_string(prompt.instructions, cache, index),
        client=client,
        uses_files=uses_files,
    )
    if thread.success is False:
        return
    
    file_msgs = utils.parse_table_string(prompt.file_msgs, cache, index)
    cfiles = utils.parse_table_string(prompt.context_files, cache, index)
    if len(file_msgs) == len(cfiles):
        for i, cfile in enumerate(cfiles):
            thread.add_message(message=file_msgs[i], file_ids=[cfile])
    elif len(prompt.context_files) > 0:
        if len(file_msgs) > 0:
            file_msg = '\n'.join(file_msgs)
        else:
            file_msg = ''
        thread.add_message(file_msg, file_ids=cfiles)

    results = []
    for question in prompt.questions:
        question_ = utils.parse_table_string(question.text)
        for key, word in question.keywords.items():
            word = str(utils.parse_table_string(word, Cache, index))
            question_ = question_.replace(key, word)
            result = thread.run_query()
            thread.add_message(question)
            for pattern in  question.regex:
                cleaned_text = result.replace("\n", "")
                match = re.search(pattern, cleaned_text)
                str_match = match.group(1)
                if str_match is not None:
                    result = str_match
                    break
        results.append(result)

    with lock:
        for i, column in enumerate(prompt.changed_columns):
            table_operations.update_entry(results[i], index, column, cache[constants.TABLE_SELF])
        table_operations.write_table(cache[constants.TABLE_SELF], instance_id, table_name, db_dir)

