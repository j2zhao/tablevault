from pydantic import Field, BaseModel
from tablevault.builders.base_builder_type import TVBuilder
from tablevault.defintions.types import Cache
from tablevault._llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
import openai
from tablevault.defintions import constants
import threading
from concurrent.futures import ThreadPoolExecutor
from tablevault.dataframe_helper import table_operations
from tablevault.builders.utils.table_string import TableString
from tablevault.helper.utils import gen_tv_id
import re
from typing import Any
from tablevault.defintions import tv_errors


class Message(BaseModel):
    text: TableString = Field(description="Message to model")
    regex: list[str] = Field(default=[], description="regex of output")


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


class OAIThreadBuilder(TVBuilder):
    n_threads: int = Field(default=1, description="Number of Threads to run.")
    upload_files: list[TableString] = Field(
        default=[], description="List of files to upload."
    )
    file_msgs: list[TableString] = Field(
        default=[], description="Context message for files."
    )
    context_msgs: list[TableString] = Field(
        default=[], description="Context message for thread."
    )
    instructions: TableString = Field(default="", description="Instructions for model.")
    questions: list[Message] = Field(description="List of Messages (output recorded).")
    model: str = Field(description="Model name.")
    temperature: float = Field(description="Model temperature.")
    retry: int = Field(default=5, description="Retry on fail.")
    key_file: str = Field(description="File location of OpenAI secret.")
    keywords: dict[str, Any] = Field(
        default={}, description="Keywords to replace message."
    )

    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
        process_id: str,
    ) -> None:
        try:
            self.transform_table_string(cache, instance_id, table_name, db_dir)
            with open(self.key_file, "r") as f:
                secret = f.read()
                add_open_ai_secret(secret)
            client = openai.OpenAI()
            indices = list(range(len(cache[constants.TABLE_SELF])))
            # lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                futures = [
                    executor.submit(
                        _execute_llm,
                        i,
                        self,
                        client,
                        # lock,
                        cache,
                        instance_id,
                        table_name,
                        db_dir,
                    )
                    for i in indices
                ]
                # Iterate over futures to force evaluation and raise any exceptions
                for future in futures:
                    future.result()
        finally:
            table_operations.make_df(instance_id, table_name, db_dir)


def _execute_llm(
    index: int,
    builder: OAIThreadBuilder,
    client: openai.OpenAI,
    # lock: threading.Lock,
    cache: Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    builder = builder.model_copy(deep=True)
    builder.transform_table_string(cache, instance_id, table_name, db_dir, index)

    is_filled, _ = table_operations.check_entry(
        index, builder.changed_columns, cache[constants.TABLE_SELF]
    )
    if is_filled:
        return

    name = "_".join([builder.name, str(index), gen_tv_id()])
    uses_files = len(builder.upload_files) > 0
    thread = Open_AI_Thread(
        name,
        builder.model,
        builder.temperature,
        builder.retry,
        builder.instructions,
        client=client,
        uses_files=uses_files,
    )
    if thread.success is False:
        raise tv_errors.TVBuilderError("Thread Not Created")
    file_msgs = builder.file_msgs
    cfiles = builder.upload_files
    if len(file_msgs) == len(cfiles):
        for i, cfile in enumerate(cfiles):
            thread.add_message(message=file_msgs[i], file_ids=[cfile])
    elif len(builder.upload_files) > 0:
        if len(file_msgs) > 0:
            file_msg = "\n".join(file_msgs)
        else:
            file_msg = ""
        thread.add_message(file_msg, file_ids=cfiles)
    results = []
    for question in builder.questions:
        question_ = question.text
        for key, word in builder.keywords.items():
            question_ = question_.replace(key, str(word))
        thread.add_message(question_)
        result = thread.run_query()
        for pattern in question.regex:
            cleaned_text = result.replace("\n", "")
            match = re.search(pattern, cleaned_text)
            if match is None:
                continue
            str_match = match.group(0)
            result = str_match
            break
        results.append(result)

    for i, column in enumerate(builder.changed_columns):
        table_operations.write_df_entry(
            results[i], index, column, instance_id, table_name, db_dir
        )
