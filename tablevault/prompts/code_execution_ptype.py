from pydantic import Field
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.prompts.utils.table_string import apply_table_string
from tablevault.defintions import constants
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Optional

class CodePrompt(TVPrompt):
    is_custom: bool = Field(description="Custom to database.")
    is_udf: bool = Field(description="Is UDF function.")
    n_threads: int = Field(default=1, description="Number of Threads to run (if udf).")
    code_module: str = Field(description="Module of function.") 
    python_function: str = Field(description="Function to execute.")
    arguments: dict[str, Any] = Field(description="Function Arguments. DataTable and TableStrings are valid.")

    def execute(self,
                cache: Cache,
                instance_id: str,
                table_name: str,
                db_dir: str) -> bool:
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_module, self.python_function, db_dir)
        df = cache[constants.TABLE_SELF]

        if self.is_udf:
            indices = list(range(len(df)))
            with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                results = list(
                    executor.map(
                        lambda i: _execute_code_from_prompt(i, self, funct, cache),
                        indices,
                    )
                )
                for col, values in zip(self.changed_columns, zip(*results)):
                    table_operations.update_column(values, cache[constants.TABLE_SELF], col)
        else:
            results = _execute_code_from_prompt(-1, self, funct, cache)
            for col, values in zip(self.changed_columns, results):
                table_operations.update_column(values, cache[constants.TABLE_SELF], col)
        table_operations.write_table(df, instance_id, table_name, db_dir)


def _execute_code_from_prompt(
    index: int,
    prompt: CodePrompt,
    funct: Callable,
    cache: Cache,
) -> Any:
    if index >=0:
        is_filled, entry = table_operations.check_entry(
            index, prompt.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return entry

    kwargs = apply_table_string(prompt.arguments, cache, index)
    results = funct(**kwargs)
    return results

