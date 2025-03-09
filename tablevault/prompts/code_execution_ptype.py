from pydantic import model_validator
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.defintions import constants
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Optional

class CodePrompt(TVPrompt):
    is_global: bool
    is_udf: bool
    n_threads: int = 1
    code_file: str
    python_function: str
    arguments: dict[str, Any]

    def execute(self,
                cache: Cache,
                instance_id: str,
                table_name: str,
                db_dir: str) -> bool:
        if self.is_global:
            code_file = self.code_file.split(".")[0]
            funct = utils.get_function_from_module(
                code_file, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_file, self.python_function, db_dir)
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
            results = _execute_code_from_prompt(None, self, funct, cache)
            for col, values in zip(self.changed_columns, results):
                table_operations.update_column(values, cache[constants.TABLE_SELF], col)
        table_operations.write_table(df, instance_id, table_name, db_dir)


def _execute_code_from_prompt(
    index: Optional[int],
    prompt: CodePrompt,
    funct: Callable,
    cache: Cache,
) -> Any:
    if index != None:
        is_filled, entry = table_operations.check_entry(
            index, prompt.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return entry

    kwargs = utils.parse_table_string(prompt.arguments, cache)
    results = funct(**kwargs)
    return results

