from pydantic import Field
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.prompts.utils.table_string import apply_table_string
from tablevault.defintions import constants
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any
from tablevault.prompts.utils.table_string import TableReference
from typing import Union

class CodePrompt(TVPrompt):
    is_custom:  Union[bool, TableReference] = Field(description="Custom to database.")
    is_udf: Union[bool, TableReference] = Field(description="Is UDF function.")
    n_threads:  Union[int, TableReference] = Field(default=1, description="Number of Threads to run (if udf).")
    code_module:  Union[str, TableReference] = Field(description="Module of function.") 
    python_function: Union[str, TableReference] = Field(description="Function to execute.")
    arguments: dict[Union[str, TableReference], Any] = Field(description="Function Arguments. DataTable and TableStrings are valid.")
    save_by_row: Union[bool, TableReference] = Field(default=False, description="If True, save to disk after each row.")
    
    def execute(self,
                cache: Cache,
                instance_id: str,
                table_name: str,
                db_dir: str) -> bool:
        self.transform_table_string(cache, index=None)
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_module, self.python_function, db_dir)
        if self.is_udf:
            indices = list(range(len(cache[constants.TABLE_SELF])))
            with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                results = list(
                    executor.map(
                        lambda i: _execute_code_from_prompt(i, self, funct, cache),
                        indices,
                    )
                )
                if not self.save_by_row:
                    for col, values in zip(self.changed_columns, zip(*results)):
                        table_operations.update_column(values, cache[constants.OUTPUT_SELF], col)
                    table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)
        else:
            results = _execute_code_from_prompt(-1, self, funct, cache)
            for col, values in zip(self.changed_columns, results):
                table_operations.update_column(values, cache[constants.OUTPUT_SELF], col)
            table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)

def _execute_code_from_prompt(
    index: int,
    prompt: CodePrompt,
    funct: Callable,
    cache: Cache,
    instance_id: str,
    table_name:str,
    db_dir:str,
) -> Any:
    if index >=0:
        is_filled, entry = table_operations.check_entry(
            index, prompt.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return entry
    
    prompt = prompt.model_copy(deep=True)
    prompt.transform_table_string(cache, instance_id, table_name, db_dir, index)
    results = funct(**prompt.arguments)
    if prompt.save_by_row:
        for i, result in enumerate(results):
            table_operations.update_entry(result, index, prompt.changed_columns[i], cache[constants.OUTPUT_SELF])
        table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)
        return None
    return results

