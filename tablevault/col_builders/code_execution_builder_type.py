from pydantic import Field
from tablevault.col_builders.base_builder_type import TVBuilder
from tablevault.defintions.types import Cache
from tablevault.col_builders.utils import utils, table_operations
from tablevault.col_builders.utils.table_string import apply_table_string
from tablevault.defintions import constants
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any
from tablevault.col_builders.utils.table_string import TableReference
from typing import Union

class CodeBuilder(TVBuilder):
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
                db_dir: str) -> None:
        self.transform_table_string(cache, instance_id, table_name, db_dir, index=None)
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_module, self.python_function, db_dir)
        #raise ValueError()
        if self.is_udf:
            indices = list(range(len(cache[constants.TABLE_SELF])))
            with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                results = list(
                    executor.map(
                        lambda i: _execute_code_from_builder(i, self, funct, cache, instance_id, table_name, db_dir),
                        indices,
                    )
                )
                if not self.save_by_row:
                    for col, values in zip(self.changed_columns, zip(*results)):
                        table_operations.update_column(values, cache[constants.OUTPUT_SELF], col)
                    table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)
        else:
            results = _execute_code_from_builder(-1, self, funct, cache, instance_id, table_name, db_dir)
            for col, values in zip(self.changed_columns, results):
                table_operations.update_column(values, cache[constants.OUTPUT_SELF], col)
            table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)

def _execute_code_from_builder(
    index: int,
    builder: CodeBuilder,
    funct: Callable,
    cache: Cache,
    instance_id: str,
    table_name:str,
    db_dir:str,
) -> Any:
    if index >=0:
        is_filled, entry = table_operations.check_entry(
            index, builder.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return entry
    
    builder = builder.model_copy(deep=True)
    builder.transform_table_string(cache, instance_id, table_name, db_dir, index)
    results = funct(**builder.arguments)
    if builder.save_by_row:
        for i, result in enumerate(results):
            table_operations.update_entry(result, index, builder.changed_columns[i], cache[constants.OUTPUT_SELF])
        table_operations.write_table(cache[constants.OUTPUT_SELF], instance_id, table_name, db_dir)
        return None
    return results

