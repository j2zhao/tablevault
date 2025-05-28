from pydantic import Field
from tablevault.builders.base_builder_type import TVBuilder
from tablevault.dataframe_helper import table_operations
from tablevault.defintions.types import Cache
from tablevault.builders.utils import utils
from tablevault.defintions import constants, tv_errors
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any
from tablevault.builders.utils.table_string import TableReference
from typing import Union
from tablevault.helper.file_operations import load_code_function, move_code_to_instance

class CodeBuilder(TVBuilder):
    is_custom: Union[bool, TableReference] = Field(description="Custom to database.")
    is_udf: Union[bool, TableReference] = Field(description="Is UDF function.")
    n_threads: Union[int, TableReference] = Field(
        default=1, description="Number of Threads to run (if udf)."
    )
    code_module: Union[str, TableReference] = Field(description="Module of function.")
    python_function: Union[str, TableReference] = Field(
        description="Function to execute."
    )
    arguments: dict[Union[str, TableReference], Any] = Field(
        description="Function Arguments. DataTable and TableStrings are valid."
    )
    save_by_row: Union[bool, TableReference] = Field(
        default=False, description="If True, save to disk after each row."
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
            self.transform_table_string(cache, instance_id, table_name, db_dir, index=None)
            if self.name.startswith(constants.GEN_BUILDER_PREFIX) and self.is_udf:
                raise tv_errors.TVArgumentError("Cannot use UDF for a generator builder")
            if not self.is_custom:
                funct = utils.get_function_from_module(
                    self.code_module, self.python_function
                )
            else:
                move_code_to_instance(self.code_module, instance_id, table_name, db_dir)
                funct, _ = load_code_function(
                    self.python_function, self.code_module, db_dir, instance_id, table_name
                )
            # raise ValueError()
            if self.is_udf and self.n_threads != 1:
                indices = list(range(len(cache[constants.TABLE_SELF])))
                with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                    results = list(
                        executor.map(
                            lambda i: _execute_code_from_builder(
                                i, self, funct, cache, instance_id, table_name, db_dir
                            ),
                            indices,
                        )
                    )
                    if not self.save_by_row:
                        table_operations.update_columns(results, self.changed_columns, instance_id, table_name, db_dir)
            elif self.is_udf and self.n_threads == 1:
                results = []
                for i in list(range(len(cache[constants.TABLE_SELF]))):
                    row = _execute_code_from_builder(
                                    i, self, funct, cache, instance_id, table_name, db_dir
                                )
                    if not self.save_by_row:
                        results.append(row)
                if not self.save_by_row:
                    table_operations.update_columns(results, self.changed_columns, instance_id, table_name, db_dir)
            else:
                results = _execute_code_from_builder(
                    -1, self, funct, cache, instance_id, table_name, db_dir
                )
                table_operations.update_columns(results, self.changed_columns, instance_id, table_name, db_dir)
        finally:
            table_operations.make_df(instance_id, table_name, db_dir)



def _execute_code_from_builder(
    index: int,
    builder: CodeBuilder,
    funct: Callable,
    cache: Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> Any:
    if index >= 0:
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
            table_operations.write_df_entry(result, index, builder.changed_columns[i], 
                                            instance_id,table_name,db_dir)
        
        return None
    return results
