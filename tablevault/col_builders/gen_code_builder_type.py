from pydantic import Field
from tablevault.col_builders.base_builder_type import TVBuilder
from tablevault.defintions.types import Cache
from tablevault.col_builders.utils import utils, table_operations
from tablevault.defintions import constants
from typing import Any, Union
from tablevault.col_builders.utils.table_string import TableReference


class GeneratorBuilder(TVBuilder):
    is_custom: Union[bool, TableReference] = Field(description="Custom to database.")
    code_module: Union[str, TableReference] = Field(description="Module of function.")
    python_function: Union[str, TableReference] = Field(
        description="Function to execute."
    )
    arguments: dict[Union[str, TableReference], Any] = Field(
        description="Function Arguments. DataTable and TableStrings are valid."
    )

    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
        process_id: str,
    ) -> bool:
        self.transform_table_string(cache, instance_id, table_name, db_dir)
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(
                self.code_module, self.python_function, db_dir
            )

        results = funct(**self.arguments)
        merged_df, diff_flag = table_operations.merge_columns(
            self.changed_columns, results, cache[constants.OUTPUT_SELF]
        )
        if diff_flag:
            table_operations.write_table(merged_df, instance_id, table_name, db_dir)
            return True
        else:
            return False
