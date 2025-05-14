from pydantic import Field, BaseModel
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.prompts.utils.table_string import apply_table_string
from tablevault.defintions import constants
from tablevault.helper import file_operations
from typing import Any, Optional, Union
from tablevault.prompts.utils.table_string import TableReference

class GeneratorPrompt(TVPrompt):
    is_custom: Union[bool, TableReference] = Field(description="Custom to database.")
    code_module: Union[str, TableReference] = Field(description="Module of function.") 
    python_function: Union[str, TableReference] = Field(description="Function to execute.")
    arguments: Union[bool, TableReference] = Field(description="Function Arguments. DataTable and TableStrings are valid.")
    
    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
    ) -> bool:
        self.transform_table_string(cache, instance_id, table_name, db_dir)
        if not self.is_custom:
            funct = utils.get_function_from_module(
                self.code_module, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_module, self.python_function, db_dir)
        results = funct(**self.arguments)
        merged_df, diff_flag = table_operations.merge_columns(
            self.changed_columns, results, cache[constants.OUTPUT_SELF]
        )
        if diff_flag:
            table_operations.write_table(merged_df, instance_id, table_name, db_dir)
            return True
        else:
            return False
        

        
    