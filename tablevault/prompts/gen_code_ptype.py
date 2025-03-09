from pydantic import Field
from tablevault.prompts.base_ptype import TVPrompt
from tablevault.defintions.types import Cache
from tablevault.prompts.utils import utils, table_operations
from tablevault.prompts.table_string import parse_table_string

from tablevault.defintions import constants
from typing import Any

class GeneratorPrompt(TVPrompt):
    is_global: bool
    code_file: str
    python_function: str
    arguments: dict[str, Any]

    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
    ) -> bool:
        if self.is_global:
            code_file = self.code_file.split(".")[0]
            funct = utils.get_function_from_module(
                code_file, self.python_function
            )
        else:
            funct, _ = utils.load_function_from_file(self.code_file, self.python_function, db_dir)

        kwargs = {}
        kwargs = parse_table_string(self.arguments, cache)
        results = funct(**kwargs)
        merged_df, changed = table_operations.merge_columns(
            self[self.changed_columns], results, cache[constants.TABLE_SELF]
        )
        if changed:
            cache[constants.TABLE_SELF] = merged_df
            table_operations.write_table(merged_df, instance_id, table_name, db_dir)
            return True
        else:
            return False