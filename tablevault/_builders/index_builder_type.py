from pydantic import Field
from tablevault._builders.base_builder_type import TVBuilder
from tablevault._dataframe_helper import table_operations
from tablevault._defintions.types import Cache
from tablevault._helper import utils
from tablevault._defintions import constants
from typing import Union, Callable, Optional
from tablevault._table_reference.table_reference import TableReference
from tablevault._helper.file_operations import load_code_function, move_code_to_instance
from concurrent.futures import ThreadPoolExecutor
from tablevault._defintions import tv_errors


class IndexBuilder(TVBuilder):
    primary_key: list[Union[str, TableReference]] = Field(
        default=[],
        description="Specifies the output column names that index the table.",
    )
    keep_old: Union[bool, TableReference] = Field(
        default=False, description="Keep previous dataframe rows."
    )

    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
        process_id: str,
    ) -> bool:
        diff_flag = None
        try:
            self.transform_table_string(
                cache, instance_id, table_name, db_dir, index=None
            )
            if not set(self.primary_key).issubset(self.changed_columns):
                raise tv_errors.TVArgumentError(
                    "primary key needs to be included in changed columns"
                )
            if not self.is_custom:
                funct = utils.get_function_from_module(
                    self.code_module, self.python_function
                )
            else:
                move_code_to_instance(self.code_module, instance_id, table_name, db_dir)
                funct, _ = load_code_function(
                    self.python_function,
                    self.code_module,
                    db_dir,
                    instance_id,
                    table_name,
                )
            if self.row_wise and self.n_threads != 1:
                indices = list(range(len(cache[constants.TABLE_SELF])))
                with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                    _ = list(
                        executor.map(
                            lambda i: _execute_code_from_builder(
                                i, self, funct, cache, instance_id, table_name, db_dir
                            ),
                            indices,
                        )
                    )
            elif self.row_wise and self.n_threads == 1:
                for i in list(range(len(cache[constants.TABLE_SELF]))):
                    _execute_code_from_builder(
                        i, self, funct, cache, instance_id, table_name, db_dir
                    )
            else:
                diff_flag = _execute_code_from_builder(
                    None, self, funct, cache, instance_id, table_name, db_dir
                )
        finally:
            output = table_operations.make_df(
                instance_id,
                table_name,
                db_dir,
                primary_key=self.primary_key,
                keep_old=self.keep_old,
            )
            if diff_flag is None:
                diff_flag = output
        return diff_flag


def _execute_code_from_builder(
    index: Optional[int],
    builder: IndexBuilder,
    funct: Callable,
    cache: Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    builder = builder.model_copy(deep=True)
    builder.transform_table_string(cache, instance_id, table_name, db_dir, index)
    results = funct(**builder.arguments)

    if index is not None:
        for i, result in enumerate(results):
            table_operations.write_df_entry(
                result,
                index,
                builder.changed_columns[i],
                instance_id,
                table_name,
                db_dir,
            )
        return None
    else:
        diff_flag = table_operations.save_new_columns(
            results,
            builder.changed_columns,
            instance_id,
            table_name,
            db_dir,
            primary_key=builder.primary_key,
            keep_old=builder.keep_old,
        )
        return diff_flag
