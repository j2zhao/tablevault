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
from tablevault._helper.copy_write_file import CopyOnWriteFile


class IndexBuilder(TVBuilder):
    primary_key: Union[TableReference, list[Union[str, TableReference]]] = Field(
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
        file_writer: CopyOnWriteFile,
    ) -> bool:
        diff_flag = None
        try:
            self.transform_table_string(
                cache, instance_id, table_name, db_dir, process_id, index=None
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
                move_code_to_instance(
                    self.code_module, instance_id, table_name, db_dir, file_writer
                )
                funct, _ = load_code_function(
                    self.python_function,
                    self.code_module,
                    db_dir,
                    file_writer,
                    instance_id,
                    table_name,
                )
            if (
                self.return_type == constants.BUILDER_RTYPE_ROWWISE
                and self.n_threads != 1
            ):
                # temp = cache[constants.TABLE_SELF].dropna(subset=self.changed_columns, how='all')
                # if len(temp) == 0:
                #     return False
                indices = list(range(len(cache[constants.TABLE_SELF])))
                with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                    _ = list(
                        executor.map(
                            lambda i: _execute_code_from_builder(
                                i,
                                self,
                                funct,
                                cache,
                                instance_id,
                                table_name,
                                db_dir,
                                process_id,
                                file_writer,
                            ),
                            indices,
                        )
                    )
            elif (
                self.return_type == constants.BUILDER_RTYPE_ROWWISE
                and self.n_threads == 1
            ):
                # temp = cache[constants.TABLE_SELF].dropna(subset=self.changed_columns, how='all')
                # if len(temp) == 0:
                #     return False
                for i in list(range(len(cache[constants.TABLE_SELF]))):
                    _execute_code_from_builder(
                        i,
                        self,
                        funct,
                        cache,
                        instance_id,
                        table_name,
                        db_dir,
                        process_id,
                        file_writer=file_writer,
                    )
            else:
                diff_flag = _execute_code_from_builder(
                    None,
                    self,
                    funct,
                    cache,
                    instance_id,
                    table_name,
                    db_dir,
                    process_id,
                    file_writer=file_writer,
                )
        finally:
            output = table_operations.make_df(
                instance_id,
                table_name,
                db_dir,
                file_writer=file_writer,
                primary_key=self.primary_key,
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
    process_id: str,
    file_writer: CopyOnWriteFile,
) -> None:
    builder = builder.model_copy(deep=True)
    builder.transform_table_string(
        cache, instance_id, table_name, db_dir, process_id, index, arguments=True
    )
    results = funct(**builder.arguments)
    if index is not None and builder.return_type == constants.BUILDER_RTYPE_ROWWISE:
        if len(builder.changed_columns) == 1:
            table_operations.write_df_entry(
                results,
                index,
                builder.changed_columns[0],
                instance_id,
                table_name,
                db_dir,
                file_writer,
            )
        else:
            for i, result in enumerate(results):
                table_operations.write_df_entry(
                    result,
                    index,
                    builder.changed_columns[i],
                    instance_id,
                    table_name,
                    db_dir,
                    file_writer,
                )
    elif builder.return_type == constants.BUILDER_RTYPE_GENERATOR:
        count = 0
        for index, results_ in enumerate(results):
            count += 1
            if len(builder.changed_columns) == 1:
                table_operations.write_df_entry(
                    results_,
                    index,
                    builder.changed_columns[0],
                    instance_id,
                    table_name,
                    db_dir,
                    file_writer,
                )
            else:
                for i, result in enumerate(results_):
                    table_operations.write_df_entry(
                        result,
                        index,
                        builder.changed_columns[i],
                        instance_id,
                        table_name,
                        db_dir,
                        file_writer,
                    )
        # if count == 0:
        #     return False
    elif builder.return_type == constants.BUILDER_RTYPE_DATAFRAME:
        # if len(results) == 0:
        #     return False
        diff_flag = table_operations.save_new_columns(
            results,
            builder.changed_columns,
            instance_id,
            table_name,
            db_dir,
            file_writer=file_writer,
            primary_key=builder.primary_key,
        )
        return diff_flag
    else:
        raise tv_errors.TVBuilderError("return_type not recognized")
