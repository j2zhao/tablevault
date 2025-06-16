from tablevault._builders.base_builder_type import TVBuilder
from tablevault._dataframe_helper import table_operations
from tablevault._defintions.types import Cache
from tablevault._helper import utils
from tablevault._defintions import constants, tv_errors
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional
from tablevault._helper.file_operations import load_code_function, move_code_to_instance
from tablevault._helper.copy_write_file import CopyOnWriteFile


class ColumnBuilder(TVBuilder):
    def execute(
        self,
        cache: Cache,
        instance_id: str,
        table_name: str,
        db_dir: str,
        process_id: str,
        file_writer: CopyOnWriteFile,
    ) -> None:
        try:
            self.transform_table_string(
                cache, instance_id, table_name, db_dir, process_id, index=None
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
                for i in range(len(cache[constants.TABLE_SELF])):
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
                _execute_code_from_builder(
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
            table_operations.make_df(
                instance_id, table_name, db_dir, file_writer=file_writer
            )


def _execute_code_from_builder(
    index: Optional[None],
    builder: ColumnBuilder,
    funct: Callable,
    cache: Cache,
    instance_id: str,
    table_name: str,
    db_dir: str,
    process_id: str,
    file_writer: CopyOnWriteFile,
) -> None:
    if index is not None:
        is_filled = table_operations.check_entry(
            index, builder.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return
    else:
        is_filled = table_operations.check_entry(
            index, builder.changed_columns, cache[constants.TABLE_SELF]
        )
        if is_filled:
            return
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
            cache[constants.TABLE_SELF].at[index, builder.changed_columns[0]] = results
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
                cache[constants.TABLE_SELF].at[index, builder.changed_columns[i]] = (
                    result
                )
    elif builder.return_type == constants.BUILDER_RTYPE_GENERATOR:
        for index, results_ in enumerate(results):
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
                cache[constants.TABLE_SELF].at[index, builder.changed_columns[0]] = (
                    results_
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
                    cache[constants.TABLE_SELF].at[
                        index, builder.changed_columns[i]
                    ] = result
    elif builder.return_type == constants.BUILDER_RTYPE_DATAFRAME:
        table_operations.save_new_columns(
            results,
            builder.changed_columns,
            instance_id,
            table_name,
            db_dir,
            file_writer=file_writer,
        )
    else:
        raise tv_errors.TVBuilderError("return_type not recognized")
