from tablevault import _vault_operations
from tablevault.helper.metadata_store import ActiveProcessDict
from tablevault.helper.utils import gen_tv_id
from tablevault.defintions import constants
import pandas as pd
from typing import Optional

class TableVault:
    """A TableVault object that interfaces with a TableVault directory.

    Initialization optionally creates the directory, copies information from
    different folders and executes tables.

    Args:
        db_dir (str): The directory where the database files are stored.

        author (str): The name of the user or system initiating the operation.

        create (bool, optional): If True, a new database will be created at db_dir.
        Defaults to False.

        restart (bool, optional): If True, the current active processes will be
        restarted. Defaults to False.

        allow_multiple_tables (list[str], optional): A list of tables that are
        allowed to have multiple valid versions. Defaults to an empty list.

        yaml_dir (str, optional): The directory containing YAML configuration files
        for different tables. Defaults to an empty string.

        code_dir (str, optional): The directory containing code functions to execute.
        Defaults to an empty string.

        execute (bool, optional): If True, materializes the tables. Defaults to False.

    """

    def __init__(
        self,
        db_dir: str,
        author: str,
        description: str = "",
        create: bool = False,
        restart: bool = False,
        allow_multiple_artifacts: list[str] = [],
        has_side_effects: list[str] = [],
        yaml_dir: str = "",
        code_dir: str = "",
        execute: bool = False,
        background_execute: bool = False,
    ) -> None:
        self.db_dir = db_dir
        self.author = author
        if create:
            _vault_operations.setup_database(
                db_dir=db_dir, description=description, replace=True
            )
            if yaml_dir != "" or code_dir != "":
                _vault_operations.copy_database_files(
                    author=self.author,
                    yaml_dir=yaml_dir,
                    code_dir=code_dir,
                    execute=execute,
                    allow_multiple_artifacts=allow_multiple_artifacts,
                    has_side_effects=has_side_effects,
                    process_id="",
                    db_dir=db_dir,
                    background_execute=background_execute,
                )
        elif restart:
            _vault_operations.restart_database(
                author=self.author, db_dir=self.db_dir, process_id=""
            )
        return None

    def get_process_completion(self, process_id: str) -> bool:
        return _vault_operations.complete_process(process_id, self.db_dir)

    def get_artifact_folder(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        is_temp: bool = True,
    ) -> str:
        return _vault_operations.get_artifact_folder(
            instance_id, table_name, version, self.db_dir, is_temp
        )

    def get_active_processes(self) -> ActiveProcessDict:
        """
        Return a dictionary of currently active processes.

        """
        return _vault_operations.active_processes(self.db_dir)

    def get_instances(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
    ) -> list[str]:
        """
        Return a list of materialized instance names for a table.

        Args:
            table_name (str): Table name.

            version (str): Specify a version of the table. Defaults
            to empty string.

        """
        return _vault_operations.list_instances(
            table_name=table_name, db_dir=self.db_dir, version=version
        )

    def get_descriptions():
        "TODO: IMPLEMENT"
        pass

    def stop_process(
        self,
        to_stop_process_id: str,
        force: bool = False,
        materialize: bool = False,
        process_id: str = "",
    ) -> str:
        """
        Stop a currently active process and release all of its locks.

        Args:
            process_id (str): Active process ID.
            force (bool): If True, actively running process stopped. If False,
            raises exception on actively running process. Defauts to False.

        """

        # THIS CHANGES STATE -> NEEDS LOGGING
        return _vault_operations.stop_process(
            author=self.author,
            to_stop_process_id=to_stop_process_id,
            force=force,
            materialize=materialize,
            db_dir=self.db_dir,
            process_id=process_id,
        )

    def get_table(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        active_only: bool = True,
        safe_locking: bool = True,
        rows: Optional[int] = None
    ) -> pd.DataFrame:
        return _vault_operations.get_table(
            instance_id, table_name, version, self.db_dir, active_only, safe_locking, rows
        )

    def copy_files(
        self, file_dir: str, table_name: str = "", process_id: str = ""
    ) -> None:
        """
        Copy builder files into a table.

        Args:
            prompt_dir (str): Directory of files or individual
            file directory.

            table_name (str): Optional, table name to copy into. If filled, copies
            into prompt directory of table. Otherwise, copies Python files into
            code directory. Defaults to empty string.

            process_id (str): Optional, a identifier for the process (to re-execute).
            Defaults to empty string.

        """
        return _vault_operations.copy_files(
            author=self.author,
            table_name=table_name,
            file_dir=file_dir,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_table(self, table_name: str, process_id: str = "") -> None:
        """
        Delete a table.

        Args:
            table_name (str): Table name to delete.

            process_id (str): Optional, a identifier for the process (to re-execute).
            Defaults to empty string.

        """
        return _vault_operations.delete_table(
            author=self.author,
            table_name=table_name,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_instance(
        self, instance_id: str, table_name: str, process_id: str = ""
    ) -> None:
        """
        Delete a materialized table instance.

        Args:
            instance_id (str): Instance identifier.

            table_name (str): Table name.

            process_id (str): Optional, a identifier for the process (to re-execute).
            Defaults to empty string.

        """
        return _vault_operations.delete_instance(
            author=self.author,
            table_name=table_name,
            instance_id=instance_id,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def materialize_instance(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        dependencies: list[tuple[str, str]] = [],
        artifact_columns: list[str] = [],
        process_id: str = "",
    ):
        return _vault_operations.materialize_instance(
            self.author,
            "",
            table_name,
            version,
            "",
            "",
            "",
            artifact_columns,
            [],
            [],
            dependencies,
            process_id,
            self.db_dir,
        )

    def write_table(
        self,
        table_df: pd.DataFrame,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        dependencies: list[tuple[str, str]] = [],
        artifact_columns: list[str] = [],
        process_id: str = "",
    ):
        return _vault_operations.write_table(
            author=self.author,
            table_df=table_df,
            table_name=table_name,
            version=version,
            dependencies=dependencies,
            artifact_columns=artifact_columns,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def execute_instance(
        self,
        table_name: str,
        version: str = "",
        force_execute: bool = False,
        process_id: str = "",
        background: bool = False,
    ) -> None:
        """
        Materialize a table instance from YAML prompts.

        Args:
            table_name (str): Table name.

            version (str): Version of the table. Defaults to empty string.
            On default, if table has versions, defaults to "base".

            force_execute (bool): If True, completely execute the whole table.
            Otherwise, copies table from previous version.

            process_id (str): Optional, a identifier for the process (to re-execute).
            Defaults to empty string.

            background (bool): Optional, whether to execute the operation in a
            background process. Defaults to False.
        """
        return _vault_operations.execute_instance(
            author=self.author,
            table_name=table_name,
            version=version,
            force_execute=force_execute,
            process_id=process_id,
            db_dir=self.db_dir,
            background=background,
        )

    def setup_temp_instance(
        self,
        table_name: str,
        version: str = "",
        origin_id: str = "",
        origin_table: str = "",
        external_edit: bool = False,
        copy_version: bool = False,
        builder_names: list[str] = [],
        execute: bool = False,
        process_id: str = "",
        background_execute: bool = False,
        description: str = "",
    ) -> None:
        """
        Setup temporary instance to execute.

        Note: Only at most one of prev_id, copy_previous, prompts
        should be given.

        Args:
            table_name (str): Table name.

            version (str): Version of the table. Defaults to empty string.
            On default, if table has versions, defaults to "base".

            prev_id (str): Optional, previous instance ID. If given, the
            prompts are copied. Defaults to empty string.

            copy_previous (bool): If true, copy prompts from latest materialized
            table of same version. Defaults to false.

            prompt_names (list[str]): If given, copies list of prompts from table
            prompts. Defaults to empty list.

            execute (bool): If True, executes instance after creation. Defaults to
            False.

            process_id (str): Optional, a identifier for the process (to re-execute).
            Defaults to empty string.


        """
        return _vault_operations.setup_temp_instance(
            self.author,
            table_name=table_name,
            version=version,
            description=description,
            origin_id=origin_id,
            origin_table=origin_table,
            external_edit=external_edit,
            copy_version=copy_version,
            builder_names=builder_names,
            execute=execute,
            process_id=process_id,
            db_dir=self.db_dir,
            background_execute=background_execute,
        )

    def setup_table(
        self,
        table_name: str,
        create_temp: bool = False,
        execute: bool = False,
        allow_multiple_artifacts: bool = False,
        has_side_effects: bool = False,
        yaml_dir: str = "",
        process_id: str = "",
        background_execute: bool = False,
        description="",
    ) -> None:
        """
        Setup new table.

        Args:
             table_name (str): Table name.

             execute (bool): If True, executes instance after creation.

             create_temp (bool): If True, copies all prompts from table prompts
             to create temporary instance.

             allow_multiple (bool): If True, table is allowed multiple versions and
             multiple valid instances.

             prompt_dir (str): Optional, a directory of prompts to copy from.

             process_id (str): Optional, a identifier for the process (to re-execute).
             Defaults to empty string.

        """
        return _vault_operations.setup_table(
            author=self.author,
            table_name=table_name,
            yaml_dir=yaml_dir,
            create_temp=create_temp,
            execute=execute,
            allow_multiple_artifacts=allow_multiple_artifacts,
            has_side_effects=has_side_effects,
            process_id=process_id,
            db_dir=self.db_dir,
            background_execute=background_execute,
            description=description,
        )

    def generate_process_id(self) -> str:
        """
        Generates new valid process id.

        Returns:
             New process id (str)

        """
        return gen_tv_id()
