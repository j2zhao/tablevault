from tablevault._operations import _vault_operations
from tablevault._helper.metadata_store import ActiveProcessDict
from tablevault._helper.utils import gen_tv_id
from tablevault._defintions import constants
import pandas as pd
from typing import Optional
import os
import tarfile
from tablevault._defintions import tv_errors
from tablevault._helper.user_lock import set_tv_lock


def can_program_modify_permissions(filepath):
    current_euid = os.geteuid()
    if current_euid == 0:
        return True
    file_stat = os.stat(filepath)
    file_owner_uid = file_stat.st_uid
    if current_euid == file_owner_uid:
        return True
    else:
        return False


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
    ) -> None:
        self.db_dir = db_dir
        self.author = author
        if create:
            _vault_operations.setup_database(
                db_dir=db_dir, description=description, replace=True
            )
        else:
            if os.path.isdir(db_dir):
                if not can_program_modify_permissions(db_dir):
                    raise tv_errors.TVArgumentError(
                        f"Need Ownership Permission for {db_dir}"
                    )
            else:
                raise tv_errors.TVArgumentError(f"No Folder Found at {db_dir}")
        if restart:
            _vault_operations.restart_database(
                author=self.author, db_dir=self.db_dir, process_id=""
            )
        set_tv_lock("", "", db_dir)

    def get_process_completion(self, process_id: str) -> bool:
        return _vault_operations.complete_process(
            process_id=process_id, db_dir=self.db_dir
        )

    def get_artifact_folder(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        is_temp: bool = True,
    ) -> str:
        return _vault_operations.get_artifact_folder(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            db_dir=self.db_dir,
            is_temp=is_temp,
        )

    def get_active_processes(self) -> ActiveProcessDict:
        """
        Return a dictionary of currently active processes.
        """
        return _vault_operations.active_processes(db_dir=self.db_dir)

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
        return _vault_operations.get_instances(
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
        return _vault_operations.stop_process(
            author=self.author,
            to_stop_process_id=to_stop_process_id,
            force=force,
            materialize=materialize,
            db_dir=self.db_dir,
            process_id=process_id,
        )

    def get_dataframe(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        active_only: bool = True,
        safe_locking: bool = True,
        rows: Optional[int] = None,
        artifact_path=True,
    ) -> tuple[pd.DataFrame, str]:
        return _vault_operations.get_table(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            db_dir=self.db_dir,
            active_only=active_only,
            safe_locking=safe_locking,
            rows=rows,
            artifact_path=artifact_path,
        )

    def create_code_module(
        self, module_name: str = "", copy_dir: str = "", process_id: str = ""
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
        return _vault_operations.create_code_module(
            author=self.author,
            module_name=module_name,
            copy_dir=copy_dir,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_code_module(self, module_name: str, process_id: str = "") -> None:
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
        return _vault_operations.delete_code_module(
            author=self.author,
            module_name=module_name,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def create_builder_file(
        self,
        table_name: str,
        builder_name: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        copy_dir: str = "",
        process_id: str = "",
    ):
        return _vault_operations.create_builder_file(
            self.author,
            builder_name=builder_name,
            table_name=table_name,
            version=version,
            copy_dir=copy_dir,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_builder_file(
        self,
        builder_name: str,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        process_id: str = "",
    ):
        return _vault_operations.delete_builder_file(
            self.author,
            builder_name=builder_name,
            table_name=table_name,
            version=version,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def rename_table(
        self, new_table_name: str, table_name: str, process_id: str = ""
    ):  # TODO
        return _vault_operations.rename_table(
            author=self.author,
            new_table_name=new_table_name,
            table_name=table_name,
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

    def write_instance(
        self,
        table_df: pd.DataFrame,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        dependencies: Optional[list[tuple[str, str]]] = None,
        dtypes: Optional[dict[str, str]] = None,
        process_id: str = "",
    ):
        if dependencies is None:
            dependencies = []
        if dtypes is None:
            dtypes = {}
        return _vault_operations.write_instance(
            author=self.author,
            table_df=table_df,
            table_name=table_name,
            version=version,
            dependencies=dependencies,
            dtypes=dtypes,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def execute_instance(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
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

    def create_instance(
        self,
        table_name: str,
        version: str = "",
        origin_id: str = "",
        origin_table: str = "",
        external_edit: bool = False,
        copy: bool = True,
        builders: Optional[dict[str, str] | list[str]] = None,
        process_id: str = "",
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
        if builders is None:
            builders = []
        return _vault_operations.create_instance(
            author=self.author,
            table_name=table_name,
            version=version,
            description=description,
            origin_id=origin_id,
            origin_table=origin_table,
            external_edit=external_edit,
            copy=copy,
            builder_names=builders,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def create_table(
        self,
        table_name: str,
        allow_multiple_artifacts: bool = False,
        has_side_effects: bool = False,
        yaml_dir: str = "",
        process_id: str = "",
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
        return _vault_operations.create_table(
            author=self.author,
            table_name=table_name,
            yaml_dir=yaml_dir,
            allow_multiple_artifacts=allow_multiple_artifacts,
            has_side_effects=has_side_effects,
            process_id=process_id,
            db_dir=self.db_dir,
            description=description,
        )

    def generate_process_id(self) -> str:
        """
        Generates new valid process id.

        Returns:
             New process id (str)

        """
        return gen_tv_id()


def compress_vault(db_dir: str, preset: int = 6) -> None:
    # Ensure folder exists
    if not os.path.isdir(db_dir):
        raise FileNotFoundError(f"No such directory: {db_dir}")

    # Build output name in cwd
    base = os.path.basename(os.path.normpath(db_dir))
    output_tar_xz = f"{base}.tar.xz"

    with tarfile.open(output_tar_xz, mode="w:xz", preset=preset) as tar:
        tar.add(db_dir, arcname=base)
    print(f"Compressed {db_dir!r} → {output_tar_xz!r}")


def decompress_vault(db_dir: str) -> None:
    db_dir_compressed = db_dir + ".tar.xz"
    if not os.path.isfile(db_dir_compressed):
        raise FileNotFoundError(f"No such file: {db_dir_compressed}")

    base = os.path.basename(db_dir_compressed)[:-7]
    extract_to = base

    if not os.path.isdir(extract_to):
        os.makedirs(extract_to)

    with tarfile.open(db_dir_compressed, mode="r:xz") as tar:
        tar.extractall(path=extract_to)
