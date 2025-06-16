from tablevault._operations import _vault_operations, _get_operations
from tablevault._helper.metadata_store import ActiveProcessDict
from tablevault._helper.utils import gen_tv_id
from tablevault._defintions import constants
import pandas as pd
from typing import Optional
import os
import tarfile
from tablevault._defintions import tv_errors
from tablevault._helper.user_lock import set_tv_lock, set_writable
import logging
import shutil
from rich.tree import Tree
from tablevault._helper.copy_write_file import CopyOnWriteFile
# from tablevault._remote_helper import remote_save_process
# import multiprocessing


class TableVault:
    """Interface with a TableVault directory.

    Initialisation can create a new vault directory and optionally restart any
    active processes. Subsequent methods allow interaction with tables,
    instances, code modules, and builder files within that vault.

    Parameters
    ----------
    db_dir : str
        Directory path where the TableVault is stored (or should be created).
    author : str
        Name or identifier of the user/system performing the operations.
    description : str, optional
        Description for the vault creation (used only when `create` is ``True``).
        Defaults to "".
    create : bool, optional
        If ``True``, initialise a new vault at `db_dir`. Defaults to ``False``.
    restart : bool, optional
        If ``True``, restart any processes previously active in this vault.
        Defaults to ``False``.
    verbose : bool, optional
        If ``True``, prints detailed logs of every operation. Defaults to ``False``.
    """

    def __init__(
        self,
        db_dir: str,
        author: str,
        description: str = "",
        create: bool = False,
        restart: bool = False,
        verbose: bool = False,
        parent_id: str = "",
    ) -> None:
        self.author = author
        self.db_dir = db_dir
        self.parent_id = parent_id
        if create:
            _vault_operations.setup_database(
                db_dir=db_dir, description=description, replace=True
            )
        elif os.path.exists(db_dir):
            if not os.path.isfile(
                os.path.join(db_dir, constants.TABLEVAULT_IDENTIFIER)
            ):
                raise tv_errors.TVArgumentError(
                    f"Path at {db_dir} is not a TableVault Repository"
                )
        else:
            raise tv_errors.TVArgumentError(f"No folder found at {db_dir}")
        self.file_writer = CopyOnWriteFile(db_dir)
        if restart:
            _vault_operations.restart_database(
                author=self.author,
                db_dir=self.db_dir,
                process_id="",
                file_writer=self.file_writer,
                parent_id=parent_id,
            )
        if verbose:
            logging.basicConfig(level=logging.INFO)
        set_tv_lock(table_name="", instance_id="", db_dir=db_dir)

    def get_process_completion(self, process_id: str) -> bool:
        """Retrieves the completion status of a specific process.

        Parameters
        ----------
        process_id : str
            Identifier of the process.

        Returns
        -------
        bool
            ``True`` if the process has completed, ``False`` otherwise.
        """
        return _get_operations.get_process_completion(
            process_id=process_id, db_dir=self.db_dir
        )

    def get_artifact_folder(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        is_temp: bool = True,
    ) -> str:
        """Retrieves the path to the artifact folder for a given table instance.
        If `allow_multiple_artifacts` is **False** for the table,
        the instance is not temporary, *and* the instance was successfully executed,
        the folder for the whole table is returned.

        Parameters
        ----------
        table_name : str
            Name of the table.
        instance_id : str, optional
            Table-instance ID. Defaults to "".
        version : str, optional
            Latest (or temporary) instance ID for the version if `instance_id`
            is not supplied. Defaults to the base version.
        is_temp : bool, optional
            If ``True`` return the path of the temporary artifact folder;
            if ``False`` return the path of the latest materialized artifact folder.
            Defaults to ``True``.

        Returns
        -------
        str
            Path to the requested artifact folder.
        """
        return _get_operations.get_artifact_folder(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            is_temp=is_temp,
            db_dir=self.db_dir,
        )

    def get_active_processes(self) -> ActiveProcessDict:
        """Retrieves a dictionary of currently active processes in this vault.

        Each key is a process ID and each value is metadata about that process.

        Returns
        -------
        ActiveProcessDict
            Mapping of process IDs to metadata.
        """
        return _get_operations.get_active_processes(db_dir=self.db_dir)

    def get_table_instances(
        self,
        table_name: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        include_temp: bool = False,
    ) -> list[str]:
        """Retrieves a list of table names, or instance IDs for a specific table and version.

        Parameters
        ----------
        table_name : str
            Name of the table whose instances are requested. If
        version : str, optional
            Version of the table. Defaults to `base` if table name is given
        include_temp : bool
            Include non-materialized instances. Defaults to False.
        Returns
        -------
        list[str]
            List of Instance IDs or table names.
        """
        return _get_operations.get_table_instances(
            table_name=table_name,
            version=version,
            db_dir=self.db_dir,
            include_temp=include_temp,
        )

    def get_descriptions(self, instance_id: str = "", table_name: str = "") -> dict:
        """Retrieves description dictionary for specified item in the database.

        If no parameters are given, return the description for the database itself.

        Parameters
        ----------
        instance_id : str, optional
            Specified instance ID. Defaults to "".
        table_name : str, optional
            Name of the table whose description is requested. Defaults to "".

        Returns
        -------
        dict
            Recorded description dictionary of the specified item.
        """
        return _get_operations.get_descriptions(
            instance_id=instance_id, table_name=table_name, db_dir=self.db_dir
        )

    def get_file_tree(
        self,
        instance_id: str = "",
        table_name: str = "",
        code_files: bool = True,
        builder_files: bool = True,
        metadata_files: bool = False,
        artifact_files: bool = False,
    ) -> Tree:
        """Retrieves a RichTree object representation of the repository.

        Parameters.
        ----------
        instance_id : str, optional
            Retrieve partial instance tree .
            Defaults to "".
        table_name : str, optional
             Retrieve partial table tree . Defaults to "".
        code_files : bool, optional
            If ``True``, include stored code modules in the tree. Defaults to ``True``.
        builder_files : bool, optional
            If ``True``, include builder files in the tree. Defaults to ``True``.
        metadata_files : bool, optional
            If ``True``, include metadata files. Defaults to ``False``.
        artifact_files : bool, optional
            If ``True``, include artifact directory contents. Defaults to ``False``.

        Returns
        -------
        rich.tree.Tree
            A printable file-tree representation.
        """
        return _get_operations.get_file_tree(
            instance_id=instance_id,
            table_name=table_name,
            code_files=code_files,
            builder_files=builder_files,
            metadata_files=metadata_files,
            artifact_files=artifact_files,
            db_dir=self.db_dir,
        )

    def get_code_modules_list(self) -> list[str]:
        """Retrieves a list of module names contained in this repository.

        Returns
        -------
        list[str]
            Python module names that have been saved to this repository.
        """
        return _get_operations.get_code_modules_list(
            db_dir=self.db_dir,
        )

    def get_builders_list(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        is_temp: bool = True,
    ) -> list[str]:
        """Retrieve a list of builder names contained in an instance.

        Parameters
        ----------
        table_name : str
            Name of the table.
        instance_id : str, optional
            ID of a specific instance. If empty, the latest instance of
            `version` that satisfies conditions is used. Defaults to "".
        version : str, optional
            Fetch the latest instance of `version` if `instance_id` is not given.
            Defaults to `BASE_TABLE_VERSION`.
        is_temp : bool, optional
            If ``True`` find the relevant temporary instance; if ``False`` find
            the latest materialized instance. Defaults to ``True``.

        Returns
        -------
        list[str]
            Python module names that have been saved to the specified instance.
        """
        return _get_operations.get_builders_list(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            is_temp=is_temp,
            db_dir=self.db_dir,
        )

    def get_builder_str(
        self,
        table_name: str,
        builder_name: str = "",
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        is_temp: bool = True,
    ) -> str:
        """Retrieve a stored builder script as plain text.

        Parameters
        ----------
        table_name : str
            Name of the table.
        builder_name : str, optional
            Name of the builder. Do not include `.yaml` extension. If empty,
            assumed as `{table_name}_index`. Defaults to "".
        instance_id : str, optional
            ID of a specific instance to fetch. If empty, the latest instance
            of `version` that satisfies conditions is used. Defaults to "".
        version : str, optional
            Fetch the latest instance of `version` if `instance_id` is not given.
            Defaults to `BASE_TABLE_VERSION`.
        is_temp : bool, optional
            If ``True`` find the relevant temporary instance; if ``False`` find
            the latest materialized instance. Defaults to ``True``.

        Returns
        -------
        str
            The contents of the `builder_name` file as a string.
        """
        return _get_operations.get_builder_str(
            instance_id=instance_id,
            builder_name=builder_name,
            table_name=table_name,
            version=version,
            is_temp=is_temp,
            db_dir=self.db_dir,
        )

    def get_code_module_str(self, module_name: str) -> str:
        """Retrieve a code module file as plain text.

        Parameters
        ----------
        module_name : str
            Name of the module. Do not include `.py` extension.

        Returns
        -------
        str
            The contents of the `module_name` file as a string.
        """
        return _get_operations.get_code_module_str(
            module_name=module_name,
            db_dir=self.db_dir,
        )

    def get_dataframe(
        self,
        table_name: str,
        instance_id: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        active_only: bool = True,
        successful_only: bool = False,
        rows: Optional[int] = None,
        full_artifact_path: bool = True,
    ) -> tuple[pd.DataFrame, str]:
        """Retrieve a pandas `DataFrame` for a table instance.

        Parameters
        ----------
        table_name : str
            Name of the table.
        instance_id : str, optional
            ID of a specific instance to fetch. If empty, the latest instance
            of `version` that satisfies conditions is used. Defaults to "".
        version : str, optional
            Fetch the latest instance of `version` if `instance_id` is not given.
            Defaults to `BASE_TABLE_VERSION`.
        active_only : bool, optional
            If ``True`` consider only active instances. Defaults to ``True``.
        successful_only : bool, optional
            If ``True`` consider only successfully executed instances.
            Defaults to ``False``.
        rows : Optional[int], optional
            If given, limit the rows fetched to this number. Defaults to ``None`` (no limit).
        full_artifact_path : bool, optional
            If ``True`` add the base folder path to all `"artifact_string"` columns.
            Defaults to ``True``.

        Returns
        -------
        tuple[pd.DataFrame, str]
            The ``DataFrame`` and the instance ID fetched.
        """
        return _get_operations.get_dataframe(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            active_only=active_only,
            successful_only=successful_only,
            rows=rows,
            full_artifact_path=full_artifact_path,
            db_dir=self.db_dir,
        )

    def stop_process(
        self,
        to_stop_process_id: str,
        force: bool = False,
        materialize: bool = False,
        process_id: str = "",
    ) -> str:
        """Stop an active process and optionally terminate it forcefully.

        Parameters
        ----------
        to_stop_process_id : str
            ID of the process to stop.
        force : bool, optional
            If ``True`` forcibly stop the running process; if ``False`` and the
            process is running, raise an exception. Defaults to ``False``.
        materialize : bool, optional
            If ``True`` materialise partial instances if relevant. Defaults to ``False``.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the `stop_process` operation.
        """
        return _vault_operations.stop_process(
            author=self.author,
            to_stop_process_id=to_stop_process_id,
            force=force,
            materialize=materialize,
            db_dir=self.db_dir,
            process_id=process_id,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def create_code_module(
        self,
        module_name: str = "",
        copy_dir: str = "",
        text: str = "",
        process_id: str = "",
    ) -> str:
        """Create or copy a Python module file into TableVault.

        Parameters
        ----------
        module_name : str, optional
            Name to assign to the new module. If empty, `copy_dir` must be
            supplied and the name is inferred from its contents. If not empty,
            the generated file will be `{module_name}.py`. Defaults to "".
        copy_dir : str, optional
            Local directory containing Python files to copy **or** a specific
            Python-file path. If empty, a new Python file is created. Defaults to "".
        text : str, optional
            Text string containing content of the module file to save.
            Is overridden by `copy_dir` if both are given. Defaults to "".
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.create_code_module(
            author=self.author,
            module_name=module_name,
            copy_dir=copy_dir,
            text=text,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def delete_code_module(self, module_name: str, process_id: str = "") -> str:
        """Delete a Python module file from the repository.

        Parameters
        ----------
        module_name : str
            Name of the module to delete (file `{module_name}.py`).
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.delete_code_module(
            author=self.author,
            module_name=module_name,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def create_builder_file(
        self,
        table_name: str,
        builder_name: str = "",
        version: str = constants.BASE_TABLE_VERSION,
        copy_dir: str = "",
        text: str = "",
        process_id: str = "",
    ) -> str:
        """Create or update a builder (YAML) file for a temporary table instance.

        If the builder content is not specified, a template file will be created.

        Parameters
        ----------
        table_name : str
            Name of the table.
        builder_name : str, optional
            File name (without path) of the builder. If empty, inferred from
            `copy_dir`. Defaults to "".
        version : str, optional
            Version of the table. Defaults to `BASE_TABLE_VERSION`.
        copy_dir : str, optional
            Local directory containing the builder file(s) to copy. Defaults to "".
        text : str, optional
            Text string containing content of the builder file to save.
            Is overridden by `copy_dir` if both are given. Defaults to "".
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.create_builder_file(
            author=self.author,
            builder_name=builder_name,
            table_name=table_name,
            version=version,
            copy_dir=copy_dir,
            text=text,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def delete_builder_file(
        self,
        builder_name: str,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        process_id: str = "",
    ) -> str:
        """Delete a builder file from a temporary table instance.

        Parameters
        ----------
        builder_name : str
            Name of the builder file to delete.
        table_name : str
            Name of the table that owns the builder.
        version : str, optional
            Version of the table. Defaults to `BASE_TABLE_VERSION`.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.delete_builder_file(
            author=self.author,
            builder_name=builder_name,
            table_name=table_name,
            version=version,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def rename_table(
        self, new_table_name: str, table_name: str, process_id: str = ""
    ) -> str:
        """Rename an existing table within the TableVault repository.

        Parameters
        ----------
        new_table_name : str
            New name for the table.
        table_name : str
            Current name of the table to rename.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.rename_table(
            author=self.author,
            new_table_name=new_table_name,
            table_name=table_name,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def delete_table(self, table_name: str, process_id: str = "") -> str:
        """Permanently delete a table and all its instances from the repository.

        Only the dataframes and artifacts are removed; table metadata is retained.

        Parameters
        ----------
        table_name : str
            Name of the table to delete.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.delete_table(
            author=self.author,
            table_name=table_name,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def delete_instance(
        self, instance_id: str, table_name: str, process_id: str = ""
    ) -> str:
        """Delete a materialised table instance from the vault.

        Only the dataframe is removed and artifacts; instance metadata is retained.

        Parameters
        ----------
        instance_id : str
            ID of the instance to delete.
        table_name : str
            Name of the table that owns the instance.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.delete_instance(
            author=self.author,
            table_name=table_name,
            instance_id=instance_id,
            process_id=process_id,
            db_dir=self.db_dir,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def write_instance(
        self,
        table_df: pd.DataFrame,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        dependencies: Optional[list[tuple[str, str]]] = None,
        dtypes: Optional[dict[str, str]] = None,
        process_id: str = "",
    ) -> str:
        """Write `table_df` as a **materialized instance** of `table_name` and `version`.

        The table must already have a **temporary instance** of the same version that
        is open for external edits (generated by `create_instance()`).

        Parameters
        ----------
        table_df : pd.DataFrame
            Data to write.
        table_name : str
            Target table.
        version : str, optional
            Target version. Defaults to `BASE_TABLE_VERSION`.
        dependencies : Optional[list[tuple[str, str]]], optional
            `[(table_name, instance_id), ...]` pairs that this instance depends on.
            Pass `None` to record no dependencies. Defaults to None.
        dtypes : Optional[dict[str, str]], optional
            `{column: pandas-dtype}`. `None` implies use of nullable defaults.
            Defaults to None.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The `process_id` that executed the write.
        """
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
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def execute_instance(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        force_execute: bool = False,
        process_id: str = "",
        background: bool = False,
    ) -> str:
        """Executes and materialise an existing temporary table instance
        from builder files.

        Parameters
        ----------
        table_name : str
            Name of the table to materialise.
        version : str, optional
            Version of the table. Defaults to `BASE_TABLE_VERSION`.
        force_execute : bool, optional
            If ``True`` force a full rebuild; if ``False`` attempt to reuse an
            origin instance when possible. Defaults to ``False``.
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".
        background : bool, optional
            If ``True`` runs in a background process. Defaults to ``False``.

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.execute_instance(
            author=self.author,
            table_name=table_name,
            version=version,
            force_execute=force_execute,
            process_id=process_id,
            db_dir=self.db_dir,
            background=background,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def create_instance(
        self,
        table_name: str,
        version: str = "",
        origin_id: str = "",
        origin_table: str = "",
        external_edit: bool = False,
        copy: bool = False,
        builders: Optional[dict[str, str] | list[str]] = None,
        description: str = "",
        process_id: str = "",
    ) -> str:
        """Create a new temporary instance of a table.

        Parameters
        ----------
        table_name : str
            Name of the table.
        version : str, optional
            Version of the table. Defaults to `BASE_TABLE_VERSION` when empty.
        origin_id : str, optional
            If supplied, copy state from an existing instance. Defaults to "".
        origin_table : str, optional
            Table associated with `origin_id`. If not given, defaults to `table_name`.
        external_edit : bool, optional
            If ``True`` this instance will be edited externally and no builder
            files are constructed. Defaults to ``False``.
        copy : bool, optional
            If ``True`` and `origin_id` is not provided, use the latest
            materialised instance of (`table_name`, `version`) as the origin
            (if it exists). Defaults to ``False``.
        builders : Optional[list[str]], optional
            List of new builder names to generate. Defaults to None.
        description : str, optional
            Description for this instance. Defaults to "".
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
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
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def create_table(
        self,
        table_name: str,
        allow_multiple_artifacts: bool = False,
        has_side_effects: bool = False,
        description: str = "",
        process_id: str = "",
    ) -> str:
        """Create a new table definition in the vault.

        Parameters
        ----------
        table_name : str
            Name of the new table.
        allow_multiple_artifacts : bool, optional
            If ``True`` each materialised instance gets its own artifact folder;
            if ``False`` only one folder is allowed and only one active instance
            at a time. Defaults to ``False``.
        has_side_effects : bool, optional
            If ``True`` builder files have side effects (e.g. external API calls).
            When a new temporary instance starts executing, all other instances
            are marked inactive. Defaults to ``False``.
        description : str, optional
            Description for the table, stored in metadata. Defaults to "".
        process_id : str, optional
            Author-generated identifier for the process. Defaults to "".

        Returns
        -------
        str
            The process ID of the executed operation.
        """
        return _vault_operations.create_table(
            author=self.author,
            table_name=table_name,
            allow_multiple_artifacts=allow_multiple_artifacts,
            has_side_effects=has_side_effects,
            process_id=process_id,
            db_dir=self.db_dir,
            description=description,
            file_writer=self.file_writer,
            parent_id=self.parent_id,
        )

    def generate_process_id(self) -> str:
        """Generate and return a unique process ID.

        If a process ID is supplied to an operation, that operation persists on
        errors and can be restarted with the same ID.

        Returns
        -------
        str
            A new, unique process identifier.
        """
        return gen_tv_id()


def compress_vault(db_dir: str, preset: int = 6) -> None:
    """Compress a TableVault directory into a `.tar.xz` archive.

    Parameters
    ----------
    db_dir : str
        Path to the TableVault directory to compress.
    preset : int, optional
        LZMA compression level ``1``-``9`` (higher is slower but smaller).
        Defaults to ``6``.

    Raises
    ------
    FileNotFoundError
        If `db_dir` does not exist or is not a directory.
    tv_errors.TVArgumentError
        If the folder at `db_dir` is not a TableVault Repository.
    """
    if not os.path.isdir(db_dir):
        raise FileNotFoundError(f"No such directory: {db_dir}")
    if not os.path.isfile(os.path.join(db_dir, constants.TABLEVAULT_IDENTIFIER)):
        raise tv_errors.TVArgumentError(
            f"Folder at {db_dir} is not a TableVault Repository"
        )
    set_writable(db_dir)

    base = os.path.basename(os.path.normpath(db_dir))
    output_tar_xz = f"{base}.tar.xz"

    with tarfile.open(output_tar_xz, mode="w:xz", preset=preset) as tar:
        # Archive the entire vault directory under its base name
        tar.add(db_dir, arcname=base)


def decompress_vault(db_dir: str) -> None:
    """Decompress a `.tar.xz` archive created by :py:func:`compress_vault`.

    Parameters
    ----------
    db_dir : str
        Path to the TableVault directory **without** the `.tar.xz` extension.
        The function looks for `{db_dir}.tar.xz`.

    Raises
    ------
    FileNotFoundError
        If the expected archive file is missing.
    tv_errors.TVArgumentError
        If the folder at `db_dir` is not a TableVault Repository after decompression.
    """
    db_dir_compressed = db_dir + ".tar.xz"
    if not os.path.isfile(db_dir_compressed):
        raise FileNotFoundError(f"No such file: {db_dir_compressed}")

    if not os.path.isdir(db_dir):
        os.makedirs(db_dir)

    with tarfile.open(db_dir_compressed, mode="r:xz") as tar:
        tar.extractall(path=db_dir)
    if not os.path.isfile(os.path.join(db_dir, constants.TABLEVAULT_IDENTIFIER)):
        raise tv_errors.TVArgumentError(
            f"Folder at {db_dir} is not a TableVault Repository"
        )
    set_tv_lock(table_name="", instance_id="", db_dir=db_dir)


def delete_vault(db_dir: str):
    """Delete a TableVault directory.

    Parameters
    ----------
    db_dir : str
        Base directory.

    Raises
    ------
    tv_errors.TVArgumentError
        If the folder at `db_dir` is not a TableVault Repository.
    """
    if not os.path.isfile(os.path.join(db_dir, constants.TABLEVAULT_IDENTIFIER)):
        raise tv_errors.TVArgumentError(
            f"Folder at {db_dir} is not a TableVault Repository"
        )
    set_writable(db_dir)
    shutil.rmtree(db_dir)
