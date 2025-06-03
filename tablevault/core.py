from tablevault._operations import _vault_operations
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


def _can_program_modify_permissions(filepath: str) -> bool:
    if os.name == "nt":
        return True
    current_euid = os.geteuid()
    if current_euid == 0:
        return True

    file_stat = os.stat(filepath)
    file_owner_uid = file_stat.st_uid
    return current_euid == file_owner_uid


class TableVault:
    """Interface with a TableVault directory.

    Initialisation can create a new vault directory and optionally restart any
    active processes.  Subsequent methods allow interaction with tables,
    instances, code modules, and builder files within that vault.

    :param str db_dir: Directory path where the TableVault is stored (or should
        be created).
    :param str author: Name or identifier of the user/system performing the
        operations.
    :param str description: Description for the vault creation (used only when
        *create* is ``True``). *Optional*.
    :param bool create: If ``True``, initialise a new vault at *db_dir*.
        Defaults to ``False``. *Optional*.
    :param bool restart: If ``True``, restart any processes previously active in
        this vault.  Defaults to ``False``. *Optional*.
    :param bool verbose: If ``True``, prints detailed logs of every operation.
        Defaults to ``False``. *Optional*.
    """

    def __init__(
        self,
        db_dir: str,
        author: str,
        description: str = "",
        create: bool = False,
        restart: bool = False,
        verbose: bool = True,
    ) -> None:
        self.db_dir = db_dir
        self.author = author

        if create:
            _vault_operations.setup_database(
                db_dir=db_dir, description=description, replace=True
            )
        else:
            # Ensure the directory exists and is writable by this user
            if os.path.isdir(db_dir):
                if not _can_program_modify_permissions(db_dir):
                    raise tv_errors.TVArgumentError(
                        f"Need ownership permission for {db_dir}"
                    )
            else:
                raise tv_errors.TVArgumentError(f"No folder found at {db_dir}")

        if restart:
            _vault_operations.restart_database(
                author=self.author, db_dir=self.db_dir, process_id=""
            )
        if verbose:
            logging.basicConfig(level=logging.INFO)
        set_tv_lock(table_name="", instance_id="", db_dir=db_dir)

    def get_process_completion(self, process_id: str) -> bool:
        """Return the completion status of a specific process.

        :param str process_id: Identifier of the process.
        :returns bool: ``True`` if the process has completed, ``False`` otherwise.
        """
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
        """Return the path to the artifact folder for a given table
        instance.

        If ``allow_multiple_artifacts`` is ``False`` for *table_name* and the instance
        is materialised, the folder for the whole table is returned.

        :param str table_name: Name of the table.
        :param str instance_id: Table-instance ID. *Optional*.
        :param str version: Latest (or temporary) instance ID for the version if
            *instance_id* is not supplied.  Defaults to the base version. *Optional*.
        :param bool is_temp: If ``True`` return the path to the temporary instance;
            if ``False`` return the path to the last materialised instance.
            *Optional*.
        :returns str: Path to the requested artifact folder.
        """
        return _vault_operations.get_artifact_folder(
            instance_id=instance_id,
            table_name=table_name,
            version=version,
            db_dir=self.db_dir,
            is_temp=is_temp,
        )

    def get_active_processes(self) -> ActiveProcessDict:
        """Return a dictionary of currently active processes in this
        vault.

        Each key is a process ID and each value is metadata about that process.

        :returns dict[str, Mapping[str, Any]]: Mapping of process IDs to metadata.
        """
        return _vault_operations.active_processes(db_dir=self.db_dir)

    def get_instances(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
    ) -> list[str]:
        """Return a list of materialised instance IDs for a specific
        table and version.

        :param str table_name: Name of the table whose instances are requested.
        :param str version: Version of the table.  Defaults to ``BASE_TABLE_VERSION``.
            *Optional*.
        :returns list[str]: Instance IDs that have been materialised for this
            table/version.
        """
        return _vault_operations.get_instances(
            table_name=table_name, db_dir=self.db_dir, version=version
        )

    def get_descriptions(self):
        """(Planned) Return descriptions or metadata for all tables.

        .. note::  This routine is not yet implemented.
        """
        raise NotImplementedError("Currently not implemented.")

    def stop_process(
        self,
        to_stop_process_id: str,
        force: bool = False,
        materialize: bool = False,
        process_id: str = "",
    ) -> str:
        """Stop an active process and optionally terminate it
        forcefully.

        :param str to_stop_process_id: ID of the process to stop.
        :param bool force: If ``True`` forcibly stop the running process; if ``False``
            and the process is running, raise an exception.  Defaults to ``False``.
        :param bool materialize: If ``True`` materialise partial instances if
            relevant.  Defaults to ``False``.
        :param str process_id: ID of the calling process (for audit/logging).
            Defaults to ``""``. *Optional*.
        :returns str: The process ID of the ``stop_process`` operation.
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
        artifact_path: bool = True,
    ) -> tuple[pd.DataFrame, str]:
        """Retrieve a pandas ``DataFrame`` for a table instance.

        :param str table_name: Name of the table.
        :param str instance_id: ID of a specific instance to fetch.  If empty, the
            latest instance of *version* is used. *Optional*.
        :param str version: Fetch the latest instance of *version* if *instance_id* is
            not given.  Defaults to ``BASE_TABLE_VERSION``. *Optional*.
        :param bool active_only: If ``True`` consider only active instances.
            Defaults to ``True``. *Optional*.
        :param bool safe_locking: If ``True`` acquire locks to prevent concurrent
            writes.  Defaults to ``True``. *Optional*.
        :param int | None rows: If given, limit the rows fetched to this number.
            Defaults to ``None`` (no limit). *Optional*.
        :param bool artifact_path: If ``True`` add the base folder path to all
            ``"artifact_string"`` columns.  Defaults to ``True``. *Optional*.
        :returns tuple[pandas.DataFrame, str]: The ``DataFrame`` and the instance ID
            fetched.
        """
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
    ) -> str:
        """Copy (or create) a code-module file or directory into the
        vault.

        :param str module_name: Name to assign to the new module.  If empty,
            *copy_dir* must be supplied and the name is inferred from its contents.
            If not empty, the generated file will be ``{module_name}.py``. *Optional*.
        :param str copy_dir: Local directory containing Python files to copy **or**
            a specific Python-file path.  If empty, a new Python file is created.
            *Optional*.
        :param str process_id: Identifier for the calling process (used for logging).
            Defaults to ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
        return _vault_operations.create_code_module(
            author=self.author,
            module_name=module_name,
            copy_dir=copy_dir,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_code_module(self, module_name: str, process_id: str = "") -> str:
        """Delete a code-module file from the vault.

        :param str module_name: Name of the module to delete (file
            ``{module_name}.py`` is searched for).
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
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
    ) -> str:
        """Add or update a builder (YAML) file for a temporary table
        instance.

        If the builder is new, its type is inferred from *builder_name*:
        ``{table_name}_index`` ⇒ *IndexBuilder*; any other name ⇒ *ColumnBuilder*.

        :param str table_name: Name of the table.
        :param str builder_name: File name (without path) of the builder.  If empty,
            inferred from *copy_dir*. *Optional*.
        :param str version: Version of the table.  Defaults to
            ``BASE_TABLE_VERSION``. *Optional*.
        :param str copy_dir: Local directory containing the builder file(s) to copy.
            *Optional*.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
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
    ) -> str:
        """Remove a builder file from a temporary table instance.

        :param str builder_name: Name of the builder file to delete.
        :param str table_name: Name of the table that owns the builder.
        :param str version: Version of the table.  Defaults to
            ``BASE_TABLE_VERSION``. *Optional*.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
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
    ) -> str:
        """Rename an existing table within the vault.

        :param str new_table_name: New name for the table.
        :param str table_name: Current name of the table to rename.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
        return _vault_operations.rename_table(
            author=self.author,
            new_table_name=new_table_name,
            table_name=table_name,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_table(self, table_name: str, process_id: str = "") -> str:
        """Permanently delete a table and all its instances from the
        vault.

        Only the dataframes are removed; table metadata is retained.

        :param str table_name: Name of the table to delete.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
        return _vault_operations.delete_table(
            author=self.author,
            table_name=table_name,
            process_id=process_id,
            db_dir=self.db_dir,
        )

    def delete_instance(
        self, instance_id: str, table_name: str, process_id: str = ""
    ) -> str:
        """Delete a materialised table instance from the vault.

        Only the dataframe is removed; instance metadata is retained.

        :param str instance_id: ID of the instance to delete.
        :param str table_name: Name of the table that owns the instance.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
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
    ) -> str:
        """Write *table_df* as a **materialized instance** of
        *table_name* and *version*.

        The table must already have a **temporary instance** of the same version that
        is open for external edits (see :py:meth:`create_instance`).

        :param pd.DataFrame table_df: Data to write.
        :param str table_name: Target table.
        :param str version: Target version.  Defaults to :pydataattr:`BASE_TABLE_VERSION`.
        :param list[tuple[str, str]] dependencies:
            ``[(table_name, instance_id), …]`` pairs that this instance depends on.
            Pass *None* to record no dependencies.
        :param dict[str, str] dtypes:
            ``{column: pandas-dtype}``.  *None* ⇒ use nullable defaults.
        :param str process_id: Identifier for the calling process.  Empty for default.
        :returns str: The *process_id* that executed the write.
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
        )

    def execute_instance(
        self,
        table_name: str,
        version: str = constants.BASE_TABLE_VERSION,
        force_execute: bool = False,
        process_id: str = "",
        background: bool = False,
    ) -> str:
        """Materialise an existing temporary table instance.

        :param str table_name: Name of the table to materialise.
        :param str version: Version of the table.  Defaults to
            ``BASE_TABLE_VERSION``. *Optional*.
        :param bool force_execute: If ``True`` force a full rebuild; if ``False``
            attempt to reuse an origin instance when possible.  Defaults to
            ``False``. *Optional*.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :param bool background: If ``True`` run materialisation in a background
            process.  Defaults to ``False``. *Optional*.
        :returns str: The process ID of the executed operation.
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
    ) -> str:
        """Create a new temporary instance of a table.

        At most one of *origin_id* or *builders* should be supplied.

        :param str table_name: Name of the table.
        :param str version: Version of the table.  Defaults to
            ``BASE_TABLE_VERSION`` when empty. *Optional*.
        :param str origin_id: If supplied, copy state from an existing instance.
            *Optional*.
        :param str origin_table: Table associated with *origin_id*.  If not given,
            defaults to *table_name*. *Optional*.
        :param bool external_edit: If ``True`` this instance will be edited
            externally and no builder files are constructed.  Defaults to ``False``.
            *Optional*.
        :param bool copy: If ``True`` and *origin_id* is not provided, use the latest
            materialised instance of (*table_name*, *version*) as the origin (if it
            exists).  Defaults to ``True``. *Optional*.
        :param list[str] builders: List of new builder names to generate. *Optional*.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :param str description: Description for this instance.  Defaults to
            ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
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
        process_id: str = "",
        description: str = "",
    ) -> str:
        """Create a new table definition in the vault.

        :param str table_name: Name of the new table.
        :param bool allow_multiple_artifacts: If ``True`` each materialised instance
            gets its own artifact folder; if ``False`` only one folder is allowed and
            only one active instance at a time. *Optional*.
        :param bool has_side_effects: If ``True`` builder files have side effects
            (e.g. external API calls).  When a new temporary instance starts
            executing, all other instances are marked inactive.  Defaults to
            ``False``. *Optional*.
        :param str process_id: Identifier for the calling process.  Defaults to
            ``""``. *Optional*.
        :param str description: Description for the table, stored in metadata.
            Defaults to ``""``. *Optional*.
        :returns str: The process ID of the executed operation.
        """
        return _vault_operations.create_table(
            author=self.author,
            table_name=table_name,
            allow_multiple_artifacts=allow_multiple_artifacts,
            has_side_effects=has_side_effects,
            process_id=process_id,
            db_dir=self.db_dir,
            description=description,
        )

    def generate_process_id(self) -> str:
        """Generate and return a unique process ID.

        If a process ID is supplied to an operation, that operation persists on errors
        and can be restarted with the same ID.

        :returns str: A new, unique process identifier.
        """
        return gen_tv_id()


def compress_vault(db_dir: str, preset: int = 6) -> None:
    """Compress a TableVault directory into a ``.tar.xz`` archive.

    :param str db_dir: Path to the TableVault directory to compress.
    :param int preset: LZMA compression level ``1``–``9`` (higher is slower but
        smaller).  Defaults to ``6``. *Optional*.
    :raises FileNotFoundError: If *db_dir* does not exist or is not a directory.
    """
    if not os.path.isdir(db_dir):
        raise FileNotFoundError(f"No such directory: {db_dir}")

    base = os.path.basename(os.path.normpath(db_dir))
    output_tar_xz = f"{base}.tar.xz"

    with tarfile.open(output_tar_xz, mode="w:xz", preset=preset) as tar:
        # Archive the entire vault directory under its base name
        tar.add(db_dir, arcname=base)


def decompress_vault(db_dir: str) -> None:
    """Decompress a ``.tar.xz`` archive created by
    :pyfunc:`compress_vault`.

    :param str db_dir: Path to the TableVault directory **without** the
        ``.tar.xz`` extension.  The function looks for ``{db_dir}.tar.xz``.
    :raises FileNotFoundError: If the expected archive file is missing.
    """
    db_dir_compressed = db_dir + ".tar.xz"
    if not os.path.isfile(db_dir_compressed):
        raise FileNotFoundError(f"No such file: {db_dir_compressed}")

    base = os.path.basename(db_dir_compressed)[:-7]  # strip ".tar.xz"
    extract_to = base

    if not os.path.isdir(extract_to):
        os.makedirs(extract_to)

    with tarfile.open(db_dir_compressed, mode="r:xz") as tar:
        tar.extractall(path=extract_to)


def delete_vault(db_dir: str):
    """Delete a TableVault directory

    :param str db_dir: Base directory.
    """
    set_writable(db_dir)
    shutil.rmtree(db_dir)
