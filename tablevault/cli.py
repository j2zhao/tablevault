# tablevault_cli.py
"""
TableVault command-line interface.

Run 'tablevault --help' for usage.
"""

import json
import sys
from pathlib import Path
from typing import Optional, Tuple

import click
import pandas as pd

# --- TableVault imports --------------------------------------------------------
from tablevault.core import (
    TableVault,
    compress_vault,
    decompress_vault,
    delete_vault,
)
from tablevault._defintions import tv_errors, constants

# --- Helpers -------------------------------------------------------------------


def _echo(obj):
    """Pretty-print obj for easy shell capture."""
    if isinstance(obj, (dict, list)):
        click.echo(json.dumps(obj, indent=2, default=str))
    else:
        click.echo(str(obj))


def _bail(ctx: click.Context, param: str):
    """Exit with an error for a missing required option."""
    ctx.fail(
        f"Missing required option `{param}` – supply it via `--{param.replace('_', '-')}`"
    )


# --- Main CLI group ------------------------------------------------------------


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--db-dir",
    type=click.Path(path_type=Path),
    help="Path to the TableVault directory.",
)
@click.option("--author", type=str, help="Author name for logging.")
@click.pass_context
def cli(ctx: click.Context, db_dir: Optional[Path], author: Optional[str]):
    """Command-line interface for TableVault operations."""
    ctx.ensure_object(dict)
    ctx.obj["db_dir"] = db_dir
    ctx.obj["author"] = author


# --- Lazy TableVault instantiation ---------------------------------------------


def _get_vault(ctx: click.Context, verbose: bool = True) -> TableVault:
    """Gets a TableVault instance from the context."""
    db_dir: Optional[Path] = ctx.obj.get("db_dir")
    author: Optional[str] = ctx.obj.get("author")

    if db_dir is None:
        _bail(ctx, "db_dir")
    if author is None:
        _bail(ctx, "author")

    return TableVault(db_dir=str(db_dir), author=author, verbose=verbose)


# --- Vault-independent utilities (archive / delete) ----------------------------


@cli.command("compress")
@click.argument("db_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--preset", default=6, show_default=True, help="LZMA compression level (1-9)."
)
def compress_cmd(db_dir: Path, preset: int):
    """Compress a vault directory into a .tar.xz archive."""
    compress_vault(str(db_dir), preset=preset)
    _echo(f"Compressed → {db_dir.with_suffix('.tar.xz').name}")


@cli.command("decompress")
@click.argument("archive", type=click.Path(exists=True, path_type=Path))
def decompress_cmd(archive: Path):
    """Decompress a vault from a .tar.xz archive."""
    decompress_vault(str(archive.with_suffix("")))
    _echo(f"Decompressed → {archive.with_suffix('').name}/")


@cli.command("delete-vault")
@click.argument("db_dir", type=click.Path(exists=True, path_type=Path))
@click.confirmation_option(prompt="⚠️  Permanently delete the entire vault?")
def delete_vault_cmd(db_dir: Path):
    """Irreversibly delete an entire vault directory."""
    delete_vault(str(db_dir))
    _echo("Vault deleted ✔")


# --- Instance / table queries --------------------------------------------------


@cli.command("get-process-completion")
@click.argument("process_id")
@click.pass_context
def get_process_completion_cmd(ctx: click.Context, process_id: str):
    """Check if a process has completed (returns True/False)."""
    vault = _get_vault(ctx)
    _echo(vault.get_process_completion(process_id))


@cli.command("get-artifact-folder")
@click.argument("table_name")
@click.option(
    "--instance-id", default="", help="Specific instance ID (latest if omitted)."
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option(
    "--temp/--materialised",
    "is_temp",
    default=True,
    help="Path to temporary or materialised instance folder.",
)
@click.pass_context
def get_artifact_folder_cmd(
    ctx: click.Context,
    table_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
):
    """Get the path to an instance's artifact folder."""
    vault = _get_vault(ctx)
    _echo(vault.get_artifact_folder(table_name, instance_id, version, is_temp))


@cli.command("get-active-processes")
@click.pass_context
def get_active_processes_cmd(ctx: click.Context):
    """List all currently active processes in the vault."""
    vault = _get_vault(ctx)
    _echo(vault.get_active_processes())


@cli.command("get-table-instances")
@click.option("--table_name", default="", help="Table name.")
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option("--temp/--no-temp", default=False, help="Include temp instances.")
@click.pass_context
def get_table_instances_cmd(
    ctx: click.Context, table_name: str, version: str, temp: bool
):
    """List table name or instance IDs for a specific table and version."""
    vault = _get_vault(ctx)
    _echo(
        vault.get_table_instances(
            table_name=table_name, version=version, include_temp=temp
        )
    )


# --- Metadata helpers ----------------------------------------------------------


@cli.command("get-descriptions")
@click.option("--table-name", default="", help="Target table name (optional).")
@click.option("--instance-id", default="", help="Target instance ID (optional).")
@click.pass_context
def get_descriptions_cmd(ctx: click.Context, table_name: str, instance_id: str):
    """Get descriptions for the vault, a table, or an instance."""
    vault = _get_vault(ctx)
    _echo(vault.get_descriptions(instance_id=instance_id, table_name=table_name))


@cli.command("get-file-tree")
@click.option("--table-name", default="", help="Retrieve partial table tree.")
@click.option("--instance-id", default="", help="Retrieve partial instance tree.")
@click.option(
    "--code-files/--no-code-files", default=True, help="Include code modules."
)
@click.option(
    "--builder-files/--no-builder-files", default=True, help="Include builder files."
)
@click.option(
    "--metadata-files/--no-metadata-files",
    default=False,
    help="Include metadata files.",
)
@click.option(
    "--artifact-files/--no-artifact-files",
    default=False,
    help="Include artifact files.",
)
@click.pass_context
def get_file_tree_cmd(
    ctx: click.Context,
    table_name: str,
    instance_id: str,
    code_files: bool,
    builder_files: bool,
    metadata_files: bool,
    artifact_files: bool,
):
    """Get a file-tree representation of the vault."""
    vault = _get_vault(ctx)
    _echo(
        vault.get_file_tree(
            table_name=table_name,
            instance_id=instance_id,
            code_files=code_files,
            builder_files=builder_files,
            metadata_files=metadata_files,
            artifact_files=artifact_files,
        )
    )


@cli.command("get-modules-list")
@click.pass_context
def get_code_modules_list_cmd(ctx: click.Context):
    """List all Python code modules stored in the vault."""
    vault = _get_vault(ctx)
    _echo(vault.get_code_modules_list())


@cli.command("get-builders-list")
@click.argument("table_name")
@click.option(
    "--instance-id", default="", help="Specific instance (latest if omitted)."
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option(
    "--temp/--materialised",
    "is_temp",
    default=True,
    help="Look in temporary or materialised instance.",
)
@click.pass_context
def get_builders_list_cmd(
    ctx: click.Context,
    table_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
):
    """List builder names for a specific instance."""
    vault = _get_vault(ctx)
    _echo(
        vault.get_builders_list(
            table_name=table_name,
            instance_id=instance_id,
            version=version,
            is_temp=is_temp,
        )
    )


@cli.command("get-builder-str")
@click.argument("table_name")
@click.option(
    "--builder-name", default="", help="Builder file name (inferred if omitted)."
)
@click.option(
    "--instance-id", default="", help="Specific instance (latest if omitted)."
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option(
    "--temp/--materialised",
    "is_temp",
    default=True,
    help="Read from temporary or materialised instance.",
)
@click.pass_context
def get_builder_str_cmd(
    ctx: click.Context,
    builder_name: str,
    table_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
):
    """Get the content of a builder file as a string."""
    vault = _get_vault(ctx)
    _echo(
        vault.get_builder_str(
            builder_name=builder_name,
            table_name=table_name,
            instance_id=instance_id,
            version=version,
            is_temp=is_temp,
        )
    )


@cli.command("get-code-module-str")
@click.argument("module_name")
@click.pass_context
def get_code_module_str_cmd(ctx: click.Context, module_name: str):
    """Get the source code of a stored module as a string."""
    vault = _get_vault(ctx)
    _echo(vault.get_code_module_str(module_name))


# --- Dataframe extraction ------------------------------------------------------


@cli.command("get-dataframe")
@click.argument("table_name")
@click.option(
    "--output",
    "output_csv",
    required=True,
    type=click.Path(path_type=Path),
    help="Destination file path for the output CSV.",
)
@click.option(
    "--instance-id", default="", help="Specific instance ID (latest if omitted)."
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option("--rows", type=int, default=None, help="Max number of rows to return.")
@click.option("--include-inactive", is_flag=True, help="Consider non-active instances.")
@click.option(
    "--no-artifact-path",
    is_flag=True,
    help="Don't prefix repo path to 'artifact_string' columns.",
)
@click.pass_context
def get_dataframe_cmd(
    ctx: click.Context,
    table_name: str,
    output_csv: Path,
    instance_id: str,
    version: str,
    rows: Optional[int],
    include_inactive: bool,
    no_artifact_path: bool,
):
    """Fetch a table instance and save it as a CSV file."""
    vault = _get_vault(ctx)
    df, inst_id = vault.get_dataframe(
        table_name=table_name,
        instance_id=instance_id,
        version=version,
        active_only=not include_inactive,
        rows=rows,
        full_artifact_path=not no_artifact_path,
    )
    df.to_csv(output_csv, index=False)
    _echo({"instance_id": inst_id, "rows": len(df), "csv": str(output_csv)})


# --- Process control -----------------------------------------------------------


@cli.command("stop-process")
@click.argument("process_id")
@click.option("--force", is_flag=True, help="Forcibly terminate the running process.")
@click.option(
    "--materialize", is_flag=True, help="Materialise partial results if possible."
)
@click.pass_context
def stop_process_cmd(
    ctx: click.Context, process_id: str, force: bool, materialize: bool
):
    """Stop an active process."""
    vault = _get_vault(ctx)
    _echo(vault.stop_process(process_id, force=force, materialize=materialize))


# --- Code modules --------------------------------------------------------------


@cli.command("create-code-module")
@click.option("--module-name", default="", help="Name for the new module.")
@click.option(
    "--copy-dir",
    default="",
    type=click.Path(),
    help="File or directory to copy into the vault.",
)
@click.pass_context
def create_code_module_cmd(ctx: click.Context, module_name: str, copy_dir: str):
    """Create or copy a Python module into the vault."""
    vault = _get_vault(ctx)
    _echo(vault.create_code_module(module_name, copy_dir))


@cli.command("delete-code-module")
@click.argument("module_name")
@click.pass_context
def delete_code_module_cmd(ctx: click.Context, module_name: str):
    """Delete a Python module from the vault."""
    vault = _get_vault(ctx)
    _echo(vault.delete_code_module(module_name))


# --- Builders -----------------------------------------------------------------


@cli.command("create-builder-file")
@click.argument("table_name")
@click.option(
    "--builder-name", default="", help="Name of builder (inferred if omitted)."
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option(
    "--copy-dir",
    default="",
    type=click.Path(),
    help="Directory containing builder file(s).",
)
@click.pass_context
def create_builder_file_cmd(
    ctx: click.Context,
    table_name: str,
    builder_name: str,
    version: str,
    copy_dir: str,
):
    """Create or copy a builder file for a temporary instance."""
    vault = _get_vault(ctx)
    _echo(
        vault.create_builder_file(
            table_name=table_name,
            builder_name=builder_name,
            version=version,
            copy_dir=copy_dir,
        )
    )


@cli.command("delete-builder-file")
@click.argument("table_name")
@click.argument("builder_name")
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.pass_context
def delete_builder_file_cmd(
    ctx: click.Context, table_name: str, builder_name: str, version: str
):
    """Delete a builder file from a temporary instance."""
    vault = _get_vault(ctx)
    _echo(vault.delete_builder_file(builder_name, table_name, version))


# --- Table / instance housekeeping --------------------------------------------


@cli.command("rename-table")
@click.argument("old_name")
@click.argument("new_name")
@click.pass_context
def rename_table_cmd(ctx: click.Context, old_name: str, new_name: str):
    """Rename an existing table."""
    vault = _get_vault(ctx)
    _echo(vault.rename_table(new_table_name=new_name, table_name=old_name))


@cli.command("delete-table")
@click.argument("table_name")
@click.pass_context
def delete_table_cmd(ctx: click.Context, table_name: str):
    """Delete a table's data and artifacts (keeps metadata)."""
    vault = _get_vault(ctx)
    _echo(vault.delete_table(table_name))


@cli.command("delete-instance")
@click.argument("table_name")
@click.argument("instance_id")
@click.pass_context
def delete_instance_cmd(ctx: click.Context, table_name: str, instance_id: str):
    """Delete an instance's data and artifacts (keeps metadata)."""
    vault = _get_vault(ctx)
    _echo(vault.delete_instance(instance_id, table_name))


@cli.command("write-instance")
@click.argument("table_name")
@click.option(
    "--csv",
    "csv_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to the source CSV file.",
)
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Target version.")
@click.pass_context
def write_instance_cmd(
    ctx: click.Context, table_name: str, csv_path: Path, version: str
):
    """Write a CSV file as a materialised instance."""
    vault = _get_vault(ctx)
    df = pd.read_csv(csv_path)
    _echo(vault.write_instance(df, table_name, version=version))


@cli.command("execute-instance")
@click.argument("table_name")
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version.")
@click.option("--force", is_flag=True, help="Force a full rebuild of the instance.")
@click.option(
    "--background", is_flag=True, help="Run the materialisation in the background."
)
@click.pass_context
def execute_instance_cmd(
    ctx: click.Context, table_name: str, version: str, force: bool, background: bool
):
    """Materialise an instance from its builder files."""
    vault = _get_vault(ctx)
    _echo(
        vault.execute_instance(
            table_name=table_name,
            version=version,
            force_execute=force,
            background=background,
        )
    )


@cli.command("create-instance")
@click.argument("table_name")
@click.option("--version", default="", help="Table version.")
@click.option("--origin-id", default="", help="Copy state from this instance ID.")
@click.option("--origin-table", default="", help="Table for origin_id (if different).")
@click.option(
    "--external-edit",
    is_flag=True,
    help="Mark instance for external edit (no builders).",
)
@click.option(
    "--copy", is_flag=True, help="Use latest materialised instance as origin."
)
@click.option(
    "--builder",
    "builders",
    multiple=True,
    help="Builder name to generate (can be used multiple times).",
)
@click.pass_context
def create_instance_cmd(
    ctx: click.Context,
    table_name: str,
    version: str,
    origin_id: str,
    origin_table: str,
    external_edit: bool,
    copy: bool,
    builders: Tuple[str, ...],
):
    """Create a new temporary instance of a table."""
    vault = _get_vault(ctx)
    _echo(
        vault.create_instance(
            table_name=table_name,
            version=version,
            origin_id=origin_id,
            origin_table=origin_table,
            external_edit=external_edit,
            copy=copy,
            builders=list(builders),
        )
    )


@cli.command("create-table")
@click.argument("table_name")
@click.option(
    "--multiple-artifacts",
    is_flag=True,
    help="Allow each instance to have its own artifact folder.",
)
@click.option(
    "--side-effects",
    is_flag=True,
    help="Mark builders as having side effects (e.g. API calls).",
)
@click.pass_context
def create_table_cmd(
    ctx: click.Context, table_name: str, multiple_artifacts: bool, side_effects: bool
):
    """Create a new table definition in the vault."""
    vault = _get_vault(ctx)
    _echo(
        vault.create_table(
            table_name,
            allow_multiple_artifacts=multiple_artifacts,
            has_side_effects=side_effects,
        )
    )


# --- Misc helpers --------------------------------------------------------------


@cli.command("generate-process-id")
@click.pass_context
def generate_process_id_cmd(ctx: click.Context):
    """Generate a unique ID for a persistent process."""
    vault = _get_vault(ctx)
    _echo(vault.generate_process_id())


# --- Entry-point ---------------------------------------------------------------


def main():  # pragma: no cover
    """CLI entry point."""
    try:
        cli(obj={})
    except tv_errors.TableVaultError as exc:
        click.echo(f"TableVault error: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:
        click.echo(f"Unexpected error: {exc}", err=True)
        raise


if __name__ == "__main__":
    main()
