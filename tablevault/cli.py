import json
import sys
import click
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple

# --- TableVault imports ------------------------------------------------------
# NB: assumes the original TableVault implementation (shown in the prompt)
#     lives in tablevault.core together with the helper module‑level utilities.
from tablevault.core import (
    TableVault,
    compress_vault,
    decompress_vault,
    delete_vault,
)
from tablevault._defintions import tv_errors, constants

# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------


def _echo(obj):
    """Pretty‑print *obj* to stdout so shell users can capture the value."""
    if isinstance(obj, (dict, list)):
        click.echo(json.dumps(obj, indent=2, default=str))
    else:
        click.echo(str(obj))


def _bail(ctx: click.Context, param: str):
    ctx.fail(
        f"Missing required option `{param}` – supply it via `--{param.replace('_', '-')}`"
    )


# -----------------------------------------------------------------------------
# Main CLI group
# -----------------------------------------------------------------------------


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--db-dir",
    type=click.Path(path_type=Path),
    help="Path to the TableVault directory (required by most commands)",
)
@click.option("--author", type=str, help="Author name used for audit logging")
@click.pass_context
def cli(ctx: click.Context, db_dir: Optional[Path], author: Optional[str]):
    """A convenient **command‑line interface** for *TableVault* operations.

    The vast majority of sub‑commands need a *TableVault* instance; provide the
    `--db-dir` and `--author` options once and they will be reused by every
    command you invoke:

        $ tablevault-cli --db-dir ./my_vault --author jinjin get-active-processes
    """
    ctx.ensure_object(dict)
    ctx.obj["db_dir"] = db_dir
    ctx.obj["author"] = author


# -----------------------------------------------------------------------------
# Helper to lazily instantiate a TableVault when a command needs it
# -----------------------------------------------------------------------------


def _get_vault(ctx: click.Context, verbose: bool = True) -> TableVault:
    db_dir: Optional[Path] = ctx.obj.get("db_dir")
    author: Optional[str] = ctx.obj.get("author")

    if db_dir is None:
        _bail(ctx, "db_dir")
    if author is None:
        _bail(ctx, "author")

    return TableVault(db_dir=str(db_dir), author=author, verbose=verbose)


# -----------------------------------------------------------------------------
# Vault‑*independent* utilities (archive / delete)
# -----------------------------------------------------------------------------


@cli.command("compress")
@click.argument("db_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--preset", default=6, show_default=True, help="LZMA compression level 1–9"
)
def compress_cmd(db_dir: Path, preset: int):
    """Create a `DB_DIR.tar.xz` archive from *DB_DIR*."""
    compress_vault(str(db_dir), preset=preset)
    _echo(f"Compressed → {db_dir.with_suffix('.tar.xz').name}")


@cli.command("decompress")
@click.argument("archive", type=click.Path(exists=True, path_type=Path))
def decompress_cmd(archive: Path):
    """Extract a `*.tar.xz` archive created by *compress*."""
    decompress_vault(str(archive.with_suffix("")))
    _echo(f"Decompressed → {archive.with_suffix('').name}/")


@cli.command("delete-vault")
@click.argument("db_dir", type=click.Path(exists=True, path_type=Path))
@click.confirmation_option(
    prompt="⚠️  This will permanently delete the vault – continue?"
)
def delete_vault_cmd(db_dir: Path):
    """**Irreversibly** delete an entire vault directory."""
    delete_vault(str(db_dir))
    _echo("Vault deleted ✔")


# -----------------------------------------------------------------------------
# TableVault instance‑based commands (skip get‑dataframe for now)
# -----------------------------------------------------------------------------


@cli.command("get-process-completion")
@click.argument("process_id")
@click.pass_context
def get_process_completion_cmd(ctx: click.Context, process_id: str):
    """Return **True/False** if *PROCESS_ID* has finished."""
    vault = _get_vault(ctx)
    _echo(vault.get_process_completion(process_id))


@cli.command("get-artifact-folder")
@click.argument("table_name")
@click.option("--instance-id", default="", help="Specific instance ID (optional)")
@click.option("--version", default=constants.BASE_TABLE_VERSION, help="Table version")
@click.option(
    "--temp/--materialised",
    "is_temp",
    default=True,
    help="Return the temporary or last materialised folder",
)
@click.pass_context
def get_artifact_folder_cmd(
    ctx: click.Context,
    table_name: str,
    instance_id: str,
    version: str,
    is_temp: bool,
):
    vault = _get_vault(ctx)
    _echo(vault.get_artifact_folder(table_name, instance_id, version, is_temp))


@cli.command("get-active-processes")
@click.pass_context
def get_active_processes_cmd(ctx: click.Context):
    """List all currently active processes."""
    vault = _get_vault(ctx)
    _echo(vault.get_active_processes())


@cli.command("get-instances")
@click.argument("table_name")
@click.option("--version", default=constants.BASE_TABLE_VERSION)
@click.pass_context
def get_instances_cmd(ctx: click.Context, table_name: str, version: str):
    vault = _get_vault(ctx)
    _echo(vault.get_instances(table_name=table_name, version=version))


@cli.command("get-dataframe")
@click.argument("table_name")
@click.option(
    "--output",
    "output_csv",
    required=True,
    type=click.Path(path_type=Path),
    help="Destination CSV file",
)
@click.option("--instance-id", default="")
@click.option("--version", default=constants.BASE_TABLE_VERSION)
@click.option("--rows", type=int, default=None, help="Row limit (None ⇒ no limit)")
@click.option("--include-inactive", is_flag=True, help="Include inactive instances")
@click.option(
    "--no-artifact-path",
    is_flag=True,
    help="Skip prepending artifact folder to 'artifact_string' columns",
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
    """Fetch a table instance as CSV (written to *OUTPUT*)."""
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


@cli.command("stop-process")
@click.argument("process_id")
@click.option("--force", is_flag=True, help="Force‑kill the running process")
@click.option(
    "--materialize", is_flag=True, help="Materialise partial instances if possible"
)
@click.pass_context
def stop_process_cmd(
    ctx: click.Context, process_id: str, force: bool, materialize: bool
):
    vault = _get_vault(ctx)
    _echo(vault.stop_process(process_id, force=force, materialize=materialize))


@cli.command("create-code-module")
@click.option("--module-name", default="", help="Name for the new module (optional)")
@click.option(
    "--copy-dir", default="", type=click.Path(), help="File/dir to copy into the vault"
)
@click.pass_context
def create_code_module_cmd(ctx: click.Context, module_name: str, copy_dir: str):
    vault = _get_vault(ctx)
    _echo(vault.create_code_module(module_name, copy_dir))


@cli.command("delete-code-module")
@click.argument("module_name")
@click.pass_context
def delete_code_module_cmd(ctx: click.Context, module_name: str):
    vault = _get_vault(ctx)
    _echo(vault.delete_code_module(module_name))


@cli.command("create-builder-file")
@click.argument("table_name")
@click.option("--builder-name", default="")
@click.option("--version", default=constants.BASE_TABLE_VERSION)
@click.option("--copy-dir", default="", type=click.Path())
@click.pass_context
def create_builder_file_cmd(
    ctx: click.Context,
    table_name: str,
    builder_name: str,
    version: str,
    copy_dir: str,
):
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
@click.option("--version", default=constants.BASE_TABLE_VERSION)
@click.pass_context
def delete_builder_file_cmd(
    ctx: click.Context, table_name: str, builder_name: str, version: str
):
    vault = _get_vault(ctx)
    _echo(vault.delete_builder_file(builder_name, table_name, version))


@cli.command("rename-table")
@click.argument("old_name")
@click.argument("new_name")
@click.pass_context
def rename_table_cmd(ctx: click.Context, old_name: str, new_name: str):
    vault = _get_vault(ctx)
    _echo(vault.rename_table(new_table_name=new_name, table_name=old_name))


@cli.command("delete-table")
@click.argument("table_name")
@click.pass_context
def delete_table_cmd(ctx: click.Context, table_name: str):
    vault = _get_vault(ctx)
    _echo(vault.delete_table(table_name))


@cli.command("delete-instance")
@click.argument("table_name")
@click.argument("instance_id")
@click.pass_context
def delete_instance_cmd(ctx: click.Context, table_name: str, instance_id: str):
    vault = _get_vault(ctx)
    _echo(vault.delete_instance(instance_id, table_name))


@cli.command("write-instance")
@click.argument("table_name")
@click.option(
    "--csv",
    "csv_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="CSV file containing the dataframe",
)
@click.option("--version", default=constants.BASE_TABLE_VERSION)
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
@click.option("--version", default=constants.BASE_TABLE_VERSION)
@click.option("--force", is_flag=True, help="Force a full rebuild")
@click.option("--background", is_flag=True, help="Run in the background")
@click.pass_context
def execute_instance_cmd(
    ctx: click.Context, table_name: str, version: str, force: bool, background: bool
):
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
@click.option("--version", default="", help="Table version")
@click.option("--origin-id", default="")
@click.option("--origin-table", default="")
@click.option("--external-edit", is_flag=True)
@click.option(
    "--no-copy", is_flag=True, help="Do not copy from latest materialised instance"
)
@click.option("--builder", "builders", multiple=True, help="Add builder names (repeat)")
@click.pass_context
def create_instance_cmd(
    ctx: click.Context,
    table_name: str,
    version: str,
    origin_id: str,
    origin_table: str,
    external_edit: bool,
    no_copy: bool,
    builders: Tuple[str, ...],
):
    vault = _get_vault(ctx)
    _echo(
        vault.create_instance(
            table_name=table_name,
            version=version,
            origin_id=origin_id,
            origin_table=origin_table,
            external_edit=external_edit,
            copy=not no_copy,
            builders=list(builders),
        )
    )


@cli.command("create-table")
@click.argument("table_name")
@click.option("--multiple-artifacts", is_flag=True)
@click.option("--side-effects", is_flag=True)
@click.pass_context
def create_table_cmd(
    ctx: click.Context, table_name: str, multiple_artifacts: bool, side_effects: bool
):
    vault = _get_vault(ctx)
    _echo(
        vault.create_table(
            table_name,
            allow_multiple_artifacts=multiple_artifacts,
            has_side_effects=side_effects,
        )
    )


@cli.command("generate-process-id")
@click.pass_context
def generate_process_id_cmd(ctx: click.Context):
    vault = _get_vault(ctx)
    _echo(vault.generate_process_id())


# -----------------------------------------------------------------------------
# Entry‑point – allows `python -m tablevault_cli` and pip‑installed cmd `tablevault-cli`
# -----------------------------------------------------------------------------


def main():  # pragma: no cover – convenience shim
    try:
        cli(obj={})
    except (
        tv_errors.TableVaultError
    ) as exc:  # base class of all tablevault custom errors
        click.echo(f"TableVault error: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 – re‑raise unknown exceptions
        click.echo(f"Unexpected error: {exc}", err=True)
        raise


if __name__ == "__main__":
    main()
