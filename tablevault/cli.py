import os
import click

from tablevault import _table_operations
from tablevault._metadata_store import MetadataStore

@click.group()
def cli():
    """
    CLI tool for running various database and table operations.
    """
    pass

def database_option(f):
    """Reusable database option."""
    return click.option('-db', '--database', required=True, type=click.Path(), help="Name of the database directory.")(f)

def author_option(f):
    """Reusable author option."""
    return click.option('-a', '--author', default='command_line', help="Author of the operation.")(f)

# -----------------------------------------------------------------------------
# Subcommands
# -----------------------------------------------------------------------------

@cli.command()
@database_option
@click.option('--all', '-all', is_flag=True, default=False, help="Show all active logs information.")
def active_processes(db_dir, all_info):
    """
    Print active logs from the database.
    """
    db_metadata = MetadataStore(db_dir)
    db_metadata.print_active_processes(all_info)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@click.option('-t', '--table', required=True, help="Name of the table.")
@click.option('-v', '--version', default='', help="Filter by this table version.")
def list_instances(db_dir, table_name, version):
    """
    Print table instances.
    """
    db_metadata = MetadataStore(db_dir)
    db_metadata.print_table_instances(table_name, version)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@click.option('-r', '--replace', is_flag=True, default=False, help="Replace existing database.")
def database(database, replace):
    """
    Setup a new database (or replace it).
    """
    _table_operations.setup_database(database, replace)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-t', '--table', required=True, help="Name of the table.")
@click.option('-m', '--multiple', is_flag=True,default=False,  help="Allow multiple table instance versions.")
def setup_table(database, author, table, multiple):
    """
    Setup a new table in the database.
    """
    _table_operations.setup_table(table, database, author, multiple)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-t', '--table', required=True, help="Name of the table.")
@click.option('-v', '--version', default='', help="Table Version.")
@click.option('-pid', '--prev-id', default='', help="Previous instance ID.")
@click.option('-p', '--prompts', multiple=True, default=[], help="Prompts for this instance.")
@click.option('-gp', '--gen-prompt', default='', help="Table Generating Prompt.")
def setup_temp(database, author, table, version, prev_id, prompts, gen_prompt):
    """
    Setup a new instance in a table.
    """
    _table_operations.setup_table_instance(
        version,
        table,
        database,
        author,
        prev_id,
        list(prompts), 
        gen_prompt
    )

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-t', '--table', required=True, help="Name of the table.")
def delete_table(database, author, table):
    """
    Delete a table from the database.
    """
    _table_operations.delete_table(table, database, author)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-t', '--table', required=True, help="Name of the table.")
@click.option('-id', '--instance-id', required=True, help="Instance ID to delete.")
def delete_instance(database, author, table, instance_id):
    """
    Delete a table instance from the database.
    """
    _table_operations.delete_table_instance(instance_id, table, database, author)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-t', '--table', required=True, help="Name of the table.")
@click.option('-v', '--version', default='', help="Version to execute.")
@click.option('-f', '--force', is_flag=True, default=False, help="Force execution.")
def execute(database, author, table, version, force):
    """
    Execute table processes in the database.
    """
    _table_operations.execute_table(table, database, author, version, force)

# -----------------------------------------------------------------------------

@cli.command()
@database_option
@author_option
@click.option('-ex', '--excluded', multiple=True, default=[], help="Exclude certain processes.")
def restart(database, author, excluded):
    """
    Restart the database processes, optionally excluding some.
    """
    _table_operations.restart_database(author, database, excluded_processes=list(excluded))

# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

def main():
    cli()
