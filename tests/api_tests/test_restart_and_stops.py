from tablevault.core import TableVault
from unittest.mock import patch
from helper import evaluate_operation_logging, compare_folders, clean_up_open_ai, copy_test_dir
import shutil
from tablevault.col_builders.utils import table_operations
import pandas as pd
# todo add materialize
def raise_except():
    raise ValueError()

def evaluate_restart(process_id:str, exception_raised:bool):
    tablevault = TableVault('test_dir', 'jinjin')
    assert exception_raised
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('test_dir', 'jinjin', restart=True)
    evaluate_operation_logging([process_id])

def evaluate_stops(process_id:str, exception_raised:bool):
    tablevault = TableVault('test_dir', 'jinjin')
    assert exception_raised
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('test_dir', 'jinjin')
    process_id_ = tablevault.stop_process(process_id, force=True)
    evaluate_operation_logging([process_id, process_id_])
    assert compare_folders('test_dir', 'test_dir_copy')

def test_restart_copy_file():
    def _restart_copy_file():
        with patch("tablevault._vault_operations._copy_files", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories", process_id=process_id)
            except:
                exception_raised = True
            return process_id, exception_raised
    process_id, exception_raised =  _restart_copy_file()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_copy_file()
    evaluate_stops(process_id, exception_raised)

def test_restart_delete_table():
    def _restart_delete_table():
        with patch("tablevault._vault_operations._delete_table", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                copy_test_dir
                process_id = tablevault.generate_process_id()
                tablevault.delete_table("stories", process_id)
            except:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_delete_table()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_delete_table()
    evaluate_stops(process_id, exception_raised)

def test_restart_delete_instance():
    def _restart_delete_instance():
        with patch("tablevault._vault_operations._delete_instance", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.setup_temp_instance("stories", builder_names=["gen_stories"])
                tablevault.execute_instance("stories")
                instances = tablevault.get_instances("stories")
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.delete_instance(instances[0], "stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_delete_instance()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_delete_instance()
    evaluate_stops(process_id, exception_raised)


def update_table():
    df = pd.DataFrame({
    'id':    [1, 2, 3],
    'name':  ['Alice', 'Bob', 'Charlie'],
    'score': [85.5, 92.0, 78.0]
    })    
    table_operations.write_dtype(dict(df.dtypes), "TEMP_base", "stories", "test_dir")
    table_operations.write_table(df, "TEMP_base", "stories", "test_dir")


def test_restart_materialize_instance():
    def _restart_materialize_instance():
        with patch("tablevault._vault_operations._materialize_instance", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.setup_temp_instance("stories", external_edit=True)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                update_table()
                tablevault.materialize_instance("stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_materialize_instance()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_materialize_instance()
    evaluate_stops(process_id, exception_raised)

def test_restart_write_table():
    def _restart_write_table():
        with patch("tablevault._vault_operations._write_table", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.setup_temp_instance("stories", external_edit=True)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                df = pd.DataFrame({
                                    'id':    [1, 2, 3],
                                    'name':  ['Alice', 'Bob', 'Charlie'],
                                    'score': [85.5, 92.0, 78.0]
                                    }) 
                tablevault.write_table(df, "stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    # process_id, exception_raised =  _restart_write_table()
    # evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_write_table()
    evaluate_stops(process_id, exception_raised)

def test_restart_execute_instance():
    def _restart_execute_instance():
        with patch("tablevault._vault_operations._execute_instance", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.setup_temp_instance("stories", builder_names=["gen_stories"])
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.execute_instance("stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_execute_instance()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_execute_instance()
    evaluate_stops(process_id, exception_raised)

def test_restart_setup_temp_instance():
    def _restart_setup_temp_instance():
        with patch("tablevault._vault_operations._setup_temp_instance_inner", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.setup_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.setup_temp_instance("stories", builder_names=["gen_stories"], process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_setup_temp_instance()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_setup_temp_instance()
    evaluate_stops(process_id, exception_raised)

def test_restart_setup_table():
    def _restart_setup_table():
        with patch("tablevault._vault_operations._setup_table_inner", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.setup_table('stories', allow_multiple_artifacts = False, process_id=process_id)
            except Exception as e:
                #raise e 
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_setup_table()
    evaluate_restart(process_id, exception_raised)
    process_id, exception_raised =  _restart_setup_table()
    evaluate_stops(process_id, exception_raised)

if __name__ == "__main__":
    test_restart_copy_file()
    test_restart_delete_table()
    test_restart_delete_instance()
    test_restart_execute_instance()
    test_restart_setup_temp_instance()
    test_restart_setup_table()
    test_restart_write_table()
    test_restart_materialize_instance()
