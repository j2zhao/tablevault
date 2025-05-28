
import pandas as pd
from unittest.mock import patch
from tablevault.core import TableVault
from helper import evaluate_operation_logging, compare_folders, clean_up_open_ai, copy_test_dir
from tablevault.col_builders.utils import table_operations

def raise_except():
    raise ValueError()

def evaluate_stop_materialize(process_id:str, exception_raised:bool):
    tablevault = TableVault('test_dir', 'jinjin')
    assert exception_raised
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('test_dir', 'jinjin')
    process_id_ = tablevault.stop_process(process_id, force=True, materialize=True)
    evaluate_operation_logging([process_id, process_id_])
    instances = tablevault.get_instances("stories")
    assert len(instances) == 1

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
                tablevault.create_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.create_instance("stories", external_edit=True)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                update_table()
                tablevault.materialize_instance("stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_materialize_instance()
    evaluate_stop_materialize(process_id, exception_raised)

def test_restart_write_table():
    def _restart_write_table():
        with patch("tablevault._vault_operations._materialize_instance", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.create_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.create_instance("stories", external_edit=True)
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                df = pd.DataFrame({
                                    'id':    [1, 2, 3],
                                    'name':  ['Alice', 'Bob', 'Charlie'],
                                    'score': [85.5, 92.0, 78.0]
                                    }) 
                tablevault.write_instance(df, "stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_write_table()
    evaluate_stop_materialize(process_id, exception_raised)

def test_restart_execute_instance():
    def _restart_execute_instance():
        with patch("tablevault._vault_operations._materialize_instance", raise_except):
            exception_raised = False
            try:
                tablevault = TableVault('test_dir', 'jinjin', create=True)
                tablevault.create_table('stories', allow_multiple_artifacts = False)
                tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
                tablevault.create_instance("stories", builders=["gen_stories"])
                copy_test_dir()
                process_id = tablevault.generate_process_id()
                tablevault.execute_instance("stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_execute_instance()
    evaluate_stop_materialize(process_id, exception_raised)

if __name__ == "__main__":
    test_restart_materialize_instance()
    test_restart_write_table()
    test_restart_execute_instance()