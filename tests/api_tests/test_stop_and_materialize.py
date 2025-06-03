
import pandas as pd
from unittest.mock import patch
from tablevault.core import TableVault
from .helper import evaluate_operation_logging, copy_example_tv

def raise_except():
    print("EXCEPTION RAISED")
    raise ValueError()

def evaluate_stop_materialize(process_id:str, exception_raised:bool):
    tablevault = TableVault('example_tv', 'jinjin')
    assert exception_raised
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('example_tv', 'jinjin')
    process_id_ = tablevault.stop_process(process_id, force=True, materialize=True)
    evaluate_operation_logging([process_id, process_id_])
    instances = tablevault.get_instances("stories")
    assert len(instances) == 1

def test_restart_write_table(tablevault:TableVault):
    def _restart_write_table(tablevault:TableVault):
        with patch("tablevault._operations._vault_operations._materialize_instance", raise_except):
            exception_raised = False
            try:
                tablevault.create_table('stories', allow_multiple_artifacts = False)
                tablevault.create_instance("stories", external_edit=True)
                copy_example_tv()
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
    process_id, exception_raised =  _restart_write_table(tablevault)
    evaluate_stop_materialize(process_id, exception_raised)

def test_restart_execute_instance(tablevault:TableVault):
    def _restart_execute_instance(tablevault:TableVault):
        with patch("tablevault._operations._vault_operations._materialize_instance", raise_except):
            exception_raised = False
            try:
                tablevault.create_table('stories', allow_multiple_artifacts = False)
                tablevault.create_instance("stories")
                tablevault.create_builder_file(copy_dir="./tests/test_data/test_data_db_selected/stories", table_name="stories")
                copy_example_tv()
                process_id = tablevault.generate_process_id()
                tablevault.execute_instance("stories", process_id=process_id)
            except Exception as e:
                exception_raised = True
        return process_id, exception_raised
    process_id, exception_raised =  _restart_execute_instance(tablevault)
    evaluate_stop_materialize(process_id, exception_raised)

# if __name__ == "__main__":
#     test_restart_write_table()
#     test_restart_execute_instance()