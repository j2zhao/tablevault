from tablevault.core import TableVault
from unittest.mock import patch
from helper import evaluate_operation_logging, compare_folders, copy_test_dir

def raise_except(**args):
    print("Exception Raised")
    raise ValueError()

def evaluate_restart(process_id:str):
    tablevault = TableVault('test_dir', 'jinjin')
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('test_dir', 'jinjin', restart=True)
    evaluate_operation_logging([process_id])

def evaluate_stops(process_id:str):
    tablevault = TableVault('test_dir', 'jinjin')
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault('test_dir', 'jinjin')
    process_id_ = tablevault.stop_process(process_id, force=True)
    evaluate_operation_logging([process_id, process_id_])
    assert compare_folders('test_dir', 'test_dir_copy')


def test_exception(module_path, funct_name, exception_func):
    try:
        with patch(module_path + funct_name, exception_func):
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.create_table('stories_TEST', allow_multiple_artifacts = False, process_id=last_process_id)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.rename_table('stories', 'stories_TEST', process_id=last_process_id)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.create_code_module("test", process_id=last_process_id)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.delete_code_module("test")
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.create_instance("stories", process_id=last_process_id)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.create_builder_file("test_buider", table_name="stories", process_id=last_process_id)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.delete_builder_file("test_buider", table_name="stories", process_id=last_process_id)
            tablevault.create_builder_file("../test_data/test_data_db/stories/gen_stories.yaml", table_name="stories")
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.execute_instance("stories", last_process_id)
            copy_test_dir()
            table = tablevault.get_dataframe("stories")
            tablevault.create_instance("stories", external_edit=True, copy_version=True)
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.write_instance(table, "stories", process_id=last_process_id)
            instances = tablevault.get_instances(table_name= "stories")
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.delete_instance(instance_id=instances[0], table_name="stories")
            copy_test_dir()
            last_process_id = tablevault.generate_process_id()
            tablevault.delete_table("stories")
            assert False
            
    except ValueError as e:
        evaluate_restart(last_process_id)
        evaluate_stops(last_process_id)

execute_functions = [
                   "_create_code_module", 
                   "_delete_code_module", 
                   "_create_builder_file",
                   "_delete_builder_file",
                   "_rename_table",
                   "_delete_table",
                   "_delete_instance",
                   "_write_table",
                   "_write_table_inner",
                   "_create_instance", 
                   "_setup_table",
                   "_create_table"]

execute_module = 'tablevault._vault_operations.'

if __name__ == "__main__":

    for func in execute_functions: 
        print(f'FUNCTION {func}')
        test_exception(execute_module, func, raise_except)