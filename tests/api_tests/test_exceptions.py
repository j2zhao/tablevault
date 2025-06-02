from unittest.mock import patch
from tablevault._defintions import tv_errors
from helper import compare_folders, evaluate_operation_logging, copy_test_dir
from tablevault.core import TableVault

def raise_tv_except(**args):
    print("Exception Raised")
    raise tv_errors.TableVaultError()

def base_test_code(process_ids):
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_table('stories_TEST', allow_multiple_artifacts = False, process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.rename_table('stories', 'stories_TEST', process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_code_module("test", process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_code_module("test", process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_instance("stories", process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_builder_file(builder_name="test_buider", table_name="stories", process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_builder_file(builder_name="test_buider", table_name="stories", process_id=last_process_id)
    tablevault.create_builder_file(copy_dir="../test_data/test_data_db/stories/stories_index.yaml", table_name="stories")
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.execute_instance("stories", process_id=last_process_id)
    copy_test_dir()
    table, _ = tablevault.get_dataframe("stories", artifact_path=False)
    tablevault.create_instance("stories", external_edit=True, copy=True)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.write_instance(table, "stories", process_id=last_process_id)
    instances = tablevault.get_instances(table_name= "stories")
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_instance(instance_id=instances[0], table_name="stories", process_id=last_process_id)
    copy_test_dir()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_table("stories", process_id=last_process_id)

def test_setup_exception(funct_name, exception_func):
    try:
        with patch.dict('tablevault._operations._meta_operations.SETUP_MAP', {funct_name:exception_func}):
            process_ids = []
            base_test_code(process_ids)
            assert False
    except tv_errors.TableVaultError as e:
        evaluate_operation_logging(process_ids)
        assert compare_folders('test_dir', 'test_dir_copy')

def test_exception(module_path, funct_name, exception_func):
    try:
        with patch(module_path + funct_name, exception_func):
            process_ids = []
            base_test_code(process_ids)
            assert False
            
    except tv_errors.TableVaultError as e:
        evaluate_operation_logging(process_ids)
        assert compare_folders('test_dir', 'test_dir_copy')

setup_functions = [
                   "create_code_module", 
                   "delete_code_module", 
                   "create_builder_file",
                   "delete_builder_file",
                   "rename_table",
                   "delete_table",
                   "delete_instance",
                   "write_instance",
                   "write_instance_inner",
                   "execute_instance",
                   "execute_instance_inner",
                   "create_instance", 
                   "create_table"]



execute_functions = [
                   "_create_code_module", 
                   "_delete_code_module", 
                   "_create_builder_file",
                   "_delete_builder_file",
                   "_rename_table",
                   "_delete_table",
                   "_delete_instance",
                   "_write_instance",
                   "_write_instance_inner",
                   "_execute_instance",
                   "_execute_instance_inner",
                   "_create_instance", 
                   "_create_table"]

execute_module = 'tablevault._operations._vault_operations.'
setup_module = 'tablevault._operations._meta_operations.'

if __name__ == "__main__":
    for func in setup_functions:
        print(f'FUNCTION {func}')
        test_setup_exception(func, raise_tv_except)
    
    for func in execute_functions: 
        print(f'FUNCTION {func}')
        test_exception(execute_module, func, raise_tv_except)