from unittest.mock import patch
from tablevault.defintions import tv_errors
from helper import compare_folders, evaluate_operation_logging, clean_up_open_ai, copy_test_dir
from tablevault.core import TableVault

def raise_tv_except(**args):
    print("Exception Raised")
    raise tv_errors.TableVaultError()

def test_setup_exception(funct_name, exception_func):
    try:
        with patch.dict('tablevault._operations._meta_operations.SETUP_MAP', {funct_name:exception_func}):
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            copy_test_dir()
            tablevault.create_table('stories_TEST', allow_multiple_artifacts = False)
            copy_test_dir()
            tablevault.rename_table('stories', 'stories_TEST')
            copy_test_dir()
            tablevault.create_code_module("test")
            copy_test_dir()
            tablevault.delete_code_module("test")
            copy_test_dir()
            tablevault.create_instance("stories")
            copy_test_dir()
            tablevault.create_builder_file("test_buider", table_name="stories")
            copy_test_dir()
            tablevault.delete_builder_file("test_buider", table_name="stories")
            copy_test_dir()
            tablevault.create_builder_file("../test_data/test_data_db/stories/gen_stories.yaml", table_name="stories")
            tablevault.execute_instance("stories")
            copy_test_dir()
            table = tablevault.get_dataframe("stories")
            tablevault.create_instance("stories", external_edit=True, copy_version=True)
            copy_test_dir()
            tablevault.write_instance(table, "stories")
            copy_test_dir()
            instances = tablevault.get_instances(table_name= "stories")
            tablevault.delete_instance(instance_id=instances[0], table_name="stories")
            copy_test_dir()
            tablevault.delete_table("stories")
            assert False
            
    except tv_errors.TableVaultError as e:
        evaluate_operation_logging([])
        assert compare_folders('test_dir', 'test_dir_copy')

def test_exception(module_path, funct_name, exception_func):
    try:
        with patch(module_path + funct_name, exception_func):
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            copy_test_dir()
            tablevault.create_table('stories_TEST', allow_multiple_artifacts = False)
            copy_test_dir()
            tablevault.rename_table('stories', 'stories_TEST')
            copy_test_dir()
            tablevault.create_code_module("test")
            copy_test_dir()
            tablevault.delete_code_module("test")
            copy_test_dir()
            tablevault.create_instance("stories")
            copy_test_dir()
            tablevault.create_builder_file("test_buider", table_name="stories")
            copy_test_dir()
            tablevault.delete_builder_file("test_buider", table_name="stories")
            copy_test_dir()
            tablevault.create_builder_file("../test_data/test_data_db/stories/gen_stories.yaml", table_name="stories")
            tablevault.execute_instance("stories")
            copy_test_dir()
            table = tablevault.get_dataframe("stories")
            tablevault.create_instance("stories", external_edit=True, copy_version=True)
            copy_test_dir()
            tablevault.write_instance(table, "stories")
            copy_test_dir()
            instances = tablevault.get_instances(table_name= "stories")
            tablevault.delete_instance(instance_id=instances[0], table_name="stories")
            copy_test_dir()
            tablevault.delete_table("stories")
            assert False
            
    except tv_errors.TableVaultError as e:
        evaluate_operation_logging([])
        assert compare_folders('test_dir', 'test_dir_copy')

setup_functions = [
                   "create_code_module", 
                   "delete_code_module", 
                   "create_builder_file",
                   "delete_builder_file",
                   "rename_table",
                   "delete_table",
                   "delete_instance",
                   "write_table",
                   "write_table_inner",
                   "create_instance", 
                   "setup_table",
                   "create_table"]



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
setup_module = 'tablevault._operations._meta_operations.'

if __name__ == "__main__":
    for func in setup_functions:
        print(f'FUNCTION {func}')
        test_setup_exception(func, raise_tv_except)
    
    for func in execute_functions: 
        print(f'FUNCTION {func}')
        test_exception(execute_module, func, raise_tv_except)