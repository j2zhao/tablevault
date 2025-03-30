"""
test regular exception -> takedowns 

test tv exception -> takedowns

test force-stop operations

test restarts
"""
from unittest.mock import patch
from tablevault.core import TableVault
from tablevault.defintions import tv_errors
import shutil
import os
from helper import compare_folders, evaluate_operation_logging, clean_up_open_ai

def raise_tv_except():
    raise tv_errors.TableVaultError()

def test_exception(funct_name, module_path, exception_func):
    instance = None
    try:
        with patch( module_path + funct_name, exception_func()):
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            tablevault.setup_table('stories', allow_multiple_artifacts = False)
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            
            tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            tablevault.execute_instance("stories")
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            instances = tablevault.list_instances(table_name= "stories")
            instance = instances[0]
            tablevault.delete_instance(instance_id=instances[0], table_name="stories")
            shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)
            tablevault.delete_table("stories")
            
    except tv_errors.TableVaultError:
        evaluate_operation_logging([])
        assert compare_folders('test_dir', 'test_dir_copy')
        assert os.path.isdir('test_dir/stories')
        if instance != None:
            assert not os.path.isdir(f'test_dir/stories/{instance}')
        

setup_functions = ["setup_copy_files", 
                   "setup_delete_table", 
                   "setup_delete_instance", 
                   "setup_execute_instance", 
                   "setup_setup_temp_instance", 
                   "setup_setup_table",
                   "setup_copy_database_files",
                   "setup_setup_temp_instance_innner",
                   "setup_setup_table_inner"]

setup_module = 'tablevault._operations._setup_operations.'

execute_functions = ["_copy_files", 
                   "_delete_table", 
                   "_delete_instance", 
                   "_execute_instance", 
                   "_setup_temp_instance", 
                   "_setup_table",
                   "_copy_database_files",
                   "_setup_temp_instance_innner",
                   "_setup_table_inner"]

execute_module = 'tablevault._vault_operations.'

if __name__ == "__main__":
    for func in setup_functions:
        test_exception(func, setup_module, raise_tv_except)
    
    for func in execute_functions: 
        test_exception(func, execute_module, raise_tv_except)
    clean_up_open_ai()
