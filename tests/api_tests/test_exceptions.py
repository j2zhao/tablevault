"""
test regular exception -> takedowns 

test tv exception -> takedowns

test force-stop operations

test restarts
"""
from unittest.mock import patch
from tablevault.defintions import tv_errors
from helper import compare_folders, evaluate_operation_logging, clean_up_open_ai, copy_test_dir
#from tablevault._operations._setup_operations import setup_setup_table
def raise_tv_except():
    print("HELLO")
    #raise ValueError()
    raise tv_errors.TableVaultError()

def test_exception(module_path, funct_name, exception_func):
    try:
        print(module_path + funct_name)
        with patch(module_path + funct_name, exception_func):
            from tablevault.core import TableVault
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            copy_test_dir()
            tablevault.setup_table('stories', allow_multiple_artifacts = False)
            copy_test_dir()
            tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
            copy_test_dir()
            tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
            copy_test_dir()
            tablevault.execute_instance("stories")
            copy_test_dir()
            instances = tablevault.list_instances(table_name= "stories")
            tablevault.delete_instance(instance_id=instances[0], table_name="stories")
            copy_test_dir()
            tablevault.delete_table("stories")
            
    except tv_errors.TableVaultError as e:
        evaluate_operation_logging([])
        assert compare_folders('test_dir', 'test_dir_copy')

setup_functions = [
                   "setup_delete_table", 
                   "setup_delete_instance", 
                   "setup_execute_instance", 
                   "setup_execute_instance_inner",
                   "setup_setup_temp_instance", 
                   "setup_setup_table",
                   "setup_copy_database_files",
                   "setup_setup_temp_instance_inner",
                   "setup_setup_table_inner"]

setup_module = 'tablevault._operations._setup_operations.'

execute_functions = [ 
                   "_delete_table", 
                   "_delete_instance", 
                   "_execute_instance",
                   "_execute_instance_inner",
                   "_setup_temp_instance", 
                   "_setup_table",
                   "_copy_database_files",
                   "_setup_table_inner",
                   "_setup_table_inner"]

execute_module = 'tablevault._vault_operations.'

if __name__ == "__main__":
    # test_exception(setup_module, "setup_setup_table", raise_tv_except)
    # # for func in setup_functions:
    # #     test_exception(setup_module, func, raise_tv_except)
    
    for func in execute_functions: 
        print(f'FUNCTION {func}')
        test_exception(execute_module, func, raise_tv_except)
