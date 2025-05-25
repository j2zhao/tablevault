from unittest.mock import patch
from tablevault.defintions import tv_errors
from helper import compare_folders, evaluate_operation_logging, clean_up_open_ai, copy_test_dir
from tablevault.core import TableVault
#from tablevault._operations._setup_operations import setup_setup_table
def raise_tv_except(**args):
    print("Exception Raised")
    raise tv_errors.TableVaultError()

def test_setup_exception(funct_name, exception_func):
    try:
        with patch.dict('tablevault._operations._meta_operations.SETUP_MAP', {funct_name:exception_func}):
            tablevault = TableVault('test_dir', 'jinjin', create=True)
            copy_test_dir()
            tablevault.create_table('stories', allow_multiple_artifacts = False)
            copy_test_dir()
            tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
            copy_test_dir()
            tablevault.create_instance("stories", builders=["gen_stories"])
            copy_test_dir()
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
            tablevault.create_table('stories', allow_multiple_artifacts = False)
            copy_test_dir()
            tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
            copy_test_dir()
            tablevault.create_instance("stories", builders=["gen_stories"])
            copy_test_dir()
            tablevault.execute_instance("stories")
            copy_test_dir()
            table = tablevault.get_table("stories")
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
                   "delete_table", 
                   "delete_instance", 
                   "execute_instance", 
                   "execute_instance_inner",
                   "write_table",
                   "write_table_inner",
                   #"materialize_instance",
                   "setup_temp_instance", 
                   "setup_table",
                   "setup_temp_instance_inner",
                   "setup_table_inner"]



execute_functions = [ 
                   "_delete_table", 
                   "_delete_instance", 
                   "_execute_instance",
                   "_execute_instance_inner",
                   "_write_table",
                   "_write_table_inner",
                   #"_materialize_instance",
                   "_setup_temp_instance", 
                   "_setup_temp_instance_inner",
                   "_setup_table",
                   "_setup_table_inner"]

execute_module = 'tablevault._vault_operations.'
setup_module = 'tablevault._operations._meta_operations.'

if __name__ == "__main__":
    for func in setup_functions:
        print(f'FUNCTION {func}')
        test_setup_exception(func, raise_tv_except)
    
    for func in execute_functions: 
        print(f'FUNCTION {func}')
        test_exception(execute_module, func, raise_tv_except)