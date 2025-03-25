"""
test regular exception -> takedowns 

test tv exception -> takedowns

test force-stop operations

test restarts
"""
from unittest.mock import patch
from tablevault.core import TableVault
from tablevault.defintions import tv_errors

def raise_except():
    raise ValueError()

def raise_tv_except():
    raise tv_errors.TableVaultError()


def test_exception(funct_name, exception_func):
    with patch(f'tablevault._vault_operations.{funct_name}', exception_func()):
        tablevault = TableVault('test_dir', 'jinjin', create=True)
        tablevault.setup_table('stories', allow_multiple_artifacts = False)
        tablevault.setup_table('llm_storage', has_side_effects=True)
        tablevault.setup_table('llm_questions')
        tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
        tablevault.copy_files("../test_data/test_data_db/llm_storage", table_name="llm_storage")
        tablevault.copy_files("../test_data/test_data_db/llm_questions", table_name="llm_questions")
        tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
        tablevault.setup_temp_instance("llm_storage", prompt_names=["gen_llm_storage", "upload_openai"])
        tablevault.setup_temp_instance("llm_questions", prompt_names=["gen_llm_questions", "question_1","question_2", "question_3"])
        tablevault.execute_instance("stories")
        tablevault.execute_instance("llm_storage")
        tablevault.execute_instance("llm_questions")
        tablevault.print_active_processes(print_all=True)
        tablevault.print_active_processes(print_all=False)
        instances = tablevault.list_instances(table_name= "stories")
        tablevault.delete_table("llm_questions")
        tablevault.delete_instance(instance_id=instances[0], table_name="stories")

setup_functions = ["setup_copy_files", 
                   "setup_delete_table", 
                   "setup_delete_instance", 
                   "setup_execute_instance", 
                   "setup_setup_temp_instance", 
                   "setup_setup_table",
                   "setup_copy_database_files",
                   "setup_restart_database",
                   "setup_setup_temp_instance_innner",
                   "setup_setup_table_inner"]
execution_functions = []

