# mock a table execution and then see if i can execute
from unittest.mock import patch
from tablevault.core import TableVault
import time
import threading
from tablevault.defintions import tv_errors
from helper import clean_up_open_ai

def fake_execution():
    print("successfully faked")
    time.sleep(100)

def test_multiprocessing_execute():
    with patch('tablevault._vault_operations._execute_instance',fake_execution):
        tablevault = TableVault('test_dir', 'jinjin', create=True)
        tablevault.setup_table('stories', allow_multiple_artifacts = False)
        tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
        tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
        tablevault.execute_instance("stories")
        
def test_multiprocessing_other_table():
    tablevault = TableVault('test_dir', 'jinjin2')
    instances = tablevault.list_instances(table_name= "stories")
    tablevault.setup_table('llm_storage', has_side_effects=True)

def test_multiprocessing_other_execute():
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.execute_instance("stories")

def test_multiprocessing_other_instance():
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])

def test_multiprocessing():
    t = threading.Thread(target=test_multiprocessing_execute)
    t.start()
    time.sleep(5)
    failed_execution = False
    try:
        test_multiprocessing_other_table()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert not failed_execution
    
    failed_execution = False
    try:
        test_multiprocessing_other_execute()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert failed_execution

    failed_execution = False
    try:
        test_multiprocessing_other_instance()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert failed_execution


if __name__ == "__main__":
    test_multiprocessing()
    clean_up_open_ai()