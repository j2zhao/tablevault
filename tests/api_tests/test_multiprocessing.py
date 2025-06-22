# mock a table execution and then see if i can execute
from unittest.mock import patch
from tablevault.core import TableVault
import threading
from tablevault._defintions import tv_errors
from multiprocessing import Event

started_evt = Event()
finish_evt = Event()


def fake_execution():
    started_evt.set()
    print("Execution Faked")
    finish_evt.wait()
    return 0


def multiprocessing_execute(tablevault: TableVault):
    with patch(
        "tablevault._operations._vault_operations._execute_instance", fake_execution
    ):
        tablevault.create_table("stories", allow_multiple_artifacts=False)
        tablevault.create_instance("stories")
        tablevault.create_builder_file(
            copy_dir="./tests/test_data/test_data_db_selected/stories",
            table_name="stories",
        )
        process_id = tablevault.generate_process_id()
        tablevault.execute_instance("stories", process_id=process_id)


def multiprocessing_other_table():
    tablevault = TableVault("example_tv", "jinjin2")
    tablevault.get_table_instances(table_name="stories")
    tablevault.create_table("llm_storage", has_side_effects=True)


def multiprocessing_other_execute():
    tablevault = TableVault("example_tv", "jinjin2")
    tablevault.execute_instance("stories")


def multiprocessing_other_instance():
    tablevault = TableVault("example_tv", "jinjin2")
    tablevault.create_instance("stories")


def test_multiprocessing(tablevault):
    t = threading.Thread(target=multiprocessing_execute, args=[tablevault], daemon=True)
    t.start()
    assert started_evt.wait(timeout=5)

    failed_execution = False
    try:
        multiprocessing_other_table()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert not failed_execution
    failed_execution = False
    try:
        multiprocessing_other_execute()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert failed_execution
    failed_execution = False
    try:
        multiprocessing_other_instance()
    except tv_errors.TableVaultError:
        failed_execution = True
    assert failed_execution

    finish_evt.set()
    t.join()


# if __name__ == "__main__":
#     test_multiprocessing()
#     #clean_up_open_ai()
