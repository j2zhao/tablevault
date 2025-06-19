from . import helper
from tablevault.core import TableVault

from .base_execution_helper import basic_function
import pytest

@pytest.mark.basic
def test_basic_function(tablevault):
    ids = basic_function(tablevault)
    helper.evaluate_operation_logging(ids)
    helper.evaluate_full_tables()

def test_empty(tablevault):
    ids = basic_function(tablevault, empty=True)
    helper.evaluate_operation_logging(ids)
    helper.evaluate_empty_tables()

def test_deletion(tablevault):
    ids = []
    basic_function(tablevault)
    tablevault = TableVault("example_tv", "jinjin")
    id = tablevault.delete_code_module("test")
    ids.append(id)
    id = tablevault.create_instance("stories", builders=["test_builder"])
    ids.append(id)
    id = tablevault.delete_builder_file("test_builder", "stories")
    ids.append(id)
    instances = tablevault.get_table_instances("stories")
    id = tablevault.delete_instance(instances[0], "stories")
    ids.append(id)
    id = tablevault.delete_table("llm_storage")
    ids.append(id)
    helper.evaluate_operation_logging(ids)
    helper.evaluate_deletion()


# if __name__ == "__main__":
#     test_basic_function()
#     test_deletion()
