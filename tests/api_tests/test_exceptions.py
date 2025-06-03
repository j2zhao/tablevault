from unittest.mock import patch
from tablevault._defintions import tv_errors
from .helper import compare_folders, evaluate_operation_logging
from .base_execution_helper import base_exception_function
from tablevault.core import TableVault
import pytest


def raise_tv_except(**args):
    raise tv_errors.TableVaultError()


@pytest.mark.parametrize(
    "funct_name",
    [
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
        "create_table",
    ],
)
def test_setup_exception(tablevault: TableVault, funct_name):
    process_ids = []
    with pytest.raises(tv_errors.TableVaultError):
        with patch.dict(
            "tablevault._operations._meta_operations.SETUP_MAP",
            {funct_name: raise_tv_except},
        ):
            base_exception_function(tablevault, process_ids)
    evaluate_operation_logging(process_ids)
    assert compare_folders("example_tv", "example_tv_copy")


@pytest.mark.parametrize(
    "funct_name",
    [
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
        "_create_table",
    ],
)
def test_exception(tablevault: TableVault, funct_name):
    process_ids = []
    with pytest.raises(tv_errors.TableVaultError):
        with patch(
            "tablevault._operations._vault_operations." + funct_name, raise_tv_except
        ):
            process_ids = []
            base_exception_function(tablevault, process_ids)
    evaluate_operation_logging(process_ids)
    assert compare_folders("example_tv", "example_tv_copy")
