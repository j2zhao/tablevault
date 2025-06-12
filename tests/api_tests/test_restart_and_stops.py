from tablevault.core import TableVault
from unittest.mock import patch
from .helper import evaluate_operation_logging, compare_folders
from .base_execution_helper import base_exception_function
import pytest


def raise_except(**args):
    print("Exception Raised")
    raise ValueError()


def evaluate_restart(process_id: str):
    tablevault = TableVault("example_tv", "jinjin")
    processes = tablevault.get_active_processes()
    assert process_id in processes
    assert not processes[process_id].force_takedown
    tablevault = TableVault("example_tv", "jinjin", restart=True)
    evaluate_operation_logging([process_id])


def evaluate_stops(process_id: str):
    tablevault = TableVault("example_tv", "jinjin")
    processes = tablevault.get_active_processes()
    assert process_id in processes
    tablevault = TableVault("example_tv", "jinjin")
    process_id_ = tablevault.stop_process(process_id, force=True)
    evaluate_operation_logging([process_id, process_id_])
    assert compare_folders("example_tv", "example_tv_copy")


_EXECUTE_FUNCS = [
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
]

_CASES: list[tuple[str, bool]] = []
for fn in _EXECUTE_FUNCS:
    if fn not in {"_write_instance", "_write_instance_inner"}:
        _CASES.append((fn, True))  # evaluate_restart=True
    _CASES.append((fn, False))  # evaluate_restart=False

_IDS = [f"{name}-{'restart' if restart else 'stop'}" for name, restart in _CASES]


@pytest.mark.parametrize(("funct_name", "eval_restart"), _CASES, ids=_IDS)
def test_restart_and_stops(tablevault: TableVault, funct_name, eval_restart):
    process_ids = []
    with pytest.raises(ValueError):
        with patch(
            "tablevault._operations._vault_operations." + funct_name, raise_except
        ):
            base_exception_function(tablevault, process_ids)
    if eval_restart:
        evaluate_restart(process_ids[-1])
    else:
        evaluate_stops(process_ids[-1])
