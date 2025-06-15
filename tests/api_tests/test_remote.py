from tablevault.core import TableVault
from . import helper
from .base_execution_helper import basic_function
import pytest
import time

# @pytest.mark.remote
# def test_remote_initial_copy(remote_tablevault):
#     ids = basic_function(remote_tablevault)
#     TableVault(db_dir="example_tv", author="jinjin", remote_dir="remote_tv/example_tv")
#     helper.compare_folders("example_tv", "remote_tv/example_tv")


# @pytest.mark.remote
# def test_remote_transfer(local_tablevault):
#     ids = basic_function(local_tablevault)
#     time.sleep(120)
#     helper.compare_folders("example_tv", "remote_tv/example_tv")