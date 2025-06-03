import pytest
from tablevault.core import TableVault, delete_vault
import shutil
import os


@pytest.fixture
def tablevault():
    tv = TableVault("example_tv", author="jinjin", create=True)

    yield tv
    if os.path.isdir("example_tv"):
        delete_vault("example_tv")
    if os.path.isdir("example_tv_copy"):
        delete_vault("example_tv_copy")


@pytest.fixture
def add_story():
    base_dir = "./tests/test_data/stories"
    story_name = "The_Clockmakers_Secret.pdf"
    org_path = os.path.join(base_dir, story_name)
    new_name = story_name.split(".")[0] + "_copy.pdf"
    new_path = os.path.join(base_dir, new_name)
    shutil.copy2(org_path, new_path)
    yield

    if os.path.isfile(new_path):
        os.remove(new_path)
