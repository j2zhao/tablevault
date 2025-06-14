import pytest
from tablevault.core import TableVault, delete_vault
from tablevault._helper.user_lock import set_writable
from tablevault._defintions import constants
import shutil
import os


@pytest.fixture
def tablevault():
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy")
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv")
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv")
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)
    tv = TableVault("example_tv", author="jinjin", create=True)
    yield tv
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy")
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv")
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv")
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)



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

@pytest.fixture
def remote_tablevault():
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy",ignore_errors=True)
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv",ignore_errors=True)
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv",ignore_errors=True)
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)
    os.mkdir("remote_tv")
    tv = TableVault("remote_tv/example_tv", author="jinjin", create=True)
    yield tv
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy",ignore_errors=True)
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv",ignore_errors=True)
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv", ignore_errors=True)
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)

@pytest.fixture
def local_tablevault():
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy",ignore_errors=True)
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv",ignore_errors=True)
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv", ignore_errors=True)
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)
    os.mkdir("remote_tv")
    tv = TableVault("example_tv", author="jinjin", create=True, remote_dir="remote_tv/example_tv", copy_interval=30)
    yield tv
    if os.path.isdir("example_tv_copy"):
        set_writable("example_tv_copy")
        shutil.rmtree("example_tv_copy", ignore_errors=True)
    if os.path.isdir("remote_tv"):
        set_writable("remote_tv")
        shutil.rmtree("remote_tv", ignore_errors=True)
    if os.path.isdir("example_tv"):
        set_writable("example_tv")
        shutil.rmtree("example_tv", ignore_errors=True)
    if os.path.exists(constants.REMOTE_LOG_FILE):
        os.remove(constants.REMOTE_LOG_FILE)