# follow previous versioning tests

from tablevault.core import TableVault
import os
from test_basic_api import clean_up_open_ai, basic_function
import shutil

def copy_story(base_dir= './test_data/stories', story_name = 'The_Clockmakers_Secret.pdf'):
    org_path = os.path.join(base_dir, story_name)
    new_name = story_name.split(".")[0] + '_copy.pdf'
    new_path = os.path.join(base_dir, new_name)
    shutil.copy2(org_path, new_path)

def delete_story(base_dir= './test_data/stories', story_name = 'The_Clockmakers_Secret_copy.pdf'):
    story_path  = os.path.join(base_dir, story_name)
    os.remove(story_path)

def test_copy_instance_no_change(cleanup=True):
    basic_function(cleanup=False)
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.setup_temp_instance("llm_storage", copy_version=True)
    tablevault.setup_temp_instance("llm_questions", copy_version=True)
    tablevault.execute_instance("llm_questions")
    tablevault.execute_instance("llm_storage")
    if cleanup:
        clean_up_open_ai()

def test_copy_instance_prompt_change(cleanup=True):
    basic_function(cleanup=False)
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.setup_temp_instance("llm_questions", prompt_names=["gen_llm_questions", "question_1a","question_2", "question_3"])
    tablevault.execute_instance("llm_questions")
    if cleanup:
        clean_up_open_ai()

def test_copy_dep_change(cleanup=True):
    basic_function(cleanup=False)
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.setup_temp_instance("llm_storage", copy_version=True)
    tablevault.setup_temp_instance("llm_questions", copy_version=True)
    tablevault.execute_instance("llm_storage", force_execute=True)
    tablevault.execute_instance("llm_questions")
    if cleanup:
        clean_up_open_ai()


def test_new_row_change(cleanup=True):
    basic_function(cleanup=False)
    copy_story()
    tablevault = TableVault('test_dir', 'jinjin2')
    tablevault.setup_temp_instance("stories", copy_version=True)
    tablevault.setup_temp_instance("llm_storage", copy_version=True)
    tablevault.setup_temp_instance("llm_questions", copy_version=True)
    tablevault.execute_instance("stories")
    tablevault.execute_instance("llm_storage")
    tablevault.execute_instance("llm_questions")
    delete_story()
    if cleanup:
        clean_up_open_ai()

if __name__ == "__main__":
    test_copy_instance_no_change()