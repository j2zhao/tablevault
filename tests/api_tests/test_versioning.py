# follow previous versioning tests

from tablevault.core import TableVault
import os
from test_basic_api import basic_function
from helper import evaluate_operation_logging, evaluate_full_tables, clean_up_open_ai, copy_test_dir
import shutil

def copy_story(base_dir= '../test_data/stories', story_name = 'The_Clockmakers_Secret.pdf'):
    org_path = os.path.join(base_dir, story_name)
    new_name = story_name.split(".")[0] + '_copy.pdf'
    new_path = os.path.join(base_dir, new_name)
    shutil.copy2(org_path, new_path)

def delete_story(base_dir= '../test_data/stories', story_name = 'The_Clockmakers_Secret_copy.pdf'):
    story_path  = os.path.join(base_dir, story_name)
    if os.path.isfile(story_path):
        os.remove(story_path)

def test_copy_instance_no_change():
    #basic_function()
    copy_test_dir("test_dir", "basic_function")
    ids = []
    tablevault = TableVault('test_dir', 'jinjin2')
    id = tablevault.create_instance("llm_storage", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_questions", copy=True)
    ids.append(id)
    copy_test_dir()
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    id = tablevault.execute_instance("llm_storage")
    ids.append(id)
    instances = tablevault.get_instances("llm_questions")
    assert len(instances) == 2
    df1 = tablevault.get_dataframe("llm_questions", instances[0])
    df2 = tablevault.get_dataframe("llm_questions", instances[1])
    assert df1.equals(df2)
    instances = tablevault.get_instances("llm_storage")
    assert len(instances) == 2
    df1 = tablevault.get_dataframe("llm_storage", instances[0])
    df2 = tablevault.get_dataframe("llm_storage", instances[1])
    assert df1.equals(df2)
    evaluate_operation_logging(ids)

def test_copy_instance_builder_change():
    #basic_function()
    copy_test_dir("test_dir", "basic_function")
    ids = []
    tablevault = TableVault('test_dir', 'jinjin2')
    id = tablevault.create_instance("llm_questions")
    builders=["gen_llm_questions", "question_1a","question_2", "question_3"]
    for bn in builders:
        id = tablevault.create_builder_file(copy_dir=f"../test_data/test_data_db/llm_questions/{bn}.yaml", table_name="llm_questions")
 
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    evaluate_operation_logging(ids)
    instances = tablevault.get_instances("llm_questions")
    assert len(instances) == 2
    df1 = tablevault.get_dataframe("llm_questions", instances[0])
    df2 = tablevault.get_dataframe("llm_questions", instances[1])
    cols_to_compare = ['paper_name', 'q2a', 'q2', 'q3a', 'q3']
    assert df1[cols_to_compare].equals(df2[cols_to_compare])
    assert not df2['q1'].equals(df1['q1'])
    assert not df2['q1'].isna().any()

def test_copy_dep_change():
    #basic_function()
    copy_test_dir("test_dir", "basic_function")
    ids = []
    tablevault = TableVault('test_dir', 'jinjin2')
    id = tablevault.create_instance("llm_storage", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_questions", copy=True)
    ids.append(id)
    id = tablevault.execute_instance("llm_storage", force_execute=True)
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    evaluate_operation_logging(ids)
    instances = tablevault.get_instances("llm_questions")
    assert len(instances) == 2
    df1 = tablevault.get_dataframe("llm_questions", instances[0])
    df2 = tablevault.get_dataframe("llm_questions", instances[1])
    cols_to_compare = ['q1', 'q2a', 'q2', 'q3a', 'q3']
    assert len(df2) == 1
    for col in cols_to_compare:
        #assert not df1[col].equals(df2[col])
        assert not df2[col].isna().any()

def test_new_row_change():
    ids = []
    delete_story()
    copy_test_dir("test_dir", "basic_function")
    copy_story()
    tablevault = TableVault('test_dir', 'jinjin2')
    id = tablevault.create_instance("stories", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_storage", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_questions", copy=True)
    ids.append(id)
    id = tablevault.execute_instance("stories")
    ids.append(id)
    id = tablevault.execute_instance("llm_storage")
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    delete_story()
    evaluate_operation_logging(ids)
    evaluate_full_tables(num_entries=2)

if __name__ == "__main__":
    test_copy_instance_no_change()
    test_copy_instance_builder_change()
    test_copy_dep_change()
    test_new_row_change()
    #clean_up_open_ai()