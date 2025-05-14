from tablevault.core import TableVault
"""
Test materialization and write table
"""
import helper
import pandas as pd

def test_materialize_basic():
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.setup_temp_instance('stories', external_edit=True)
    ids.append(id)
    id = tablevault.materialize_instance('stories')
    ids.append(id)
    helper.evaluate_operation_logging(ids)
    instances = tablevault.list_instances("stories")
    assert len(instances) == 1
    


def test_write_table_basic():
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.setup_temp_instance('stories', external_edit=True)
    ids.append(id)
    df = pd.DataFrame({
    'id':    [1, 2, 3],
    'name':  ['Alice', 'Bob', 'Charlie'],
    'score': [85.5, 92.0, 78.0]
    })
    id = tablevault.write_table(df, 'stories')
    ids.append(id)
    df2 = tablevault.fetch_table("stories")
    assert df.equals(df2)
    helper.evaluate_operation_logging(ids)

def test_materialize_copy():
    # get table and check that we can get something?
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.setup_temp_instance('stories', external_edit=True)
    ids.append(id)
    df = pd.DataFrame({
    'id':    [1, 2, 3],
    'name':  ['Alice', 'Bob', 'Charlie'],
    'score': [85.5, 92.0, 78.0]
    })
    id = tablevault.write_table(df, 'stories')
    ids.append(id)
    id = tablevault.setup_temp_instance(table_name="stories", copy_version=True, external_edit=True)
    ids.append(id)
    tablevault.materialize_instance("stories")
    df2 = tablevault.fetch_table("stories")
    assert df.equals(df2)
    helper.evaluate_operation_logging(ids)

def test_write_table_copy():
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.setup_temp_instance('stories', external_edit=True)
    ids.append(id)
    df = pd.DataFrame({
    'id':    [1, 2, 3],
    'name':  ['Alice', 'Bob', 'Charlie'],
    'score': [85.5, 92.0, 78.0]
    })
    id = tablevault.write_table(df, 'stories')
    ids.append(id)
    id = tablevault.setup_temp_instance(table_name="stories", copy_version=True, external_edit=True)
    ids.append(id)
    tablevault.write_table(df, "stories")
    df2 = tablevault.fetch_table("stories")
    assert df.equals(df2)
    helper.evaluate_operation_logging(ids)
    
if __name__ == "__main__":
    test_materialize_basic()
    test_write_table_basic()
    test_materialize_copy()
    test_write_table_copy()