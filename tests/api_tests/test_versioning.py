# follow previous versioning tests

from tablevault.core import TableVault
from .helper import evaluate_operation_logging, evaluate_full_tables, copy_example_tv
from .base_execution_helper import basic_function


def test_copy_instance_no_change(tablevault: TableVault):
    basic_function(tablevault)
    ids = []
    tablevault = TableVault("example_tv", "jinjin2")
    id = tablevault.create_instance("llm_storage", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_questions", copy=True)
    ids.append(id)
    copy_example_tv()
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    id = tablevault.execute_instance("llm_storage")
    ids.append(id)
    instances = tablevault.get_table_instances("llm_questions")
    assert len(instances) == 2
    df1, _ = tablevault.get_dataframe("llm_questions", instances[0])
    df2, _ = tablevault.get_dataframe("llm_questions", instances[1])
    assert df1.equals(df2)
    instances = tablevault.get_table_instances("llm_storage")
    assert len(instances) == 2
    df1, _ = tablevault.get_dataframe("llm_storage", instances[0])
    df2, _ = tablevault.get_dataframe("llm_storage", instances[1])
    assert df1.equals(df2)
    evaluate_operation_logging(ids)


def test_copy_instance_builder_change(tablevault: TableVault):
    basic_function(tablevault)
    ids = []
    tablevault = TableVault("example_tv", "jinjin2")
    id = tablevault.create_instance("llm_questions")
    builders = ["llm_questions_index", "question_1a", "question_2", "question_3"]
    for bn in builders:
        id = tablevault.create_builder_file(
            copy_dir=f"./tests/test_data/test_data_db/llm_questions/{bn}.yaml",
            table_name="llm_questions",
        )

    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    evaluate_operation_logging(ids)
    instances = tablevault.get_table_instances("llm_questions")
    assert len(instances) == 2
    df1, _ = tablevault.get_dataframe("llm_questions", instances[0])
    df2, _ = tablevault.get_dataframe("llm_questions", instances[1])
    cols_to_compare = ["paper_name", "q2a", "q2", "q3a", "q3"]
    assert df1[cols_to_compare].equals(df2[cols_to_compare])
    assert not df2["q1"].equals(df1["q1"])
    assert not df2["q1"].isna().any()


def test_copy_dep_change(tablevault: TableVault):
    basic_function(tablevault)
    ids = []
    tablevault = TableVault("example_tv", "jinjin2")
    id = tablevault.create_instance("llm_storage", copy=True)
    ids.append(id)
    id = tablevault.create_instance("llm_questions", copy=True)
    ids.append(id)
    id = tablevault.execute_instance("llm_storage", force_execute=True)
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    evaluate_operation_logging(ids)
    instances = tablevault.get_table_instances("llm_questions")
    assert len(instances) == 2
    df1, _ = tablevault.get_dataframe("llm_questions", instances[0])
    df2, _ = tablevault.get_dataframe("llm_questions", instances[1])
    cols_to_compare = ["q1", "q2a", "q2", "q3a", "q3"]
    assert len(df2) == 1
    for col in cols_to_compare:
        assert not df2[col].isna().any()


def test_new_row_change(tablevault: TableVault, add_story):
    ids = []
    basic_function(tablevault)
    add_story
    tablevault = TableVault("example_tv", "jinjin2")
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
    evaluate_operation_logging(ids)
    evaluate_full_tables(num_entries=2)


# if __name__ == "__main__":
#     test_copy_instance_no_change()
#     test_copy_instance_builder_change()
#     test_copy_dep_change()
#     test_new_row_change()
