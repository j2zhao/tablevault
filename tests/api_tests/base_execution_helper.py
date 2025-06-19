from tablevault.core import TableVault
from .helper import copy_example_tv


def basic_function(tv: TableVault, empty = False):
    ids = []
    id = tv.create_code_module("test")
    ids.append(id)
    id = tv.create_table("stories_TEST", allow_multiple_artifacts=False)
    ids.append(id)
    id = tv.rename_table("stories", "stories_TEST")
    ids.append(id)
    id = tv.create_table("llm_storage", has_side_effects=True)
    ids.append(id)

    id = tv.create_table("llm_questions")
    ids.append(id)
    id = tv.create_instance("stories")
    ids.append(id)
    id = tv.create_instance("llm_storage", builders=["upload_openai"])
    ids.append(id)
    id = tv.create_instance(
        "llm_questions", builders=["question_1", "question_2", "question_3"]
    )
    if not empty:
        id = tv.create_builder_file(
            copy_dir="./tests/test_data/test_data_db/stories/stories_index.yaml",
            table_name="stories",
        )
    else:
        id = tv.create_builder_file(
            copy_dir="./tests/test_data/test_empty_db/stories/stories_index.yaml",
            table_name="stories",
        )
    ids.append(id)

    id = tv.create_builder_file(
        copy_dir="./tests/test_data/test_data_db_selected/llm_storage",
        table_name="llm_storage",
    )
    ids.append(id)
    id = tv.create_builder_file(
        copy_dir="./tests/test_data/test_data_db_selected/llm_questions",
        table_name="llm_questions",
    )
    ids.append(id)

    id = tv.execute_instance("stories")
    ids.append(id)
    id = tv.execute_instance("llm_storage")
    ids.append(id)
    id = tv.execute_instance("llm_questions")
    ids.append(id)
    return ids


def base_exception_function(tablevault: TableVault, process_ids: list):
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_table(
        "stories_TEST", allow_multiple_artifacts=False, process_id=last_process_id
    )
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.rename_table("stories", "stories_TEST", process_id=last_process_id)
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_code_module("test", process_id=last_process_id)
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_code_module("test", process_id=last_process_id)
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_instance("stories", process_id=last_process_id)
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.create_builder_file(
        builder_name="test_buider", table_name="stories", process_id=last_process_id
    )
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_builder_file(
        builder_name="test_buider", table_name="stories", process_id=last_process_id
    )
    tablevault.create_builder_file(
        copy_dir="./tests/test_data/test_data_db/stories/stories_index.yaml",
        table_name="stories",
    )
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.execute_instance("stories", process_id=last_process_id)
    copy_example_tv()
    table, _ = tablevault.get_dataframe("stories", full_artifact_path=False)
    tablevault.create_instance("stories", external_edit=True, copy=True)
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.write_instance(table, "stories", process_id=last_process_id)
    instances = tablevault.get_table_instances(table_name="stories")
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_instance(
        instance_id=instances[0], table_name="stories", process_id=last_process_id
    )
    copy_example_tv()
    last_process_id = tablevault.generate_process_id()
    process_ids.append(last_process_id)
    tablevault.delete_table("stories", process_id=last_process_id)
