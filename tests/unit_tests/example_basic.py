from tablevault.core import TableVault
from rich import print as rprint


def basic_function():
    tv = TableVault("example_tv", author="jinjin", create=True)
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

    id = tv.create_builder_file(
        copy_dir="../test_data/test_data_db/stories/stories_index.yaml",
        table_name="stories",
    )
    ids.append(id)

    id = tv.create_builder_file(
        copy_dir="../test_data/test_data_db_selected/llm_storage",
        table_name="llm_storage",
    )
    ids.append(id)
    id = tv.create_builder_file(
        copy_dir="../test_data/test_data_db_selected/llm_questions",
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


if __name__ == "__main__":
    basic_function()
