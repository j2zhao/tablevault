from tablevault.core import TableVault
from helper import evaluate_operation_logging, clean_up_open_ai

def basic_function()-> list[str]:
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.setup_table('llm_storage', has_side_effects=True)
    ids.append(id)
    id = tablevault.setup_table('llm_questions')
    ids.append(id)
    id = tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
    ids.append(id)
    id = tablevault.copy_files("../test_data/test_data_db/llm_storage", table_name="llm_storage")
    ids.append(id)
    id = tablevault.copy_files("../test_data/test_data_db/llm_questions", table_name="llm_questions")
    ids.append(id)
    id = tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
    ids.append(id)
    id = tablevault.setup_temp_instance("llm_storage", prompt_names=["gen_llm_storage", "upload_openai"])
    ids.append(id)
    id = tablevault.setup_temp_instance("llm_questions", prompt_names=["gen_llm_questions", "question_1","question_2", "question_3"])
    ids.append(id)
    id = tablevault.execute_instance("stories")
    ids.append(id)
    id = tablevault.execute_instance("llm_storage")
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    return ids

def test_multi_execution_instance():
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.copy_files("../test_data/test_data_db_selected/stories", table_name="stories")
    ids.append(id)
    id = tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"],execute=True, background_execute=True)
    ids.append(id)
    return ids

def test_multi_execution_table():
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    id = tablevault.setup_table('stories', allow_multiple_artifacts = False, 
                           yaml_dir="../test_data/test_data_db_selected/stories",create_temp=True, execute=True)
    return [id]

def test_multi_execution_db():
    tablevault = TableVault('test_dir', 'jinjin', create=True, yaml_dir="../test_data/test_data_db_selected", execute=True)
    return []

def test_deletion():
    basic_function()
    ids = []
    tablevault = TableVault('test_dir', 'jinjin')
    instances = tablevault.list_instances("stories")
    id = tablevault.delete_instance(instances[0], "stories")
    ids.append(id)
    id = tablevault.delete_table("llm_storage")
    ids.append(id)
    return ids


def evaluate_tests():
    ids = basic_function()
    evaluate_operation_logging(ids)
    ids = test_multi_execution_instance()
    evaluate_operation_logging(ids)
    ids = test_multi_execution_table()
    evaluate_operation_logging(ids)
    ids = test_multi_execution_db()
    evaluate_operation_logging(ids)
    ids = test_deletion()
    evaluate_operation_logging(ids)
    clean_up_open_ai()

if __name__ == "__main__":
    evaluate_tests()