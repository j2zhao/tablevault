from tablevault.core import TableVault
import helper 


    

def basic_function(copy=True)-> list[str]:
    ids = []
    tablevault = TableVault('test_dir', 'jinjin', create=True,)
    id = tablevault.create_code_module("test")
    ids.append(id)
    id = tablevault.create_table('stories_TEST', allow_multiple_artifacts = False)
    ids.append(id)
    id = tablevault.rename_table('stories', 'stories_TEST')
    ids.append(id)
    id = tablevault.create_table('llm_storage', has_side_effects=True)
    ids.append(id)
    id = tablevault.create_table('llm_questions')
    ids.append(id)
    id = tablevault.create_instance("stories")
    ids.append(id)
    id = tablevault.create_instance("llm_storage", builders=["gen_llm_storage", "upload_openai"])
    ids.append(id)
    id = tablevault.create_instance("llm_questions", builders=["gen_llm_questions", "question_1","question_2", "question_3"])

    id = tablevault.create_builder_file(copy_dir="../test_data/test_data_db/stories/gen_stories.yaml", table_name="stories")
    ids.append(id)
    
    id = tablevault.create_builder_file(copy_dir="../test_data/test_data_db_selected/llm_storage", table_name="llm_storage")
    ids.append(id)
    id = tablevault.create_builder_file(copy_dir="../test_data/test_data_db_selected/llm_questions", table_name="llm_questions")
    ids.append(id)
    id = tablevault.execute_instance("stories")
    ids.append(id)
    id = tablevault.execute_instance("llm_storage")
    ids.append(id)
    id = tablevault.execute_instance("llm_questions")
    ids.append(id)
    if copy:
        helper.copy_test_dir("basic_function")
    return ids

def test_deletion():
    ids = []
    helper.copy_test_dir("test_dir", "basic_function")
    tablevault = TableVault('test_dir', 'jinjin')
    id = tablevault.delete_code_module("test")
    ids.append(id)
    id = tablevault.create_instance("stories", builders=["test_builder"])
    id = tablevault.delete_builder_file("test_builder", "stories")
    instances = tablevault.get_instances("stories")
    id = tablevault.delete_instance(instances[0], "stories")
    ids.append(id)
    id = tablevault.delete_table("llm_storage")
    ids.append(id)
    return ids


def evaluate_tests():
    print('TEST BASIC')
    ids = basic_function()
    helper.evaluate_operation_logging(ids)
    helper.evaluate_full_tables()
    print('TEST DELETION')
    ids = test_deletion()
    helper.evaluate_operation_logging(ids)
    helper.evaluate_deletion()
    #helper.clean_up_open_ai()

if __name__ == "__main__":
    #evaluate_tests()
    test_deletion()