from tablevault.core import TableVault
import os

def clean_up_open_ai(key_file = "open_ai_key/key.txt"):
    import openai
    from tqdm import tqdm
    with open(key_file, 'r') as f:
        secret = f.read()
        os.environ["OPENAI_API_KEY"] = secret
    client = openai.OpenAI()
    files = list(client.files.list())
    vector_stores = list(client.beta.vector_stores.list())
    my_assistants = list(client.beta.assistants.list())
    for store in tqdm(vector_stores):
        try:
          client.beta.vector_stores.delete(
            vector_store_id=store.id
          )
        except:
            pass
    for f in tqdm(files):
        try:
          client.files.delete(
            file_id=f.id
          )
        except:
          pass
    
    for assistant in tqdm(my_assistants):
        try:
            client.beta.assistants.delete(assistant.id)
        except:
            pass
    
    print(client.beta.vector_stores.list())
    print(client.files.list())
    print(client.beta.assistants.list())

def basic_function(cleanup = True):
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    tablevault.setup_table('stories', allow_multiple_artifacts = False)
    tablevault.setup_table('llm_storage', has_side_effects=True)
    tablevault.setup_table('llm_questions')
    tablevault.copy_files("../test_data/test_data_db/stories", table_name="stories")
    tablevault.copy_files("../test_data/test_data_db/llm_storage", table_name="llm_storage")
    tablevault.copy_files("../test_data/test_data_db/llm_questions", table_name="llm_questions")
    tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"])
    tablevault.setup_temp_instance("llm_storage", prompt_names=["gen_llm_storage", "upload_openai"])
    tablevault.setup_temp_instance("llm_questions", prompt_names=["gen_llm_questions", "question_1","question_2", "question_3"])
    tablevault.execute_instance("stories")
    tablevault.execute_instance("llm_storage")
    tablevault.execute_instance("llm_questions")
    #tablevault.print_active_processes(print_all=True)
    #tablevault.print_active_processes(print_all=False)
    #instances = tablevault.list_instances(table_name= "stories")
    #print(instances)
    #tablevault.delete_table("llm_questions")
    #tablevault.delete_instance(instance_id=instances[0], table_name="stories")
    if cleanup:
        clean_up_open_ai(key_file = "../test_data/open_ai_key/key.txt")

def test_multi_execution_instance():
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    tablevault.setup_table('stories', allow_multiple_artifacts = False)
    tablevault.copy_files("../test_data/test_data_db_selected/stories", table_name="stories")
    tablevault.setup_temp_instance("stories", prompt_names=["gen_stories"],execute=True, background_execute=True)

def test_multi_execution_table():
    tablevault = TableVault('test_dir', 'jinjin', create=True)
    tablevault.setup_table('stories', allow_multiple_artifacts = False, 
                           yaml_dir="../test_data/test_data_db_selected/stories",create_temp=True, execute=True)

def test_multi_execution_db():
    tablevault = TableVault('test_dir', 'jinjin', create=True, yaml_dir="../test_data/test_data_db_selected", execute=True)

if __name__ == "__main__":
    #basic_function(cleanup=False)
    test_multi_execution_db()