'''
Test basic features of database -> more comprehensive testing happens when we actually run the system
'''

import subprocess
import shutil
import os

def copy_files_to_table(base_dir, db_dir, table_name):
    org_path = os.path.join(base_dir, table_name)
    new_path = os.path.join(db_dir, table_name)
    new_path = os.path.join(new_path, 'prompts')
    for file in os.listdir(org_path):
        if file.endswith('.yaml'):
            org_path_ = os.path.join(org_path, file)
            new_path_ = os.path.join(new_path, file)
            if os.path.exists(new_path_):
                os.remove(new_path_)
            shutil.copy2(org_path_, new_path_)

yaml_base_dir = './test_data/test_data_db'
db_dire = './test_database'



def copy_db(orig_dir = 'sample_database', new_dir = 'test_database'):
    if os.path.exists(new_dir):
        shutil.rmtree(new_dir)
    shutil.copytree(orig_dir, new_dir) 

def create_db():
    command = ["python", "execute_operation.py",  "-op", "database", "-db", "test_database", "-r"]
    subprocess.run(command)
    
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories", ]
    subprocess.run(command)
    yaml_base_dir = './test_data/test_data_db'
    db_dire = './test_database'
    copy_files_to_table(yaml_base_dir, db_dire, "stories")
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories", 
               "-p", "fetch_stories", "-gp", "fetch_stories"]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    #raise ValueError()
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "stories"]
    instance_id = subprocess.run(command, capture_output=True, text=True)
    #raise ValueError()
    instance_id = instance_id.stdout.split('\n')[0]

    return instance_id, 'stories', db_dire

def test_basic():
    # ACTION: create db
    print('CREATE DATABASE')
    command = ["python", "execute_operation.py",  "-op", "database", "-db", "test_database", "-r"]
    subprocess.run(command)
    # ACTION: create tables
    print('CREATE TABLES')
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories", ]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "table", "-db", "test_database", "-t", "llm_questions", "-m"]
    subprocess.run(command)

    # ACTION: copy files 
    print('COPY FILES')
    copy_files_to_table(yaml_base_dir, db_dire, "stories")
    copy_files_to_table(yaml_base_dir, db_dire, "llm_storage")
    copy_files_to_table(yaml_base_dir, db_dire, "llm_questions")

    # ACTION: create table instances 
    print('CREATE INSTANCES')   
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories", 
               "-p", "fetch_stories", "-gp", "fetch_stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
               "-p", "fetch_llm_storage", "upload_openai", "-gp", "fetch_llm_storage"]
    subprocess.run(command)
    # command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
    #            "-p", "fetch_llm_question", "question_1", "question_2", "question_3", "-gp", "fetch_llm_question"]
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
               "-p", "fetch_llm_question", "question_1", "question_2", "question_3", "-gp", "fetch_llm_question",  "-id", "version_1"]
    subprocess.run(command)
  
    # ACTION: execute updates
    print('UPDATE TABLES')   
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_1"]
    subprocess.run(command)



def test_delete_operations():
    copy_db()
    # delete table instance
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "stories"]
    story_instance_id = subprocess.run(command, capture_output=True, text=True)
    story_instance_id = story_instance_id.stdout.split('\n')[0]
    
    command = ["python", "execute_operation.py",  "-op", "delete_instance", 
               "-db", "test_database", "-t", 'stories', "-id", story_instance_id]
    subprocess.run(command)
    # delete table
    command = ["python", "execute_operation.py",  "-op", "delete_table", 
               "-db", "test_database", "-t", 'storage_id']
    subprocess.run(command)    


def test_multithread():
    # ACTION: create db
    print('CREATE DATABASE')
    command = ["python", "execute_operation.py",  "-op", "database", "-db", "test_database", "-r"]
    subprocess.run(command)
    # ACTION: create tables
    print('CREATE TABLES')
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories", ]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "table", "-db", "test_database", "-t", "llm_questions", "-m"]
    subprocess.run(command)
    # ACTION: copy files 
    print('COPY FILES')
    copy_files_to_table(yaml_base_dir, db_dire, "stories")
    copy_files_to_table(yaml_base_dir, db_dire, "llm_storage")
    copy_files_to_table(yaml_base_dir, db_dire, "llm_questions")

    # ACTION: create table instances 
    print('CREATE INSTANCES')   
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories", 
               "-p", "fetch_stories_5", "-gp", "fetch_stories_5"]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
               "-p", "fetch_llm_storage", "upload_openai_multi", "-gp", "fetch_llm_storage"]
    subprocess.run(command)

    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
               "-p", "fetch_llm_question", "question_1_multi", "-gp", "fetch_llm_question",  "-id", "version_1"]
    subprocess.run(command)
  
    # ACTION: execute updates
    print('UPDATE TABLES')   
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_1"]
    subprocess.run(command)


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

def cleanup():

    clean_up_open_ai()

if __name__ == '__main__':
    #test_multithread()
    cleanup()
    # #test_delete_operations()
    test_basic()
    copy_db(orig_dir = 'test_database', new_dir = 'sample_database')
    cleanup()
