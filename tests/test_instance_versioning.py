'''
This is way more complicated than i thought T_T

'''
from test_basic import copy_db
import subprocess
import os
import shutil

def replace_prompt(prompt, instance_id, table_name, base_dir = './test_data/test_data_db', db_dir = './test_database', prev_prompt= None):
    org_path = os.path.join(base_dir, table_name, prompt)
    new_dir = os.path.join(db_dir, table_name, instance_id, 'prompts')
    new_path = os.path.join(new_dir, prompt)
    if prev_prompt != None:
        prev_prompt_path = os.path.join(new_dir, prev_prompt)
        if os.path.exists(prev_prompt_path):
            print(prev_prompt_path)
            os.remove(prev_prompt_path)
    shutil.copy2(org_path, new_path)

def copy_story(base_dir= './test_data/stories', story_name = 'The_Clockmakers_Secret.pdf'):
    org_path = os.path.join(base_dir, story_name)
    new_name = story_name.split(".")[0] + '_copy.pdf'
    new_path = os.path.join(base_dir, new_name)
    shutil.copy2(org_path, new_path)

def delete_story(base_dir= './test_data/stories', story_name = 'The_Clockmakers_Secret_copy.pdf'):
    story_path  = os.path.join(base_dir, story_name)
    os.remove(story_path)

def test_copy_instance_no_change():
    copy_db()

    # get instances
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_questions"]
    llm_instance_id = subprocess.run(command, capture_output=True, text=True)
    llm_instance_id = llm_instance_id.stdout.split('\n')[0]

    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]

    # copy instance with llm prompts
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
                "-pid", llm_instance_id,  "-id", "version_2"]
    subprocess.run(command)

    # copy instance with code prompts
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
                "-pid", code_instance_id]
    subprocess.run(command)

    # # nothing should execute - execute llm
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_2"]
    subprocess.run(command)

    # # nothing should execute - excute code
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)



def test_copy_instance_prompt_change():
    # change prompt name
    copy_db()

    # get instances
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_questions"]
    llm_instance_id = subprocess.run(command, capture_output=True, text=True)
    llm_instance_id = llm_instance_id.stdout.split('\n')[0]

    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]

    # copy instance with llm prompts
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
                "-pid", llm_instance_id,  "-id", "version_2"]
    subprocess.run(command)

    # change prompt
    replace_prompt("question_1a.yaml", "TEMP_version_2", "llm_questions", prev_prompt= "question_1.yaml")

    #  execute llm
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_2"]
    subprocess.run(command)

def test_copy_dep_change():
    # change prompt name
    copy_db()

    # get instances
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_questions"]
    llm_instance_id = subprocess.run(command, capture_output=True, text=True)
    llm_instance_id = llm_instance_id.stdout.split('\n')[0]

    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]

    # copy instance with llm prompts
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
                "-pid", llm_instance_id,  "-id", "version_2"]
    subprocess.run(command)
    # copy instance with code prompts
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
                "-pid", code_instance_id]
    subprocess.run(command)
    print('execute storage')
    # execute code -> forced
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage", "-f"]
    subprocess.run(command)
    print('execute llm')
    #  execute llm
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_2"]
    subprocess.run(command)

def test_new_row_change():
    copy_db()

    # make new story file
    copy_story()

    # get previous story id
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "stories"]
    story_instance_id = subprocess.run(command, capture_output=True, text=True)
    story_instance_id = story_instance_id.stdout.split('\n')[0]

    # make new story instance 
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories", 
                "-pid", story_instance_id]
    subprocess.run(command)

    # execute story instance
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)

    
    # execute code instance
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]
        
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
                "-pid", code_instance_id]
    subprocess.run(command)

    # # nothing should execute - execute llm
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    
    # # execute llm instance
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_questions"]
    llm_instance_id = subprocess.run(command, capture_output=True, text=True)
    llm_instance_id = llm_instance_id.stdout.split('\n')[0]
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_questions", 
                "-pid", llm_instance_id,  "-id", "version_2"]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_questions", "-id", "version_2"]
    subprocess.run(command)

    # delete story instance
    delete_story()

if __name__ == "__main__":
    # test_copy_instance_no_change()
    # test_copy_instance_prompt_change()
    # test_copy_dep_change()
    
    test_new_row_change()