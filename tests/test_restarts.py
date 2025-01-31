"""
Needs for manual interupts to different functions

"""
from test_basic import copy_db
import subprocess

def basic_restart(): #DONE
    copy_db()
    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_creation():
    copy_db()
    # set up new table -> but fail (add raise exception)
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "log_test", ]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_instance_creation():
    copy_db()
    # set up new table instnace -> but fail
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_deletion():
    copy_db()
    # delete table -> but fail (add raise exception)
    command = ["python", "execute_operation.py",  "-op", "delete_table", 
               "-db", "test_database", "-t", 'llm_storage']
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_instance_deletion():
    copy_db()
    # set up new table instance deletion -> but fail
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "stories"]
    story_instance_id = subprocess.run(command, capture_output=True, text=True)
    story_instance_id = story_instance_id.stdout.split('\n')[0]
    
    command = ["python", "execute_operation.py",  "-op", "delete_instance", 
               "-db", "test_database", "-t", 'stories', "-id", story_instance_id]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_execution_success(): # TODO
    # need to test many different points
    copy_db()
    # set up new table instance execution -> but fail -> and then continue
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
                "-pid", code_instance_id]
    subprocess.run(command)
    # execute code -> forced
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage", "-f"]
    subprocess.run(command)

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database"]
    subprocess.run(command)

def restart_table_execution_fail():
    # need to test many different points
    copy_db()
    # set up new table instance execution -> but fail -> and keep fail
    command = ["python", "execute_operation.py", "-op", "get_instances", "-db", "test_database", "-t", "llm_storage"]
    code_instance_id = subprocess.run(command, capture_output=True, text=True)
    code_instance_id = code_instance_id.stdout.split('\n')[0]
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
                "-pid", code_instance_id]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage", "-f"]
    subprocess.run(command)
    # get process id and fail
    command = ["python", "execute_operation.py", "-op", "active_logs", "-db", "test_database"]
    process_id = subprocess.run(command, capture_output=True, text=True)
    process_id = process_id.stdout.split('\n')[0]

    command = ["python", "execute_operation.py",  "-op", "restart", "-db", "test_database", '-ex', process_id]
    subprocess.run(command)

if __name__ == "__main__":
    restart_table_execution_fail()
    #restart_table_execution_success()
    #basic_restart()
    #restart_table_instance_deletion()
