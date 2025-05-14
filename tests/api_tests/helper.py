import os
from tablevault.core import TableVault
import shutil

def copy_test_dir():
    if os.path.isdir('test_dir_copy'):
        shutil.rmtree('test_dir_copy')
    shutil.copytree('test_dir', 'test_dir_copy', dirs_exist_ok=True)


def clean_up_open_ai(key_file = "../test_data/open_ai_key/key.txt"):
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

def evaluate_operation_logging(ids):
    # check all ids are logged
    tablevault = TableVault('test_dir', 'jinjin')
    for id in ids:
        print(id)
        assert tablevault.check_process_completion(id)
    # check no processes
    processes = tablevault.active_processes()
    print(processes)
    assert len(processes) == 0
    # checked no locks
    lock_dir = 'test_dir/locks'
    for root, dirs, files in os.walk(lock_dir):
        for file in files:
            assert not file.endswith('.shlock')
            assert not file.endswith('.exlock')
    # check no temp files
    temp_dir = 'test_dir/_temp'
    for entry in os.listdir(temp_dir):
        print(entry)
        assert entry.startswith('.')


def evaluate_full_tables(tables = ["stories", "llm_storage","llm_questions" ], num_entries:int = 1):
    tablevault = TableVault('test_dir', 'jinjin')
    for table_name in tables:
        df = tablevault.fetch_table(table_name)
        assert not df.isnull().values.any()
        assert len(df) == num_entries


def evaluate_deletion():
    temp_dir = 'test_dir/llm_storage'
    entries = os.listdir(temp_dir)
    assert 'table.csv' not in entries
    tablevault = TableVault('test_dir', 'jinjin')
    instances = tablevault.list_instances('stories')
    assert len(instances) == 0
    temp_dir = 'test_dir/stories'
    entries = os.listdir(temp_dir)
    assert 'table.csv' not in entries

def get_all_file_paths(folder):
    file_paths = set()
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, folder)
            file_paths.add(rel_path)
    return file_paths

def compare_folders(folder1, folder2):
    folder1_files = get_all_file_paths(folder1)
    folder2_files = get_all_file_paths(folder2)

    missing_in_folder2 = folder1_files - folder2_files
    missing_in_folder1 = folder2_files - folder1_files

    if not missing_in_folder2 and not missing_in_folder1:
        print("✅ Both folders have the same file paths.")
        return True
    
    else:
        print("❌ Folders are different.\n")
        if missing_in_folder2:
            print("Files missing in folder2:")
            for file in sorted(missing_in_folder2):
                print(f"  {file}")
        if missing_in_folder1:
            print("\nFiles missing in folder1:")
            for file in sorted(missing_in_folder1):
                print(f"  {file}")
        return False