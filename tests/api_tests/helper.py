import os
from tablevault.core import TableVault
import shutil
from tablevault._helper import user_lock


def copy_example_tv(new_dir_name="example_tv_copy", old_dir_name="example_tv"):
    if os.path.exists(new_dir_name):
        user_lock.set_writable(new_dir_name)
    if os.path.isdir(new_dir_name):
        shutil.rmtree(new_dir_name)
    shutil.copytree(old_dir_name, new_dir_name, dirs_exist_ok=True)


def evaluate_operation_logging(ids):
    # check all ids are logged
    tablevault = TableVault("example_tv", "jinjin")
    for id in ids:
        assert tablevault.get_process_completion(id)
    # check no processes
    processes = tablevault.get_active_processes()
    assert len(processes) == 0
    # checked no locks
    lock_dir = "example_tv/locks"
    for root, dirs, files in os.walk(lock_dir):
        for file in files:
            assert not file.endswith(".shlock")
            assert not file.endswith(".exlock")
    # check no temp files
    temp_dir = "example_tv/_temp"
    for entry in os.listdir(temp_dir):
        assert entry.startswith(".")


def evaluate_empty_tables(
    tables=["stories", "llm_storage", "llm_questions"]
):
    tablevault = TableVault("example_tv", "jinjin")
    for table_name in tables:
        df, _ = tablevault.get_dataframe(table_name)
        df = df.dropna()
        assert len(df) == 0

def evaluate_full_tables(
    tables=["stories", "llm_storage", "llm_questions"], num_entries: int = 1
):
    tablevault = TableVault("example_tv", "jinjin")
    for table_name in tables:
        df, _ = tablevault.get_dataframe(table_name)
        assert not df.isnull().values.any()
        assert len(df) == num_entries


def evaluate_deletion():
    temp_dir = "example_tv/metadata/ARCHIVED_TRASH/llm_storage"
    assert os.path.exists(temp_dir)
    entries = os.listdir(temp_dir)
    assert "table.csv" not in entries
    tablevault = TableVault("example_tv", "jinjin")
    instances = tablevault.get_table_instances("stories")
    assert len(instances) == 0
    temp_dir = "example_tv/metadata/ARCHIVED_TRASH/stories"
    assert os.path.exists(temp_dir)
    entries = os.listdir(temp_dir)
    assert "table.csv" not in entries


def get_all_file_paths(folder):
    file_paths = set()
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, folder)
            if not str(rel_path).endswith(".lock"):
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
            print(f"Files missing in {folder2}:")
            for file in sorted(missing_in_folder2):
                print(f"  {file}")
        if missing_in_folder1:
            print(f"\nFiles missing in {folder1}:")
            for file in sorted(missing_in_folder1):
                print(f"  {file}")
        return False
