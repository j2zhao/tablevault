import re
import os
import pandas as pd
import shutil


def clean_and_lowercase(text):
    # Use a regular expression to keep only letters
    letters_only = re.sub(r"[^a-zA-Z]", "", text)
    # Convert the result to lowercase
    return letters_only.lower()


def create_paper_table_from_folder(folder_dir, copies, artifact_folder):
    """Custom"""
    papers = []
    for file_name in os.listdir(folder_dir):
        if file_name.endswith(".pdf"):
            name, extension = file_name.split(".")
            path = os.path.join(folder_dir, file_name)
            if copies == 1:
                artifact_path = os.path.join(artifact_folder, file_name)
                shutil.copy(path, artifact_path)
                papers.append([name, file_name, path])
            else:
                for i in range(copies):
                    name_ = name + "_" + str(i)
                    file_name_ = name_ + "." + extension
                    artifact_path = os.path.join(artifact_folder, file_name_)
                    shutil.copy(path, artifact_path)
                    papers.append([name_, file_name_, path])
    df = pd.DataFrame(papers, columns=["paper_name", "artifact_name", "original_path"])
    return df


def create_data_table_from_table(df: pd.DataFrame, nrows=None):
    if nrows is not None:
        return df.head(nrows).copy()
    return df.copy()


def create_data_table_from_csv(csv_file_path: str):
    df = pd.read_csv(
        csv_file_path,
    )
    return df.copy()
