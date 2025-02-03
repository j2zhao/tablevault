import re
import os
import pandas as pd


def clean_and_lowercase(text):
    # Use a regular expression to keep only letters
    letters_only = re.sub(r"[^a-zA-Z]", "", text)
    # Convert the result to lowercase
    return letters_only.lower()


def create_paper_table_from_folder(folder_dir, copies):
    """Custom"""
    papers = []
    for file in os.listdir(folder_dir):
        if file.endswith(".pdf"):
            name = file.split(".")[0]
            path = os.path.join(folder_dir, file)
            if copies == 1:
                papers.append([name, path])
            else:
                for i in range(copies):
                    name_ = name + "_" + str(i)
                    papers.append([name_, path])
    df = pd.DataFrame(papers, columns=["paper_name", "paper_path"])
    return df


def create_paper_table_from_folders(folder_name: str, table_name: str, db_dir: str):
    """Custom"""
    pass


def create_data_table_from_table(df: pd.DataFrame, columns: list[str]):
    return df[columns].copy()
