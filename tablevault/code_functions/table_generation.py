import os
import pandas as pd
import shutil


def create_paper_table_from_folder(folder_dir, copies, artifact_folder):
    """
    Scan a folder for PDF files, copy them into an artifact directory, and build a DataFrame
    describing each paper.

    :param folder_dir: Path to the directory containing PDF files to process.
    :type folder_dir: str
    :param copies: Number of copies to make of each PDF. If ``copies == 1``, a single copy is made
                   under its original filename. If ``copies > 1``, multiple copies are made with a
                   suffix ``_0``, ``_1``, etc. appended to the base filename (before the ``.pdf``
                   extension).
    :type copies: int
    :param artifact_folder: Path to the directory where copies of the PDF files will be written.
    :type artifact_folder: str

    :return: A DataFrame with three columns: "paper_name", "artifact_name", and "original_path".
             Each row corresponds to one copied file artifact.
    :rtype: pandas.DataFrame
    """
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


def create_data_table_from_table(df, nrows=None):
    """
    Return a copy of the given DataFrame, optionally truncated to the first n rows.

    :param df: The source DataFrame to copy.
    :type df: pandas.DataFrame
    :param nrows: If provided, only the first `nrows` rows of `df` will be included in the returned
                  DataFrame. If ``None``, the entire DataFrame is copied.
    :type nrows: int or None

    :return: A copy of `df`, truncated to `nrows` rows if specified, otherwise a full copy.
    :rtype: pandas.DataFrame
    """
    if nrows is not None:
        return df.head(nrows).copy()
    return df.copy()


def create_data_table_from_csv(csv_file_path):
    """
    Read a CSV file into a DataFrame and return a copy.

    :param csv_file_path: Path to the CSV file to read.
    :type csv_file_path: str

    :return: A DataFrame containing the contents of the CSV file. The returned DataFrame is a copy
             of the data loaded from disk.
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(csv_file_path)
    return df.copy()


def create_data_table_from_list(vals):
    """
    Construct a DataFrame from a list of values by creating a single-column table.

    :param vals: A Python list of values. Each element in the list becomes a row in the output
                 DataFrame under the column name "temp_name".
    :type vals: list

    :return: A DataFrame with one column, "temp_name", where each row corresponds to an entry in
             `vals`.
    :rtype: pandas.DataFrame
    """
    return pd.DataFrame({"temp_name": vals})
