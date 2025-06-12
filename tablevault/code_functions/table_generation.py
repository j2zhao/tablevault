import os
import pandas as pd
import shutil


def create_paper_table_from_folder(
    folder_dir, copies, artifact_folder, extension=".pdf"
):
    """Scan a folder for files, copy them, and create a descriptive DataFrame.

    Scans a specified folder for files with a given extension, copies them
    to an artifact directory, and builds a pandas DataFrame that catalogs
    the original and copied files.

    Parameters
    ----------
    folder_dir : str
        Path to the directory containing the files to process.
    copies : int
        Number of copies to make of each file. If `copies > 1`, a numeric
        suffix (e.g., `_0`, `_1`) is appended to the filename.
    artifact_folder : str
        Path to the directory where copies of the files will be written.
    extension : str, optional
        The file extension to filter for, by default ".pdf".

    Returns
    -------
    pandas.DataFrame
        A DataFrame with three columns: "file_name", "artifact_name", and
        "original_path". Each row corresponds to one copied file.
    """
    papers = []
    for file_name in os.listdir(folder_dir):
        if file_name.endswith(extension):
            name, ext = os.path.splitext(file_name)
            path = os.path.join(folder_dir, file_name)
            if copies == 1:
                artifact_path = os.path.join(artifact_folder, file_name)
                shutil.copy(path, artifact_path)
                papers.append([name, file_name, path])
            else:
                for i in range(copies):
                    name_ = f"{name}_{i}"
                    file_name_ = f"{name_}{ext}"
                    artifact_path = os.path.join(artifact_folder, file_name_)
                    shutil.copy(path, artifact_path)
                    papers.append([name_, file_name_, path])
    df = pd.DataFrame(papers, columns=["file_name", "artifact_name", "original_path"])
    return df


def create_data_table_from_table(df, nrows=None, random_sample=False):
    """Return a copy of a DataFrame with optional truncation or sampling.

    Parameters
    ----------
    df : pandas.DataFrame
        The source DataFrame to copy.
    nrows : int, optional
        The number of rows to include. If None, the entire DataFrame is
        copied. By default None.
    random_sample : bool, optional
        If True and `nrows` is specified, randomly sample `nrows` rows
        without replacement. If False, the first `nrows` are taken.
        By default False.

    Returns
    -------
    pandas.DataFrame
        A modified copy of the input DataFrame.
    """
    if nrows is None:
        return df.copy()

    if random_sample:
        return df.sample(n=nrows, replace=False).copy()

    return df.head(nrows).copy()


def create_data_table_from_csv(csv_file_path):
    """Read a CSV file into a DataFrame.

    Parameters
    ----------
    csv_file_path : str
        Path to the CSV file to read.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing a copy of the data from the CSV file.
    """
    df = pd.read_csv(csv_file_path)
    return df.copy()


def create_data_table_from_list(vals):
    """Create a single-column DataFrame from a list of values.

    Parameters
    ----------
    vals : list
        A list of values where each element will become a row.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with one column named "temp_name".
    """
    return pd.DataFrame({"temp_name": vals})
