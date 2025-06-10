import pandas as pd
import random
import string

def random_column_string(df, colunm_names, **kwargs):
    """
    Generate a new DataFrame of the same length as `df` with random string values in specified columns.

    :param df: DataFrame whose length determines the number of rows in the output.
    :type df: pandas.DataFrame
    :param colunm_names: List of column names for which to generate random string values.
    :type colunm_names: list of str
    :param kwargs: Additional keyword arguments (currently unused).
    :return: A DataFrame containing columns named in `colunm_names`, each filled with random
             alphanumeric strings of length 20. The number of rows matches `len(df)`.
    :rtype: pandas.DataFrame
    """
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    df_length = len(df)
    columns = {}
    for col in colunm_names:
        column = []
        for _ in range(df_length):
            val = "".join(random.choices(characters, k=20))
            column.append(val)
        columns[col] = column
    df = pd.DataFrame(columns)
    return df


def random_row_string(colunm_names, **kwargs):
    """
    Generate a single tuple of random string values corresponding to the given column names.

    :param colunm_names: List of column names for which to generate one random string value per column.
    :type colunm_names: list of str
    :param kwargs: Additional keyword arguments (currently unused).
    :return: A tuple of random alphanumeric strings of length 20, one for each entry in `colunm_names`.
    :rtype: tuple of str
    """
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    row = []
    for col in colunm_names:
        val = "".join(random.choices(characters, k=20))
        row.append(val)
    return tuple(row)
