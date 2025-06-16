import pandas as pd
import random
import string


def random_column_string(df, column_names, **kwargs):
    """Generate a DataFrame with random string columns.

    This function creates a new DataFrame of the same length as an input
    DataFrame `df`, with random string values in the specified columns.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose length determines the number of rows in the
        output.
    column_names : list of str
        A list of column names for which to generate random string values.
    **kwargs
        Additional keyword arguments (currently unused).

    Returns
    -------
    pd.DataFrame
        A new DataFrame containing the specified columns, each filled with
        random alphanumeric strings of length 20. The number of rows
        matches the length of `df`.
    """
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    df_length = len(df)
    columns = {}
    for col in column_names:
        column = []
        for _ in range(df_length):
            val = "".join(random.choices(characters, k=20))
            column.append(val)
        columns[col] = column
    df = pd.DataFrame(columns)
    return df


def random_row_string(column_names, **kwargs):
    """Generate a single row of random string values.

    Parameters
    ----------
    column_names : list of str
        A list of column names for which to generate one random string value
        per column.
    **kwargs
        Additional keyword arguments (currently unused).

    Returns
    -------
    list
        A list of random alphanumeric strings, each of length 20,
        corresponding to the entries in `column_names` or a single string
        if `column_names` has only one entry
    """
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    row = []
    for col in column_names:
        val = "".join(random.choices(characters, k=20))
        row.append(val)
    if len(row) == 1:
        return row[0]
    return row


def random_gen_string(column_names, df, **kwargs):
    """Generate a single row of random string values.

    Parameters
    ----------
    column_names : list of str
        A list of column names for which to generate one random string value
        per column.
    **kwargs
        Additional keyword arguments (currently unused).

    Returns
    -------
    list
        A list of random alphanumeric strings, each of length 20,
        corresponding to the entries in `column_names`.
    """
    for i in range(len(df)):
        characters = string.ascii_letters + string.digits  # a-zA-Z0-9
        row = []
        if len(column_names) > 1:
            for col in column_names:
                val = "".join(random.choices(characters, k=20))
                row.append(val)
        else:
            row = "".join(random.choices(characters, k=20))
        yield row
