import pandas as pd
import random
import string


def random_column_string(df, colunm_names, **kwargs):
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
    characters = string.ascii_letters + string.digits  # a-zA-Z0-9
    row = []
    for col in colunm_names:
        val = "".join(random.choices(characters, k=20))
        row.append(val)
    return tuple(row)
