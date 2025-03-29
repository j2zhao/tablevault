import pandas as pd
import os
from typing import Optional
from tablevault.defintions.types import Cache
from typing import Any
import json
import numpy as np
from tablevault.defintions.tv_errors import TVTableError
from tablevault.defintions import constants
from tablevault.helper.metadata_store import MetadataStore
from tablevault.prompts.utils import artifact

def write_table(
    df: pd.DataFrame, instance_id: str, table_name: str, db_dir: str
) -> None:
    if constants.TABLE_INDEX in df.columns:
        df.drop(columns=constants.TABLE_INDEX , inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    df.to_csv(table_path, index=False)
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    with open(type_path, "w") as f:
        json.dump(dtypes, f)


def get_table(
    instance_id: str, table_name: str, db_dir: str, rows: Optional[int] = None
) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    try:
        with open(type_path, "r") as f:
            content = f.read().strip()
            if not content:
                dtypes = {}
            else:
                dtypes = json.loads(content)
        df = pd.read_csv(table_path, nrows=rows, dtype=dtypes)
        df.index.name = constants.TABLE_INDEX
        df = df.reset_index()
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def fetch_table_cache(
    external_dependencies: list,
    instance_id: str,
    table_name: str,
    db_metadata:MetadataStore,
) -> Cache:
    table_meta = db_metadata.get_table_property()
    cache = {}
    temp = get_table(instance_id, table_name, db_metadata.db_dir)

    a_dir = artifact.get_artifact_folder(instance_id, 
                                         table_name,
                                         db_metadata.db_dir, 
                                         table_meta[table_name][constants.TABLE_ALLOW_MARTIFACT])
    
    cache["self"] = artifact.df_artifact_to_path(temp, a_dir)
    
    for dep in external_dependencies:
        table, _, instance, _, version = dep
        temp = get_table(instance, table, db_metadata.db_dir)
        a_dir = artifact.get_artifact_folder(instance, 
                                             table,
                                             db_metadata.db_dir, 
                                             table_meta[table][constants.TABLE_ALLOW_MARTIFACT])
    
        cache[(table, version)] = artifact.df_artifact_to_path(temp, a_dir)
    return cache


def update_table_columns(
    to_change_columns: list[str],
    all_columns: list[str],
    dtypes: dict[str, str],
    instance_id: str,
    table_name: str,
    db_dir: str,
) -> None:
    df = get_table(instance_id, table_name, db_dir)
    columns = list(dict.fromkeys(df.columns).keys()) + [
        col for col in all_columns if col not in df.columns
    ]
    for col in columns:
        if col not in all_columns:
            df.drop(col, axis=1)
        elif len(df) == 0:
            df[col] = pd.Series()
        elif col in to_change_columns or col not in df.columns:
            df[col] = pd.NA
        if col in dtypes:
            df[col] = df[col].astype(dtypes[col])
        else:
            df[col] = df[col].astype("string")
    write_table(df, instance_id, table_name, db_dir)


def merge_columns(
    columns: list[str], new_df: pd.DataFrame, old_df: pd.DataFrame
) -> tuple[pd.DataFrame, bool, pd.DataFrame, pd.DataFrame]:
    # Make sure new_df columns have the same dtype as in old_df
    all_columns = list(old_df.columns)
    for col in all_columns:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(old_df[col].dtype)
    
    # Merge on the provided key columns
    merged_df = pd.merge(new_df, old_df, how="left", on=columns)
    merged_df = merged_df[all_columns]
    
    # Check if key columns differ between merged_df and old_df
    diff_flag = not merged_df[columns].equals(old_df[columns])
    
    # # Use a full outer merge with indicator to get differences
    # diff_df = pd.merge(merged_df, old_df, how="outer", indicator=True)
    
    # # Rows that exist in merged_df but not in old_df
    # only_in_merged = diff_df[diff_df["_merge"] == "left_only"].drop(columns=["_merge"])
    
    # # Rows that exist in old_df but not in merged_df
    # only_in_old = diff_df[diff_df["_merge"] == "right_only"].drop(columns=["_merge"])
    
    return merged_df, diff_flag



def update_column(colunm: Any, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    colunm = _convert_series_to_dtype(colunm, df[col_name].dtype)
    df[col_name] = colunm
    return df


def update_entry(value: Any, index: int, column: str, df: pd.DataFrame) -> pd.DataFrame:
    value = _convert_to_dtype(value, df[column].dtype)
    df.at[index, column] = value
    return df


def check_entry(
    index: int, columns: list[str], df: pd.DataFrame
) -> tuple[bool, tuple[Any]]:
    is_filled = True
    entry = []
    for col in columns:
        value = df.at[index, col]
        if pd.isna(value):
            is_filled = False
        entry.append(value)
    return is_filled, tuple(entry)


def _convert_series_to_dtype(values: Any, dtype: Any) -> pd.Series:
    s = pd.Series(values)
    if pd.api.types.is_categorical_dtype(dtype):
        return s.astype(dtype)
    try:
        return s.astype(dtype)
    except Exception:

        def convert_element(x):
            if pd.isna(x):
                return x
            try:
                if hasattr(dtype, "type"):
                    return dtype.type(x)
                return np.dtype(dtype).type(x)
            except Exception as inner_e:
                raise TVTableError(
                    f"Could not convert {x!r} to dtype {dtype!r}: {inner_e}"
                )

        return s.apply(convert_element)


def _convert_to_dtype(value: Any, dtype: Any) -> Any:
    if pd.isna(value):
        return value

    if isinstance(dtype, pd.CategoricalDtype):
        return pd.Series([value]).astype(dtype)[0]
    try:
        if hasattr(dtype, "type"):
            return dtype.type(value)
        else:
            return np.dtype(dtype).type(value)
    except Exception as e:
        raise TVTableError(f"Could not convert value {value!r} to dtype {dtype!r}: {e}")
