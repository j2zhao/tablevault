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
from tablevault.col_builders.utils import artifact
import shutil


def update_dtypes(
    dtypes: dict[str, str], instance_id: str, table_name: str, db_dir: str
) -> None:
    type_path = os.path.join(db_dir, table_name, instance_id, constants.DTYPE_FILE)
    with open(type_path, "r") as f:
        dtypes_ = json.load(f)
    dtypes_.update(dtypes)
    with open(type_path, "w") as f:
        json.dump(dtypes_, f)


def write_dtype(dtypes, instance_id, table_name, db_dir) -> None:
    table_dir = os.path.join(db_dir, table_name, instance_id)
    dtypes = {col: str(dtype) for col, dtype in dtypes.items()}
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    with open(type_path, "w") as f:
        json.dump(dtypes, f)


def write_table(
    df: pd.DataFrame, instance_id: str, table_name: str, db_dir: str
) -> None:
    if constants.TABLE_INDEX in df.columns:
        df.drop(columns=constants.TABLE_INDEX, inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    df.to_csv(table_path, index=False)


def get_table(
    instance_id: str,
    table_name: str,
    db_dir: str,
    rows: Optional[int] = None,
    artifact_dir: bool = False,
) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    if not os.path.exists(table_path):
        raise TVTableError("Table doesn't exist")
    with open(type_path, "r") as f:
        content = f.read().strip()
        if not content:
            dtypes = {}
        else:
            dtypes = json.loads(content)
    try:
        df = pd.read_csv(table_path)
        df = pd.read_csv(table_path, nrows=rows, dtype=dtypes)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        raise TVTableError(f"Error Reading Table (likely datatype mismatch): {e}")
    df.index.name = constants.TABLE_INDEX
    df = df.reset_index()
    if artifact_dir:
        a_dir = artifact.get_artifact_folder(instance_id, table_name, db_dir)
        df = artifact.df_artifact_to_path(df, a_dir)
    return df


def fetch_table_cache(
    external_dependencies: list,
    instance_id: str,
    table_name: str,
    db_metadata: MetadataStore,
    cache: Cache,
) -> Cache:
    temp = get_table(instance_id, table_name, db_metadata.db_dir, artifact_dir=False)
    cache[constants.OUTPUT_SELF] = temp.copy(deep=True)
    a_dir = artifact.get_artifact_folder(instance_id, table_name, db_metadata.db_dir)
    cache[constants.TABLE_SELF] = artifact.df_artifact_to_path(temp, a_dir)

    for dep in external_dependencies:
        table, _, instance, _, version = dep
        if (table, version) not in cache:
            cache[(table, version)] = get_table(
                instance, table, db_metadata.db_dir, artifact_dir=True
            )
    return cache


def update_table_columns(
    changed_columns: list[str],
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
        elif col in changed_columns or col not in df.columns:
            df[col] = pd.NA
        if col in dtypes:
            df[col] = df[col].astype(dtypes[col])
        else:
            df[col] = df[col].astype("string")
    write_table(df, instance_id, table_name, db_dir)
    write_dtype(df.dtypes, instance_id, table_name, db_dir)


def merge_columns(
    columns: list[str], new_df: pd.DataFrame, old_df: pd.DataFrame
) -> tuple[pd.DataFrame, bool]:
    all_columns = list(old_df.columns)
    for col in all_columns:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(old_df[col].dtype)

    merged_df = pd.merge(new_df, old_df, how="left", on=columns)
    merged_df = merged_df[all_columns]

    diff_flag = not merged_df[columns].equals(old_df[columns])

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
    if isinstance(dtype, pd.CategoricalDtype):
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


def is_hidden(file_path: str) -> bool:
    system_patterns = (
        ".DS_Store",  # macOS
        "Thumbs.db",  # Windows
        ".localized",  # macOS
        "$RECYCLE.BIN",  # Windows
        "System Volume Information",  # Windows
        ".Spotlight-V100",  # macOS
        ".Trashes",  # macOS
        "desktop.ini",  # Windows
    )
    if file_path.startswith("."):
        return True
    if file_path in system_patterns:
        return True
    if os.name == "nt":
        try:
            attrs = os.stat(file_path).st_file_attributes
            if attrs & 2:
                return True
        except OSError:
            return False

    return True


def check_table(instance_id: str, table_name: str, db_dir: str) -> None:
    df = get_table(instance_id, table_name, db_dir, artifact_dir=False)
    cols = [col for col, dt in df.dtypes.items() if dt.name == constants.ARTIFACT_DTYPE]
    df_custom = df[cols]
    if df_custom.shape[1] == 0:
        return
    artifact_paths = []
    artifact_temp_dir = artifact.get_artifact_folder(instance_id, table_name, db_dir)
    artifact_main_dir = artifact.get_artifact_folder(
        instance_id, table_name, db_dir, respect_temp=False
    )

    for _, row in df_custom.iterrows():
        for _, val in row.items():
            if not pd.isna(val) and val != "":
                artifact_temp_path = os.path.join(artifact_temp_dir, val)
                artifact_main_path = os.path.join(artifact_main_dir, val)

                if not os.path.exists(artifact_temp_path):
                    if os.path.exists(artifact_main_path):
                        shutil.copy2(artifact_main_path, artifact_temp_path)
                    else:
                        raise TVTableError(f"Artifact {val} not found")
                artifact_paths.append(artifact_temp_path)

    for root, _, files in os.walk(artifact_temp_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if not is_hidden(file_path):
                if file_path not in artifact_paths:
                    raise TVTableError(f"Artifact {file_name} not indexed")


def check_changed_columns(
    y: pd.DataFrame, instance_id: str, table_name, db_dir
) -> list[str]:
    try:
        x = get_table(instance_id, table_name, db_dir)
    except TVTableError:
        return list(y.columns)

    if len(x) != len(y):
        return list(y.columns)

    new_cols = set(y.columns) - set(x.columns)
    common = set(y.columns).intersection(x.columns)
    changed = {col for col in common if not x[col].equals(y[col])}

    return list(new_cols | changed)
