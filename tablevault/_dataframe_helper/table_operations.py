import pandas as pd
import os
from typing import Optional
from tablevault._defintions.types import Cache
from typing import Any
from tablevault._defintions import tv_errors
from tablevault._defintions import constants
from tablevault._helper.metadata_store import MetadataStore
from tablevault._dataframe_helper import artifact
import pickle
from tablevault._helper.utils import gen_tv_id
from tablevault._helper.copy_write_file import CopyOnWriteFile
import json

# Currently only support nullable datatypes
nullable_map = {
    # signed integers
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "int": "Int64",
    # unsigned integers
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "uint": "UInt64",
    # floats (all mapped to Float64, the only nullable float)
    "float16": "Float64",
    "float32": "Float64",
    "float64": "Float64",
    "float": "Float64",
    "str": "string",
}

valid_nullable_dtypes = [
    # Numeric
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float64",
    # Boolean & string
    "boolean",
    "string",
    # Time types
    "datetime64[ns]",
    "datetime64[ns, tz]",  # e.g. DatetimeTZDtype("ns", "UTC")
    "timedelta64[ns]",
    # Categorical, Period, Interval
    "category",
    "Period[D]",
    "Period[M]",
    "Period[A]",  # etc.
    "interval",
    "object",
    "str",
    constants.ARTIFACT_DTYPE,
]


def update_dtypes(
    dtypes: dict[str, str],
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
) -> dict[str, str]:
    type_path = os.path.join(db_dir, table_name, instance_id, constants.DTYPE_FILE)
    with file_writer.open(type_path, "r") as f:
        dtypes_ = json.load(f)
    for col_name, dtype in dtypes:
        if dtype in nullable_map:
            dtype = nullable_map[dtype]
        if dtype not in valid_nullable_dtypes:
            raise tv_errors.TVTableError(
                "Currently only support select nullable data types"
            )
        dtypes_[col_name] = dtype
    with file_writer.open(type_path, "w") as f:
        json.dump(dtypes_, f)
    return dtypes


def write_dtype(
    dtypes, instance_id, table_name, db_dir, file_writer: CopyOnWriteFile
) -> dict[str, str]:
    table_dir = os.path.join(db_dir, table_name, instance_id)
    dtypes = {col: str(dtype) for col, dtype in dtypes.items()}
    for col_name, dtype in dtypes.items():
        if dtype in nullable_map:
            dtype = nullable_map[dtype]
        if dtype not in valid_nullable_dtypes:
            raise tv_errors.TVTableError(
                "Currently only support select nullable data types"
            )
        dtypes[col_name] = dtype
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    with file_writer.open(type_path, "w") as f:
        json.dump(dtypes, f)
    return dtypes


def write_table(
    df: pd.DataFrame,
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
) -> None:
    if constants.TABLE_INDEX in df.columns:
        df.drop(columns=constants.TABLE_INDEX, inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    file_writer.write_csv(table_path, df)


def get_table(
    instance_id: str,
    table_name: str,
    db_dir: str,
    rows: Optional[int] = None,
    artifact_dir: bool = False,
    get_index: bool = True,
    try_make_df: bool = True,
    file_writer: CopyOnWriteFile = None,
) -> pd.DataFrame:
    if file_writer is None:
        file_writer = CopyOnWriteFile(db_dir=db_dir, check_hardlink=False)

    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, instance_id)
    table_path = os.path.join(table_dir, constants.TABLE_FILE)
    type_path = os.path.join(table_dir, constants.DTYPE_FILE)
    if not os.path.exists(table_path):
        raise tv_errors.TVTableError(f"Table {table_name}: {instance_id} doesn't exist")
    with file_writer.open(type_path, "r") as f:
        content = f.read().strip()
        if not content:
            dtypes = {}
        else:
            dtypes = json.loads(content)
    if try_make_df:
        make_df(instance_id, table_name, db_dir, file_writer=file_writer)
    try:
        df = file_writer.read_csv(table_path, nrows=rows, dtype=dtypes)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        raise tv_errors.TVTableError(
            f"Error Reading Table (likely datatype mismatch): {e}"
        )
    if get_index:
        df.index.name = constants.TABLE_INDEX
        df = df.reset_index()
    if artifact_dir:
        a_dir = artifact.get_artifact_folder(instance_id, table_name, db_dir)
        df = artifact.df_artifact_to_path(df, a_dir)
    return df


def fetch_table_cache(
    external_dependencies: list,
    internal_dependencies: list,
    instance_id: str,
    table_name: str,
    db_metadata: MetadataStore,
    cache: Cache,
    file_writer: CopyOnWriteFile,
) -> Cache:
    if len(internal_dependencies) > 0:
        cache[constants.TABLE_SELF] = get_table(
            instance_id,
            table_name,
            db_metadata.db_dir,
            artifact_dir=True,
            file_writer=file_writer,
        )
    for dep in external_dependencies:
        table, _, instance, _, version = dep
        if (table, version) not in cache and (table, instance) not in cache:
            cache[(table, version)] = get_table(
                instance,
                table,
                db_metadata.db_dir,
                artifact_dir=True,
                file_writer=file_writer,
            )
        elif (table, instance) in cache:
            cache[(table, version)] = cache[(table, instance)]
    return cache


def update_table_columns(
    changed_columns: list[str],
    all_columns: list[str],
    dtypes: dict[str, str],
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
) -> None:
    df = get_table(instance_id, table_name, db_dir, file_writer=file_writer)
    columns = all_columns
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
    write_table(df, instance_id, table_name, db_dir, file_writer)
    write_dtype(df.dtypes, instance_id, table_name, db_dir, file_writer)


def save_new_columns(
    new_df: pd.DataFrame,
    col_names: list[str],  # might not be in the right order (!)
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
    primary_key: Optional[list[str]] = None,
) -> bool:
    df = get_table(
        instance_id,
        table_name,
        db_dir,
        get_index=False,
        try_make_df=False,
        file_writer=file_writer,
    )
    new_df.columns = col_names
    for col in col_names:
        new_df[col] = new_df[col].astype(df[col].dtype)
    if primary_key is None:
        diff_flag = not df[col_names].equals(new_df)
        if diff_flag:
            df_ = new_df.combine_first(df)
            df_ = df_[df.columns]
            df = df_
    elif len(primary_key) == 0:
        df_combined = new_df.combine_first(df)
        df_combined = df_combined[df.columns]
        df_combined = df_combined.loc[new_df.index]
        diff_flag = df_combined.equals(df)
        df = df_combined
    else:
        for pk in primary_key:
            if df[pk].dtype.name == constants.ARTIFACT_DTYPE:
                raise tv_errors.TVBuilderError(
                    "Primary key cannot be an artifact_string"
                )
        if new_df[primary_key].isnull().values.any():
            raise tv_errors.TVTableError("Primary key cannot have null value")
        new_df.index.name = constants.TABLE_INDEX
        new_df = new_df.reset_index()
        df_ = df.copy()
        df_ = df_.set_index(primary_key)
        new_df = new_df.set_index(primary_key)
        df_combined = new_df.combine_first(df_)
        df_combined = df_combined.reset_index()
        df_combined = df_combined.set_index([constants.TABLE_INDEX])

        df_combined = df_combined[df_combined.index.notna()]
        df_combined = df_combined.sort_index()
        df_combined = df_combined[df.columns]
        diff_flag = df.equals(df_combined)
        df = df_combined
    write_table(df, instance_id, table_name, db_dir, file_writer=file_writer)
    return not diff_flag


def append_old_df(
    primary_keys: list[str],
    instance_id: str,
    table_name: str,
    origin_id: str,
    origin_table: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
):
    df = get_table(
        instance_id,
        table_name,
        db_dir,
        get_index=False,
        try_make_df=False,
        file_writer=file_writer,
    )
    old_df = get_table(
        origin_id,
        origin_table,
        db_dir,
        get_index=False,
        try_make_df=False,
        file_writer=file_writer,
    )
    if primary_keys != []:
        mask = (
            old_df[primary_keys]
            .apply(tuple, axis=1)
            .isin(df[primary_keys].apply(tuple, axis=1))
        )
        diff = old_df.loc[~mask]
        df = pd.concat([diff, df], axis=0).reset_index(drop=True)
    else:
        df = pd.concat([old_df, df], axis=0).reset_index(drop=True)
    write_table(df, instance_id, table_name, db_dir, file_writer=file_writer)


def check_entry(
    index: Optional[int], columns: list[str], df: pd.DataFrame
) -> tuple[bool]:
    is_filled = True
    if index is not None:
        for col in columns:
            value = df.at[index, col]
            if pd.isna(value):
                is_filled = False
    else:
        is_filled = not df[columns].isna().values.any()
    return is_filled


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


def check_table(
    instance_id: str,
    table_name: str,
    origin_id: str,
    origin_table: str,
    db_dir: str,
    table_artifact: bool,
    file_writer: CopyOnWriteFile,
) -> None:
    df = get_table(
        instance_id, table_name, db_dir, artifact_dir=False, file_writer=file_writer
    )
    cols = [col for col, dt in df.dtypes.items() if dt.name == constants.ARTIFACT_DTYPE]
    df_custom = df[cols]
    if df_custom.shape[1] == 0:
        return
    artifact_paths = []
    artifact_dirs = []
    artifact_dirs.append(artifact.get_artifact_folder(instance_id, table_name, db_dir))
    if origin_table != "":
        try:
            dir = artifact.get_artifact_folder(
                origin_id, origin_table, db_dir, respect_temp=False
            )
            artifact_dirs.append(dir)
        except tv_errors.TVArtifactError:
            pass
    if table_artifact:
        artifact_dirs.append(
            os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
        )
    for _, row in df_custom.iterrows():
        for _, val in row.items():
            if not pd.isna(val) and val != "":
                artifact_exists = False
                artifact_path = os.path.join(artifact_dirs[0], val)
                for dir in artifact_dirs:
                    artifact_temp_path = os.path.join(dir, val)
                    if os.path.exists(artifact_temp_path):
                        if artifact_temp_path != artifact_path:
                            file_writer.linkfile(artifact_temp_path, artifact_path)
                        artifact_exists = True
                        artifact_paths.append(artifact_temp_path)
                        break
                if not artifact_exists:
                    raise tv_errors.TVTableError(f"Artifact {val} not found")

    for root, _, files in os.walk(artifact_dirs[0]):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if not is_hidden(file_path):
                if file_path not in artifact_paths:
                    raise tv_errors.TVTableError(f"Artifact {file_name} not indexed")


def check_changed_columns(
    y: pd.DataFrame,
    instance_id: str,
    table_name,
    db_dir,
    file_writer,
) -> list[str]:
    try:
        x = get_table(
            instance_id, table_name, db_dir, get_index=False, file_writer=file_writer
        )
    except tv_errors.TVTableError:
        return list(y.columns)

    if len(x) != len(y):
        return list(y.columns)

    new_cols = set(y.columns) - set(x.columns)
    common = set(y.columns).intersection(x.columns)
    changed = {col for col in common if not x[col].equals(y[col])}
    return list(new_cols | changed)


def write_df_entry(
    value: Any,
    index: int,
    col_name: str,
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
):
    file_name = gen_tv_id() + ".df.pkl"
    file_name = os.path.join(db_dir, table_name, instance_id, file_name)
    pkf = {"value": value, "index": index, "col_name": col_name}
    with file_writer.open(file_name, "wb") as f:
        pickle.dump(pkf, f)


def make_df(
    instance_id: str,
    table_name: str,
    db_dir: str,
    file_writer: CopyOnWriteFile,
    primary_key: Optional[list[str]] = None,
) -> bool:
    file_dir = os.path.join(db_dir, table_name, instance_id)
    pkl_files = [f for f in os.listdir(file_dir) if f.endswith(".df.pkl")]
    if not pkl_files:
        return False

    df = pd.DataFrame()
    records = []
    for file_name in pkl_files:
        file_path = os.path.join(file_dir, file_name)
        with file_writer.open(file_path, "rb") as f:
            pkf = pickle.load(f)
            records.append(
                {"index": pkf["index"], "col": pkf["col_name"], "val": pkf["value"]}
            )
    df = pd.DataFrame.from_records(records)
    df = df.pivot(index="index", columns="col", values="val")
    diff_flag = save_new_columns(
        df,
        col_names=df.columns,
        instance_id=instance_id,
        table_name=table_name,
        db_dir=db_dir,
        primary_key=primary_key,
        file_writer=file_writer,
    )

    for file_name in pkl_files:
        file_path = os.path.join(file_dir, file_name)
        file_writer.remove(file_path)
    return diff_flag
