import numpy as np
from pandas.api.extensions import (
    ExtensionDtype,
    ExtensionArray,
    register_extension_dtype,
    take,
)
import os
import pandas as pd
from tablevault.defintions import constants
from tablevault.helper.file_operations import get_description
from typing import Any


def join_path(artifact: str, path_dir: str) -> str:
    if not pd.isna(artifact) and artifact != "":
        return os.path.join(path_dir, artifact)
    else:
        return ""


def df_artifact_to_path(df: pd.DataFrame, path_dir: str) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == constants.ARTIFACT_DTYPE:
            df[col] = df[col].apply(lambda x: join_path(x, path_dir))
            df[col] = df[col].astype(constants.ARTIFACT_DTYPE)
    return df


def get_artifact_folder(
    instance_id: str, table_name: str, db_dir: str, respect_temp=True
) -> str:
    instance_folder = os.path.join(
        db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER
    )
    if instance_id.startswith(constants.TEMP_INSTANCE) and respect_temp:
        return instance_folder
    table_data = get_description("", table_name, db_dir)
    if table_data[constants.TABLE_ALLOW_MARTIFACT]:
        return instance_folder
    else:
        table_folder = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
        return table_folder


def apply_artifact_path(
    arg: Any, instance_id: str, table_name: str, db_dir: str
) -> Any:
    artifact_path = get_artifact_folder(instance_id, table_name, db_dir)
    if isinstance(arg, str):
        arg = arg.replace(constants.ARTIFACT_REFERENCE, artifact_path)
        return arg
    elif isinstance(arg, list):
        return [
            apply_artifact_path(item, instance_id, table_name, db_dir) for item in arg
        ]
    elif isinstance(arg, set):
        return set(
            [apply_artifact_path(item, instance_id, table_name, db_dir) for item in arg]
        )
    elif isinstance(arg, dict):
        return {
            apply_artifact_path(
                k, instance_id, table_name, db_dir
            ): apply_artifact_path(v, instance_id, table_name, db_dir)
            for k, v in arg.items()
        }
    elif hasattr(arg, "__dict__"):
        for attr, val in vars(arg).items():
            val_ = apply_artifact_path(val, instance_id, table_name, db_dir)
            setattr(arg, attr, val_)
        return arg
    else:
        return arg


@register_extension_dtype
class ArtifactStringDtype(ExtensionDtype):
    name = constants.ARTIFACT_DTYPE
    type = str
    kind = "O"  # object

    @classmethod
    def construct_array_type(cls):
        return ArtifactStringArray


class ArtifactStringArray(ExtensionArray):
    def __init__(self, values):
        self._data = np.asarray(values, dtype=object)

    @property
    def dtype(self):
        return ArtifactStringDtype()

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        return cls(scalars)

    @classmethod
    def _from_sequence_of_strings(cls, strings, dtype=None, copy=False):
        materialized_values = []
        for s_val in strings:
            if isinstance(s_val, str):
                materialized_values.append(s_val)
            elif s_val is pd.NA:  # Already the desired NA type
                materialized_values.append(pd.NA)
            elif (
                s_val is None
                or (isinstance(s_val, float) and np.isnan(s_val))
                or (isinstance(s_val, np.datetime64) and np.isnat(s_val))
                or (isinstance(s_val, np.timedelta64) and np.isnat(s_val))
            ):  # Common NA types
                materialized_values.append(pd.NA)  # Canonicalize to pd.NA
            else:
                # If it's not a string or NA, it's an error for this method.
                raise ValueError(
                    f"Elements must be strings or NA-like (None, np.nan, pd.NA). "
                    f"Got value {s_val!r} of type {type(s_val)}"
                )
        return cls(materialized_values)

    def __getitem__(self, item):
        # When item is a slice or an array of indices, return a new ArtifactStringArray
        result = self._data[item]
        if isinstance(item, slice) or isinstance(item, np.ndarray):
            return ArtifactStringArray(result)
        return result

    def __len__(self):
        return len(self._data)

    def isna(self):
        return np.array([x is None for x in self._data])

    def take(self, indices, allow_fill=False, fill_value=None):
        # Use the pandas take utility to handle the operation
        result = take(self._data, indices, allow_fill=allow_fill, fill_value=fill_value)
        return ArtifactStringArray(result)

    def copy(self):
        return ArtifactStringArray(self._data.copy())

    def __array__(self, dtype=None):
        # Enable conversion to a NumPy array when needed (e.g. during evaluation)
        return np.asarray(self._data, dtype=dtype)

    def __repr__(self):
        return f"ArtifactStringArray({self._data})"

    def _concat_same_type(self, to_concat):
        # Concatenate the underlying data from each array in the list.
        concatenated = np.concatenate([x._data for x in to_concat])
        return ArtifactStringArray(concatenated)

    def __eq__(self, other):
        """
        Elementwise == comparison. Returns a pandas BooleanArray (nullable Boolean).
        """
        if isinstance(other, ArtifactStringArray):
            other_vals = other._data
        else:
            other_vals = other
        mask = self._data == other_vals
        return pd.array(mask, dtype="boolean")

    def equals(self, other):
        """
        True if `other` is same type, same length, and all values equal.
        """
        if not isinstance(other, ArtifactStringArray):
            return False
        # compare underlying numpy arrays for exact match (including None)
        return bool(np.array_equal(self._data, other._data))
    
    @classmethod
    def _from_factorized(cls, values, original):
        """
        Reconstruct this array after pd.factorize.
        `values` is the factorized values array, and `original` is
        the original array instance (in case you need its dtype or metadata).
        """
        # Option A: just wrap values (they should already be object-dtype strings/NA)
        return cls(values)