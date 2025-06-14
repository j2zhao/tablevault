import numpy as np
from pandas.api.extensions import (
    ExtensionDtype,
    ExtensionArray,
    register_extension_dtype,
    take,
)
import os
import pandas as pd
from tablevault._defintions import constants
from tablevault._helper.file_operations import get_description
from typing import Any
import logging


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
    if instance_id != "":
        instance_data = get_description(instance_id, table_name, db_dir)
        success = instance_data[constants.DESCRIPTION_SUCCESS]
    else:
        success = True
    table_folder = os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)
    if table_data[constants.TABLE_ALLOW_MARTIFACT] or not success:
        return instance_folder
    else:
        return table_folder


def apply_artifact_path(
    arg: Any,
    instance_id: str,
    table_name: str,
    db_dir: str,
    process_id: str,
) -> Any:
    artifact_path = get_artifact_folder(instance_id, table_name, db_dir)
    if isinstance(arg, str):
        arg = arg.replace(constants.ARTIFACT_REFERENCE, artifact_path)
        arg = arg.replace(constants.PROCESS_ID_REFERENCE, process_id)
        return arg
    elif isinstance(arg, list):
        return [
            apply_artifact_path(item, instance_id, table_name, db_dir, process_id)
            for item in arg
        ]
    elif isinstance(arg, set):
        return set(
            [
                apply_artifact_path(item, instance_id, table_name, db_dir, process_id)
                for item in arg
            ]
        )
    elif isinstance(arg, dict):
        return {
            apply_artifact_path(
                k, instance_id, table_name, db_dir, process_id
            ): apply_artifact_path(v, instance_id, table_name, db_dir, process_id)
            for k, v in arg.items()
        }
    elif hasattr(arg, "__dict__"):
        for attr, val in vars(arg).items():
            val_ = apply_artifact_path(val, instance_id, table_name, db_dir, process_id)
            try:
                setattr(arg, attr, val_)
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.info(f"Immutable Object {e}")
        return arg
    else:
        return arg


# ──────────────────────────────────────────────────────────────────────────
# Helper utilities
# ──────────────────────────────────────────────────────────────────────────
def _safe_isna(obj: Any) -> bool:
    """A robust NA‐checker that never raises on unhashable types."""
    if isinstance(obj, (list, dict)):
        return False
    try:
        return pd.isna(obj)
    except Exception:
        return False


def _compare_for_eq(a: Any, b: Any) -> Any:
    """Return True / False / <NA> for element-wise equality."""
    if _safe_isna(a):
        return pd.NA
    if pd.api.types.is_scalar(b) and _safe_isna(b):
        return pd.NA
    try:
        return bool(a == b)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────
# ArtifactString
# ──────────────────────────────────────────────────────────────────────────
@register_extension_dtype
class ArtifactStringDtype(ExtensionDtype):
    name = constants.ARTIFACT_DTYPE
    type = str
    kind = "O"

    @classmethod
    def construct_array_type(cls):
        return ArtifactStringArray

    @property
    def na_value(self):
        return pd.NA


class ArtifactStringArray(ExtensionArray):
    """Stores plain Python strings or <NA>."""

    def __init__(self, values: list[Any], copy: bool = False):
        cleaned: list[Any] = []
        for v in values:
            if isinstance(v, str) or _safe_isna(v):
                cleaned.append(v)
            else:
                raise ValueError("ArtifactStringArray elements must be str or NA.")
        arr = np.asarray(cleaned, dtype=object)
        self._data = arr.copy() if copy else arr

    # ---- core EA API ----
    @property
    def dtype(self):
        return ArtifactStringDtype()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        res = self._data[item]
        if isinstance(item, (slice, list, np.ndarray, pd.Index)):
            return ArtifactStringArray(res)
        else:
            return res

    def __setitem__(self, key, value):
        # determine positions to assign
        if pd.api.types.is_scalar(key):
            idxs = [key]
        else:
            # slice, boolean mask, integer list, etc.
            idxs = np.arange(len(self._data))[key]
        # build list of values to assign
        if pd.api.types.is_scalar(value):
            vals = [value] * len(idxs)
        else:
            vals = list(value)
            if len(vals) != len(idxs):
                raise ValueError(
                    f"Values length ({len(vals)}) doesn't match index ({len(idxs)})."
                )
        # validate and assign
        for i, v in zip(idxs, vals):
            if isinstance(v, str) or _safe_isna(v):
                self._data[i] = v
            else:
                raise ValueError("ArtifactStringArray elements must be str or NA.")

    def isna(self):
        return np.fromiter((pd.isna(x) for x in self._data), dtype=bool)

    def take(self, indices, allow_fill=False, fill_value=None):
        out = take(self._data, indices, allow_fill=allow_fill, fill_value=fill_value)
        return ArtifactStringArray(out)

    def copy(self):
        return ArtifactStringArray(self._data.copy())

    def __array__(self, dtype: np.dtype | None = None, copy: bool | None = None):
        src = self._data.copy() if copy else self._data
        return np.asarray(src, dtype=dtype if dtype is not None else object, copy=False)

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        return cls(scalars, copy=copy)

    @classmethod
    def _from_sequence_of_strings(cls, strings, dtype=None, copy=False):
        return cls(strings, copy=copy)

    @classmethod
    def _from_factorized(cls, values, original):
        return cls(values)

    @property
    def nbytes(self):
        return self._data.nbytes

    # ---- comparisons ----
    def __eq__(self, other):
        if isinstance(other, ArtifactStringArray):
            other_iter = other._data
        elif pd.api.types.is_scalar(other):
            other_iter = [other] * len(self)
        elif pd.api.types.is_list_like(other) and not isinstance(other, (str, bytes)):
            if len(other) != len(self):
                return pd.array([pd.NA] * len(self), dtype="boolean")
            other_iter = other
        else:
            return NotImplemented
        mask = [_compare_for_eq(a, b) for a, b in zip(self._data, other_iter)]
        return pd.array(mask, dtype="boolean")

    def equals(self, other):
        return (
            isinstance(other, ArtifactStringArray)
            and len(self) == len(other)
            and all(
                (_safe_isna(a) and _safe_isna(b)) or (a == b)
                for a, b in zip(self._data, other._data)
            )
        )

    # ---- concat & factorize helpers ----
    @classmethod
    def _concat_same_type(cls, to_concat):
        return cls(np.concatenate([x._data for x in to_concat]))

    def _values_for_factorize(self):
        sentinel = object()
        filled = [sentinel if _safe_isna(x) else x for x in self._data]
        return np.asarray(filled, dtype=object), sentinel

    def interpolate(self, *_, **__):
        raise NotImplementedError(
            "Interpolation not supported for ArtifactStringArray."
        )

    def __repr__(self):
        return f"ArtifactStringArray({self._data.tolist()})"
