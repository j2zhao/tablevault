"""
artifact_extension.py
~~~~~~~~~~~~~~~~~~~~~

Custom nullable extension dtypes for pandas:

* ArtifactStringDtype / ArtifactStringArray  stores `str` or <NA>
* ArtifactListDtype   / ArtifactListArray    stores `list[str]` or <NA>
* ArtifactDictDtype   / ArtifactDictArray    stores `dict[str,str]` or <NA>

All arrays fully implement the required pandas ExtensionArray API and are
registered via ``@register_extension_dtype`` so they can be referenced by the
string aliases "artifact_string", "artifact_list", and "artifact_dict".
"""

import numpy as np
import pandas as pd
from typing import Any, List, Dict
from pandas.api.extensions import (
    ExtensionArray,
    ExtensionDtype,
    take,
    register_extension_dtype,
)


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
# Shared mix-in for list & dict variants
# ──────────────────────────────────────────────────────────────────────────
class _ArtifactBase(ExtensionArray):
    """Mixin implementing boilerplate common to list & dict arrays."""

    # subclasses set: _data, _dtype
    # ---- core API ----
    @property
    def dtype(self):
        return self._dtype

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._data[item]
        return type(self)(self._data[item])

    def isna(self):
        return np.fromiter((_safe_isna(x) for x in self._data), dtype=bool)

    def take(self, indices, allow_fill=False, fill_value=None):
        if allow_fill and fill_value is None:
            fill_value = self.dtype.na_value
        out = take(self._data, indices, allow_fill=allow_fill, fill_value=fill_value)
        return type(self)(out)

    def copy(self):
        return type(self)(self._data.copy())

    def __array__(self, dtype=None):
        return np.asarray(self._data, dtype=dtype)

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        return cls(scalars, copy=copy)

    @classmethod
    def _from_factorized(cls, values, original):
        return cls(values)

    @property
    def nbytes(self):
        return self._data.nbytes

    @classmethod
    def _concat_same_type(cls, to_concat):
        return cls(np.concatenate([a._data for a in to_concat]))

    def _formatting_values(self):
        return np.asarray(
            ["<NA>" if _safe_isna(x) else str(x) for x in self._data], dtype=object
        )

    def interpolate(self, *_, **__):
        raise NotImplementedError

    # ---- equality helpers
    def equals(self, other):
        return (
            isinstance(other, type(self))
            and len(self) == len(other)
            and all(
                (_safe_isna(a) and _safe_isna(b)) or (a == b)
                for a, b in zip(self._data, other._data)
            )
        )

    def __repr__(self):
        return f"{type(self).__name__}({self._data.tolist()})"


# ──────────────────────────────────────────────────────────────────────────
# ArtifactList
# ──────────────────────────────────────────────────────────────────────────
@register_extension_dtype
class ArtifactListDtype(ExtensionDtype):
    name = "artifact_list"
    type = list
    kind = "O"

    @classmethod
    def construct_array_type(cls):
        return ArtifactListArray

    @property
    def na_value(self):
        return pd.NA


class ArtifactListArray(_ArtifactBase):
    """Stores list[str] or <NA>."""

    def __init__(self, values: List[Any], copy: bool = False):
        cleaned: list[Any] = []
        for v in values:
            if isinstance(v, list):
                if not all(isinstance(x, str) for x in v):
                    raise TypeError("All items in each list must be str.")
                cleaned.append(v)
            elif _safe_isna(v):
                cleaned.append(pd.NA)
            else:
                raise ValueError("ArtifactListArray elements must be list[str] or NA.")
        self._data = (
            np.asarray(cleaned, dtype=object).copy()
            if copy
            else np.asarray(cleaned, dtype=object)
        )
        self._dtype = ArtifactListDtype()

    # equality treating list as scalar
    def __eq__(self, other):
        from pandas.api.types import is_list_like

        if isinstance(other, ArtifactListArray):
            other_iter = other._data
        elif isinstance(other, list):
            other_iter = [other] * len(self)
        elif is_list_like(other) and not isinstance(other, (str, bytes)):
            if len(other) != len(self):
                return pd.array([pd.NA] * len(self), dtype="boolean")
            other_iter = other
        else:
            return NotImplemented

        mask = [_compare_for_eq(a, b) for a, b in zip(self._data, other_iter)]
        return pd.array(mask, dtype="boolean")

    # make factorize hashable (tuple of list)
    def _values_for_factorize(self):
        sentinel = object()
        hashed = [sentinel if _safe_isna(x) else tuple(x) for x in self._data]
        return np.asarray(hashed, dtype=object), sentinel


# ──────────────────────────────────────────────────────────────────────────
# ArtifactDict
# ──────────────────────────────────────────────────────────────────────────
@register_extension_dtype
class ArtifactDictDtype(ExtensionDtype):
    name = "artifact_dict"
    type = dict
    kind = "O"

    @classmethod
    def construct_array_type(cls):
        return ArtifactDictArray

    @property
    def na_value(self):
        return pd.NA


class ArtifactDictArray(_ArtifactBase):
    """Stores dict[str, str] or <NA>."""

    def __init__(self, values: List[Any], copy: bool = False):
        cleaned: list[Any] = []
        for v in values:
            if isinstance(v, dict):
                if not all(
                    isinstance(k, str) and isinstance(val, str) for k, val in v.items()
                ):
                    raise TypeError("Keys and values must all be str.")
                cleaned.append(v)
            elif _safe_isna(v):
                cleaned.append(pd.NA)
            else:
                raise ValueError(
                    "ArtifactDictArray elements must be dict[str,str] or NA."
                )
        self._data = (
            np.asarray(cleaned, dtype=object).copy()
            if copy
            else np.asarray(cleaned, dtype=object)
        )
        self._dtype = ArtifactDictDtype()

    def __eq__(self, other):
        from pandas.api.types import is_list_like

        if isinstance(other, ArtifactDictArray):
            other_iter = other._data
        elif isinstance(other, dict):
            other_iter = [other] * len(self)
        elif is_list_like(other) and not isinstance(other, (str, bytes)):
            if len(other) != len(self):
                return pd.array([pd.NA] * len(self), dtype="boolean")
            other_iter = other
        else:
            return NotImplemented
        mask = [_compare_for_eq(a, b) for a, b in zip(self._data, other_iter)]
        return pd.array(mask, dtype="boolean")

    # hash dicts as sorted tuple of items
    def _values_for_factorize(self):
        sentinel = object()
        hashed = [
            sentinel if _safe_isna(x) else tuple(sorted(x.items())) for x in self._data
        ]
        return np.asarray(hashed, dtype=object), sentinel
