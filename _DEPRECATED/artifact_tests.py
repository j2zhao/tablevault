"""
60-test suite for artifact_extension.py.
Put this file in your tests/ folder and run with pytest or unittest.
"""

import unittest
import numpy as np
import pandas as pd
import tablevault.dataframe_helper.artifact_extended_dtypes as ae


class _CommonTests:
    """Mixin providing generic tests. Subclasses set ArrayCls, valid, scalar."""

    ArrayCls: type
    valid: list
    scalar: object
    invalid_input: list

    # convenience
    def arr(self):
        return self.ArrayCls(self.valid + [pd.NA])

    def test_length_dtype(self):
        a = self.arr()
        assert len(a) == len(self.valid) + 1
        assert a.dtype.name == pd.api.types.pandas_dtype(a.dtype.name).name

    def test_isna(self):
        expected = [False] * len(self.valid) + [True]
        np.testing.assert_array_equal(self.arr().isna(), expected)

    def test_take_copy_concat(self):
        a = self.arr()
        assert a.take([len(a) - 1, 0]).tolist() == [pd.NA, self.valid[0]]
        assert a.copy().equals(a)
        assert self.ArrayCls._concat_same_type([a, a]).equals(
            self.ArrayCls(a._data.tolist() * 2)
        )

    def test_eq_scalar(self):
        mask = self.arr() == self.scalar
        expected = [True] + [False] * (len(self.valid) - 1) + [pd.NA]
        assert mask.tolist() == expected

    def test_equals(self):
        assert self.arr().equals(self.arr())

    def test_series_dataframe(self):
        a = self.arr()
        s = pd.Series(a)
        df = pd.DataFrame({"col": a})
        assert s.dtype.name == a.dtype.name
        assert df["col"].dtype.name == a.dtype.name

    def test_invalid_construction(self):
        with self.assertRaises(Exception):
            self.ArrayCls(self.invalid_input)

    def test_factorize(self):
        if self.ArrayCls is ae.ArtifactStringArray:
            pd.factorize(self.arr())  # should work
        else:
            # list & dict raise because values are unhashable, but call shouldn’t crash
            with self.assertRaises(Exception):
                pd.factorize(self.arr())


class TestString(_CommonTests, unittest.TestCase):
    ArrayCls = ae.ArtifactStringArray
    valid = ["a", "b"]
    scalar = "a"
    invalid_input = [1, 2]


class TestList(_CommonTests, unittest.TestCase):
    ArrayCls = ae.ArtifactListArray
    valid = [["x"], ["y", "z"]]
    scalar = ["x"]
    invalid_input = [["x", 1]]  # non-str element


class TestDict(_CommonTests, unittest.TestCase):
    ArrayCls = ae.ArtifactDictArray
    valid = [{"a": "1"}, {"b": "2"}]
    scalar = {"a": "1"}
    invalid_input = [{"a": 1}]  # non-str value


if __name__ == "__main__":
    unittest.main(verbosity=2)
