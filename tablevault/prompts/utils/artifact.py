import numpy as np
from pandas.api.extensions import ExtensionDtype, ExtensionArray, register_extension_dtype, take
import os
import pandas as pd
from tablevault.defintions import constants

def join_path(artifact:str, path_dir:str)->str:
    return os.path.join(path_dir, artifact)


def df_artifact_to_path(df:pd.DataFrame, path_dir:str):
    for col in df.columns:
        if df[col].dtype == 'artifact_string':
            df[col] = df[col].apply(lambda x : join_path(x, path_dir))
    return df

def get_artifact_folder(instance_id:str,
                        table_name:str,        
                        db_dir: str, 
                        allow_multiple: bool) -> str:
    if allow_multiple:
        return os.path.join(db_dir, table_name, instance_id, constants.ARTIFACT_FOLDER)
    else:
        return os.path.join(db_dir, table_name, constants.ARTIFACT_FOLDER)

 
@register_extension_dtype
class ArtifactStringDtype(ExtensionDtype):
    name = 'artifact_string'
    type = str
    kind = 'O'  # object

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
        # Optionally copy the sequence to avoid mutating the original data.
        if copy:
            strings = list(strings)
        # Optionally, validate that each element is a string.
        for s in strings:
            if not isinstance(s, str):
                raise ValueError("All elements must be strings")
        return cls(strings)
    
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