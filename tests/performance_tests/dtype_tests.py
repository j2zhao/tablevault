# import pandas as pd
# import numpy as np

# def convert_to_dtype(value, dtype):
#     if pd.isna(value):
#         return value

#     if  isinstance(dtype, pd.CategoricalDtype):
#         return pd.Series([value]).astype(dtype)[0]

#     # For non-categorical types, try using the type constructor
#     try:
#         # If dtype has a 'type' attribute (as with NumPy dtypes), use it:
#         if hasattr(dtype, 'type'):
#             return dtype.type(value)
#         else:
#             # Otherwise, assume dtype is a string and convert it using np.dtype
#             return np.dtype(dtype).type(value)
#     except Exception as e:
#         raise ValueError(f"Could not convert value {value!r} to dtype {dtype!r}: {e}")

# # Create a categorical dtype with specific allowed values.
# cat_dtype = pd.CategoricalDtype(categories=["apple", "banana", "cherry"], ordered=False)

# # Convert a valid value
# print(convert_to_dtype("apple", cat_dtype))  # Should output: apple

# # Convert an invalid value (not in the categories)
# print(convert_to_dtype("orange", cat_dtype))  # Likely outputs: NaN

# # Convert a numeric value
# print(convert_to_dtype("123", "int64"))  # Outputs: 123 (as an integer)

# # Convert a NaN value
# print(convert_to_dtype(np.nan, "float64"))  # Outputs: nan

import pandas as pd
import numpy as np


def convert_list_to_dtype(values, dtype):
    """
    Convert a list of Python values to a Pandas Series with a specified dtype.

    Parameters:
        values (list): A list of Python values.
        dtype: The target data type. This can be a string (e.g., "int64", "float64", "category"),
               a NumPy dtype (e.g., np.int64), or a pandas dtype (e.g., a categorical dtype).

    Returns:
        pd.Series: A Pandas Series with the values converted to the target dtype.

    Note:
        - For categorical dtypes, values not in the defined categories will be converted to NaN.
        - If a value is NaN, it is returned unchanged.
    """
    # Convert the list into a Pandas Series.
    s = pd.Series(values)

    # Handle categorical dtype: use astype to ensure that categories are respected.
    if pd.api.types.is_categorical_dtype(dtype):
        return s.astype(dtype)

    # For non-categorical dtypes, attempt to convert the entire Series using astype.
    try:
        return s.astype(dtype)
    except Exception:
        # If astype fails, fall back to element-wise conversion.
        def convert_element(x):
            if pd.isna(x):
                return x
            try:
                # If dtype has a 'type' attribute (common for NumPy dtypes), use it.
                if hasattr(dtype, "type"):
                    return dtype.type(x)
                # Otherwise, convert using np.dtype.
                return np.dtype(dtype).type(x)
            except Exception as inner_e:
                raise ValueError(
                    f"Could not convert {x!r} to dtype {dtype!r}: {inner_e}"
                )

        return s.apply(convert_element)


# Example usage:

# 1. Using a categorical dtype:
cat_dtype = pd.CategoricalDtype(categories=["apple", "banana", "cherry"], ordered=False)
fruits = ["apple", "orange", "banana", np.nan, "cherry"]
converted_fruits = convert_list_to_dtype(fruits, cat_dtype)
print(converted_fruits)
# Output: a Series of dtype 'category' where "orange" becomes NaN since it's not in the categories.

# 2. Using a numeric dtype:
nums = ["1", "2", "3", np.nan, "4"]
converted_nums = convert_list_to_dtype(nums, "int64")
print(converted_nums)
# Output: a Series of integers (NaNs will be preserved as np.nan)
