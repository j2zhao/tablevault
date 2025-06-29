# Builder and Custom Function Guide

Builders inform TableVault how to build a DataFrame upon instance execution. Builder files are YAML specifications that define how a table instance is constructed or modified.

---

## Builder File Configuration


TableVault supports two built-in builder types:

1.  **`IndexBuilder`**: Produces a new DataFrame, often by defining its primary key and core rows.
2.  **`ColumnBuilder`**: Generates or mutates individual columns within the DataFrame established by the `IndexBuilder`.

Each table instance that is executed must contain exactly one `IndexBuilder`. Additional `ColumnBuilder` files can be included to perform subsequent modifications.


===  "IndexBuilder"

    An `IndexBuilder` is used to produce the initial DataFrame for a table instance. It typically defines the primary structure, index, and the total set of rows, often involving operations like joins or initial data loading.

    **Important:** Each executed table instance must include exactly one `IndexBuilder` file, and this file **must be named** according to the pattern: `{table_name}_index.yaml`.

    ```yaml
    builder_type: IndexBuilder

    changed_columns: ['COLUMN_NAMES']        # Output columns
    python_function: FUNCTION_NAME           # Function to execute
    code_module: MODULE_NAME                 # Module containing the function

    arguments:                               # Arguments passed to the function
        ARGUMENT_NAME_1: ARG_VALUE
        ARGUMENT_NAME_2: <<TABLE.COLUMN>>    # Reference to another table's column
        ARGUMENT_NAME_3: ~ARTIFACT_FOLDER~   # Special keyword for artifact folder path
    n_threads: 1                             # Number of parallel workers (default 1)

    # Optional flags
    is_custom: false                         # True if using a user-supplied function in code_module
    return_type: dataframe                   # return type as one of [row-wise, dataframe, generator]
    primary_key: ['COLUMN_NAMES']            # DataFrame primary key
    keep_old: false                          # Keep original rows

    # Optional column dtypes
    dtypes:
        COLUMN_NAME_1: Int64
        COLUMN_NAME_2: Float64
        COLUMN_NAME_3: artifact_string     # Special type for artifact paths

    # Optional explicit table dependencies
    dependencies: ['TABLE_NAME.COLUMNS']     # e.g., ['Orders.customer_id']
    ```

=== "ColumnBuilder"

    A `ColumnBuilder` is used to add new columns or modify existing ones based on calculations or transformations. It operates on the DataFrame produced by the `IndexBuilder` (or a preceding `ColumnBuilder`).

    **Important:** Every `ColumnBuilder` must output a DataFrame that has the exact same number of rows as the DataFrame generated by the `IndexBuilder`. The columns it creates must correspond to those listed in its `changed_columns` field.

    ```yaml
    builder_type: ColumnBuilder

    changed_columns: ['COLUMN_NAMES']        # Output columns created / overwritten

    python_function: FUNCTION_NAME         # e.g., build_features
    code_module: MODULE_NAME               # e.g., my_feature_lib

    arguments:
        ARGUMENT_NAME_1: ARG_VALUE           # Scalars, lists, environment variables, etc.
        ARGUMENT_NAME_2: <<TABLE.COLUMN>>    # Table reference syntax
        ARGUMENT_NAME_3: ~ARTIFACT_FOLDER~   # Special keyword for artifact folder path
    n_threads: 1                           # Parallel workers (default 1)

    # Optional flags
    is_custom: false                       # Mark as user-supplied (searches in code_functions)
    return_type: dataframe                 # return type as one of [row-wise, dataframe, generator]

    # Optional column dtypes
    dtypes:
        COLUMN_NAME_1: Int64
        COLUMN_NAME_2: Float64
        COLUMN_NAME_3: artifact_string     # Special type for artifact paths

    # Optional explicit table dependencies
    dependencies: ['TABLE_NAME.COLUMNS']     # e.g., ['Products.price']
    ```

### Field Reference

| Field             | Type           | Applicability       | Description                                                                                                                                                                                |
| ----------------- | -------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `builder_type`    | `string`       | All                 | Specifies the type of builder. Must be exactly `"IndexBuilder"` or `"ColumnBuilder"`.                                                                                                      |
| `changed_columns` | `list[string]` | All                 | A list of column names that are introduced or modified by this builder.                                                                                                                    |
| `python_function` | `string`       | All                 | The fully qualified name of the Python function to execute (e.g., `my_module.my_function`). The function must be able to accept the declared `arguments`.                                  |
| `code_module`     | `string`       | All                 | The name of the Python module file (e.g., `my_processing_lib.py`) containing the function. Must have been added to the TableVault via the API.                                             |
| `arguments`       | `dict`         | All                 | A key-value mapping passed to the `python_function`. Values can be scalars, lists, table references (`<<TABLE.COLUMN>>`), or the special keyword `~ARTIFACT_FOLDER~`.                      |
| `n_threads`       | `int`          | All                 | The number of parallel worker threads to use for processing. **Default:** `1`.                                                                                                             |
| `is_custom`       | `bool`         | All                 | Indicates the function's source. If `true`, the function is loaded from a user-supplied `code_module`. If `false`, it's loaded from TableVault’s built-in functions. **Default:** `false`. |
| `return_type`     | `string`       | All                 | Expected return format from `python_function`. Must be one of **`row-wise`**, **`dataframe`**, or **`generator`**. **Default:** `dataframe`.                                               |
| `keep_old`        | `bool`         | `IndexBuilder` only | Determines how rows from a previous instance are handled. If `true`, existing rows not generated by the current run are kept. **Default:** `false`.                                        |
| `primary_key`     | `list[string]` | `IndexBuilder` only | **(Optional)** A list of column names that form a unique row identifier. If omitted, the row position is used.                                                                             |
| `dtypes`          | `dict`         | All                 | **(Optional)** A mapping from column names to pandas data types (e.g., `{"col1": "Int64"}`). Used for type coercion and for declaring `artifact_string` columns.                           |
| `dependencies`    | `list[string]` | All                 | **(Optional)** Overrides automatic dependency detection. A list of explicit table dependencies (e.g., `['Orders.*']`). Required if dependencies are not clear from `arguments`.            |

-----


## Special Builder Keywords

TableVault builders support special keywords that are dynamically replaced with context-aware values at runtime. These keywords enable access to file paths and data from other tables, making configurations more powerful and flexible.

| Keyword | Syntax | Scope | Description |
| :--- | :--- | :--- | :--- |
| **Artifact Folder** | `~ARTIFACT_FOLDER~` | `arguments` | A placeholder that resolves to the absolute path of the artifact folder for the current instance run. It is essential for any function that needs to save artifact files. |
| **Process ID** | `~PROCESS_ID~` | `arguments` | A placeholder that resolves to the string identifier of the current instance run. It is strongly recommended for any function interacts with the TableVault API internally. Best practice is to set `author` to the value of `~PROCESS_ID~` when creating a `TableVault()` object within an executing instance. |
| **Table Reference** | `<<...>>` | Most string fields | A dynamic reference used to fetch data from other tables or the current table instance (`self`). The expression within the `<<...>>` is resolved and its value is substituted into the field. |

**Example Usage in `arguments`:**

```yaml
arguments:
  # The ~ARTIFACT_FOLDER~ keyword provides the path for saving a generated file
  artifact_output_path: ~ARTIFACT_FOLDER~

  # The <<...>> syntax fetches a specific configuration value from another table
  source_data_id: "<<config_table.source_id[region::'US']>>"
```

---

## Creating Custom Builder Functions


Every builder YAML tells TableVault *how* to construct or modify the instance DataFrame. 
The `return_type` field aligns your Python function’s output with the framework’s expectations:

| `return_type`               | Type Use Pattern                                                 | Expected Return Output                                                                                                                       | What TableVault does                                                                                                 |
| --------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **`dataframe`** *(default)* | Create **whole table columns** in one function.                  | Function is called once and returns **one `pd.DataFrame`** that includes **every column in `changed_columns`**.                              | Writes output columns (`ColumnBuilder`) directly or joins with origin table on primary key (`IndexBuilder`).         |
| **`row‑wise`**              | Compute **each row independently**.”                             | Function is called **once per input row** and returns a scalar or tuple representing that row’s new values in `changed_columns`.             | Writes each returned row immediately; can run with multiple threads.                                                 |
| **`generator`**             | Generate **rows incrementally** in one function                  | Function is called once abd yields a scalar or tuple, representing the next row’s new values in `changed_columns` in physical index order.   | Writes each yielded row immediately.                                                                                 |

---

**Important Notes**

1. **Column order & count** must match `changed_columns`. One column → you may return/yield a scalar.
2. All values must coerce cleanly to the dtypes you declare (e.g. `artifact_string`).
3. If you write artefact files, return **relative paths** (`my_plot.png`), not absolute ones.
4. For resuming interrupted runs, rely on the previously saved-instance (you can access it by assigning `<<self>>` as an argument). **Do not** look at artifact files, since they might not be saved to your dataframe.
5. For `row-wise` functions that correspond to an IndexBuilder, the function is called for each row in the original copied table.
6. Arguments to `row-wise` will automatically resolve the `index` key-word as the current row's index.

---

## Example Builders and Functions 

### Custom `dataframe` Function

**An `IndexBuilder` that converts a `list` to a `DataFrame`**

=== "Python Code"

    ```python
    create_data_table_from_list(vals: list) -> pandas.DataFrame:
        return pd.DataFrame({"row_index": vals})
    ```

=== "Example YAML Builder"

    ```yaml
    builder_type: IndexBuilder
    changed_columns: [row_index] # only single column
    primary_key: [row_index]
    python_function: create_data_table_from_list
    code_module: table_generation
    arguments:    
        vals: [2, 4, 6, 8]
    is_custom: false

    ```

---

### Custom `row-wise` Function

**A `ColumnBuilder` that saves an `fruit_image` file for each `fruit` key**


=== "Python Code"

    ```python
    import shutil

    def fetch_image_from_string(fruit: str, artifact_dir:str ):
        
        file_path = f'./all_images/{fruit}.png' # pre-existing file
        new_file_path = f'{artifact_dir}/{fruit}.png'
        
        shutil.copy(file_path, new_file_path)
        
        return f'{fruit}.png' # return relative path

    ```

=== "Example YAML Builder"

    ```yaml
    builder_type: ColumnBuilder

    changed_columns: ['fruit_image']                        # Output columns

    python_function: create_data_table_from_table           # Function to execute
    code_module: table_generation                           # Module containing the function
    
    is_custom: true                                         # Mark as user-supplied (searches in code_functions)
    return_type: row-wise                                       # Specifies if the function processes row by row
    
    arguments:                                              # Arguments passed to the function
        fruit: <<self.fruits[index]>> 
        artifact_dir: ~ARTIFACT_FOLDER~ 

    dtypes:                                                 # Column Data Types 
        fruit_image: artifact_string            
    ```

---

### Custom `generator` Function

**An `IndexBuilder` that yields a batch of `GritLM` embedding to a new row**

**Why this custom code pattern matters:** each yielded row is immediately committed. If your process dies, rerunning the code simply skips what’s already in `self_df`.

=== "Python Code"

    ```python
    from typing import Iterator
        from tqdm import tqdm
        from gritlm import GritLM
        import pandas as pd
        import numpy as np
        import time
        import os
        import torch
        import gc

        def get_batch_embeddings(df:pd.DataFrame,
                                self_df:pd.DataFrame,
                                artifact_column: str,
                                raw_instruction:str,
                                batch_size:int,
                                artifact_name:str,
                                artifact_folder:str)-> Iterator[tuple[int, tuple[int, str, float]]]:
            gc.collect()
            torch.cuda.empty_cache()
            model = GritLM("GritLM/GritLM-7B", torch_dtype="auto", device_map="auto", mode="embedding")

            for index, start_index in enumerate(tqdm(range(0, len(df), batch_size), desc="Batches")):
                start_time = time.time()
                end_index = start_index + batch_size
                artifact_name_ = artifact_name + f"_{start_index}_{end_index}.npy"
                artifact_dir = os.path.join(artifact_folder, artifact_name_)
                if start_index in self_df['start_index']:
                    continue
                batch_df = df.iloc[start_index:end_index][artifact_column]
                batch_texts = []
                for file_path in batch_df:
                    with open(file_path, 'r') as f:
                        batch_texts.append(f.read())
                ndarr = model.encode(batch_texts, batch_size=batch_size,
                                        instruction=raw_instruction).astype(np.float16)
                end_time = time.time()
                np.save(artifact_dir, ndarr)
                yield start_index, artifact_name_, end_time - start_time
    ```

=== "Example YAML Builder"

    ```yaml
    builder_type: IndexBuilder

    changed_columns: ['start_index', 'artifact_name', 'elapsed_time']        # Output columns created / overwritten
    primary_key: ['start_index'] 
    python_function: get_batch_embeddings         # e.g., build_features
    code_module: batch_embedding                  # e.g., my_feature_lib

    arguments:
        df: <<paper_abstract_store.artifact_name>>
        self_df: <<self>>
        artifact_column: artifact_name
        raw_instruction: "<|embed|>\n"
        batch_size: 4
        artifact_name: embeddings
        artifact_folder: ~ARTIFACT_FOLDER~

    is_custom: true
    return_type: generator

    dtypes:
        artifact_name: artifact_string
        elapsed_time: float

    ```



---
