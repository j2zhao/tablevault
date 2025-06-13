# Default Code Functions API

There are several simple code functions included with the `tablevault` library. You can execute them by specifying the right parameters in a YAML `Builder` file.

---

## Dataframe Creation

These functions are loaded with `module_name: table_generation` and `is_custom: false`. They are meant to be used with an `IndexBuilder` YAML file.

The **YAML Builder** tab has the **specified** arguments of the builder file for the specific functions. You may have to fill out additional arguments not shown.

---

### `create_paper_table_from_folder`

=== "Python Code"

    ```python
    create_paper_table_from_folder(
        folder_dir: str,
        copies: int,
        artifact_folder: str,
        extension: str ='.pdf'
    ) -> pandas.DataFrame
    ```

=== "YAML Builder"

    ```yaml
    builder_type: IndexBuilder
    changed_columns: ['file_name', 'artifact_name', 'original_path']
    primary_key: ['file_name']
    python_function: create_paper_table_from_folder
    code_module: table_generation
    arguments:
        folder_dir: str
        copies: int
        artifact_folder: ~ARTIFACT_FOLDER~
        extension: str
    is_custom: false

    dtypes:
        artifact_name: artifact_string
    ```

Scan a directory for **`extension` files**, copy each into an artifact directory, and return a table describing every copy.

| Parameter         | Type  | Description                          |
| ----------------- | ----- | ------------------------------------ |
| `folder_dir`      | `str` | Folder containing the source PDFs    |
| `copies`          | `int` | How many copies per file (≥1)        |
| `artifact_folder` | `str` | Destination directory for the copies |
| `extension`       | `str` | File extension to be filtered        |

The resulting `DataFrame` has three columns:

1. **`file_name`** – base filename (without extension)
2. **`artifact_name`** – copied file’s name (includes suffixes when `copies > 1`)
3. **`original_path`** – path to the original PDF

---

### `create_data_table_from_table`

=== "Python Code"

    ```python
    create_data_table_from_table(
        df: pandas.DataFrame,
        nrows: int | None = None,
        random_sample: bool = False
    ) -> pandas.DataFrame
    ```

=== "YAML Builder"

    ```yaml
    builder_type: IndexBuilder
    changed_columns: list
    python_function: create_data_table_from_table
    code_module: table_generation
    arguments:
        df: pandas.DataFrame
        nrows: int # Optional
        random_sample: bool # Optional (need nrows)
    is_custom: false
    ```

Return a **copy** of `df`, with optional truncation or random sampling.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `df` | `pandas.DataFrame` | | Source data |
| `nrows` | `int` | `None` | Row limit (leave `None` for all rows) |
| `random_sample` | `bool` | `False` | If `True`, randomly sample `nrows` from `df` |

---

### `create_data_table_from_csv`

=== "Python Code"

    ```python
    create_data_table_from_csv(csv_file_path: str) -> pandas.DataFrame
    ```

=== "YAML Builder"

    ```yaml
    builder_type: IndexBuilder
    changed_columns: list
    python_function: create_data_table_from_csv
    code_module: table_generation
    arguments:    
        csv_file_path: str
    is_custom: false
    ```

Load a CSV file into a new `DataFrame` and return a copy.

| Parameter       | Type  | Description             |
| --------------- | ----- | ----------------------- |
| `csv_file_path` | `str` | Path to the CSV on disk |

---

### `create_data_table_from_list`

=== "Python Code"

    ```python
    create_data_table_from_list(vals: list) -> pandas.DataFrame
    ```

=== "YAML Builder"

    ```yaml
    builder_type: IndexBuilder
    changed_columns: [str] # only single column
    python_function: create_data_table_from_list
    code_module: table_generation
    arguments:    
        vals: list
    is_custom: false
    ```

Turn an in-memory Python list into a single-column table.

| Parameter | Type   | Description                   |
| --------- | ------ | ----------------------------- |
| `vals`    | `list` | Values to place in the column |

---

## Random String Module

---

### `random_row_string`

=== "Python Code"

    ```python
    random_row_string(column_names: list[str], **kwargs) -> tuple[str, ...] | str
    ```

=== "YAML Builder"

    ```yaml
    builder_type: ColumnBuilder
    changed_columns: [str]
    python_function: random_row_string
    code_module: random_string
    arguments:    
        column_names: list[str] # same length as changed_columns
    is_custom: false
    return_type: row-wise
    ```

Produce a single tuple of random strings—one per name in `column_names`.

| Parameter      | Type        | Description                               |
| -------------- | ----------- | ----------------------------------------- |
| `column_names` | `list[str]` | Column labels that determine tuple length |
| `**kwargs`     | *unused*    | Reserved for future options               |

**Returns:** a length-`len(column_names)` tuple of 20-character strings or a singular string (if only one column).

---