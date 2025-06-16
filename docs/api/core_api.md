# TableVault Python Interface API

The core way to interact with the TableVault repository is through `TableVault` instance. Here, you can find the full API for the Python interface.

Read [Basic Workflow](../workflows/workflow.md) for an example of how to use this API.

---

## Class `TableVault`

```python
class TableVault()

```

Interface with a TableVault repository. Initialisation can create a new vault repository and optionally restart any active processes. Subsequent methods allow interaction with tables instances, code modules, and builder files within that vault.

| Parameter     | Type   | Description                                                               | Default |
| ------------- | ------ | ------------------------------------------------------------------------- | ------- |
| `db_dir`      | `str`  | Directory path where the TableVault is stored (or should be created).     | –       |
| `author`      | `str`  | Name or identifier of the user/system performing the operations.          | –       |
| `description` | `str`  | Description for the vault creation (used only when *create* is **True**). | `""`    |
| `create`      | `bool` | If **True**, initialise a new vault at *db\_dir*.                         | `False` |
| `restart`     | `bool` | If **True**, restart any processes previously active in this vault.       | `False` |
| `verbose`     | `bool` | If **True**, prints detailed logs of every operation.                     | `False` |

---

## `TableVault` Data Creation Methods

### `create_table()`

Create a new table definition in the vault.

```python
def create_table(
    self,
    table_name: str,
    allow_multiple_artifacts: bool = False,
    has_side_effects: bool = False,
    description: str = "",
    process_id: str = "",
    
) -> str:
```

| Parameter                  | Type   | Description                                                                      | Default |
| -------------------------- | ------ | -------------------------------------------------------------------------------- | ------- |
| `table_name`               | `str`  | Name of the new table.                                                           | –       |
| `allow_multiple_artifacts` | `bool` | **True** ⇒ instance has own artifact folder; **False** ⇒ one folder, one active. | `False` |
| `has_side_effects`         | `bool` | **True** ⇒ builders have side effects (e.g. API calls).                          | `False` |
| `description`              | `str`  | Description for the table.                                                       | `""`    |
| `process_id`               | `str`  | Generated process identifier.                                                    | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `create_instance()`

Create a new temporary instance of a table.

```python
def create_instance(
    self,
    table_name: str,
    version: str = "base",
    origin_id: str = "",
    origin_table: str = "",
    external_edit: bool = False,
    copy: bool = False,
    builders: Optional[dict[str, str] | list[str]] = None,
    process_id: str = "",
    description: str = "",
) -> str:
```

| Parameter       | Type                                      | Description                                                                              | Default                        |
| --------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------ |
| `table_name`    | `str`                                     | Name of the table.                                                                       | –                              |
| `version`       | `str`                                     | Version of the table.                                                                    | `"base"`                       |
| `origin_id`     | `str`                                     | If supplied, copy state from this existing instance ID.                                  | `""`                           |
| `origin_table`  | `str`                                     | Table for `origin_id`; empty ⇒ `table_name`.                                             | `""`                           |
| `external_edit` | `bool`                                    | **True** ⇒ instance edited externally, no builders constructed.                          | `False`                        |
| `copy`          | `bool`                                    | **False** (no `origin_id`) ⇒ use latest materialised instance as origin if it exists.    | `True`                         |
| `builders`      | `Optional[dict[str, str] \| list[str]]`   | List of new builder names to generate.                                                   | `None`                         |
| `description`   | `str`                                     | Description for this instance.                                                           | `""`                           |
| `process_id`    | `str`                                     | Process identifier.                                                                      | `""`                           |

**Returns** → `str` – process ID of this operation.

---

### `create_code_module()`

```python
def create_code_module(
    self,
    module_name: str = "",
    copy_dir: str = "",
    process_id: str = ""
) -> str:
```

Create or copy a Python module file into TableVault.

| Parameter     | Type  | Description                       | Default |
| ------------- | ----- | --------------------------------- | ------- |
| `module_name` | `str` | Name for the new module.          | `""`    |
| `copy_dir`    | `str` | Directory or Python file to copy. | `""`    |
| `process_id`  | `str` | Generated process identifier.     | `""`    |

**Returns** → `str` – process ID of this operation.

---


### `create_builder_file()`

Create or update a builder (YAML) file for a temporary table instance.

If the builder content is not specified, a template file will be created.

```python
def create_builder_file(
    self,
    table_name: str,
    builder_name: str = "",
    version: str = "base",
    copy_dir: str = "",
    process_id: str = "",
) -> str:
```

| Parameter      | Type  | Description                           | Default                        |
| -------------- | ----- | ------------------------------------- | ------------------------------ |
| `table_name`   | `str` | Name of the table.                    | –                              |
| `builder_name` | `str` | Builder file name; empty ⇒ inferred.  | `{table_name}_index`           |
| `version`      | `str` | Version of the table.                 | `"base"`                         |
| `copy_dir`     | `str` | Directory containing builder file(s). | `""`                           |
| `process_id`   | `str` | Generated process identifier.         | `""`                           |

**Returns** → `str` – process ID of this operation.

---

## `TableVault` Instance Materialization Methods

### `write_instance()`

Write `table_df` as a **materialized instance** of `table_name` and `version`.

The table must already have a **temporary instance** of the same version that is open for external edits (generated by `create_instance()`).

```python
def write_instance(
    self,
    table_df: pd.DataFrame,
    table_name: str,
    version: str = '"base"',
    dependencies: Optional[list[tuple[str, str]]] = None,
    dtypes: Optional[dict[str, str]] = None,
    process_id: str = "",
) -> str:
```

| Parameter      | Type                                  | Description                                                           | Default                        |
| -------------- | ------------------------------------- | --------------------------------------------------------------------- | ------------------------------ |
| `table_df`     | `pd.DataFrame`                        | Data to write.                                                        | –                              |
| `table_name`   | `str`                                 | Target table.                                                         | –                              |
| `version`      | `str`                                 | Target version.                                                       | `"base"`                         |
| `dependencies` | `Optional[list[tuple[str, str]]]`     | List of `(table_name, instance_id)` dependencies. None for no deps.   | `None`                         |
| `dtypes`       | `Optional[dict[str, str]]`            | `{column: pandas-dtype}`. None for nullable defaults.                 | `None`                         |
| `process_id`   | `str`                                 | Generated process identifier.                                         | `""`                           |

**Returns** → `str` – The process ID of the executed write operation.

---

### `execute_instance()`

Executes and materialise an existing temporary table instance from builder files.

```python
def execute_instance(
    self,
    table_name: str,
    version: str = "base",
    force_execute: bool = False,
    process_id: str = "",
    background: bool = False,
) -> str:
```

| Parameter       | Type   | Description                                                                 | Default                        |
| --------------- | ------ | --------------------------------------------------------------------------- | ------------------------------ |
| `table_name`    | `str`  | Name of the table to materialise.                                           | –                              |
| `version`       | `str`  | Version of the table.                                                       | `"base"`                         |
| `force_execute` | `bool` | **True** ⇒ force full rebuild; **False** ⇒ reuse origin if possible.        | `False`                        |
| `process_id`    | `str`  | Generated process identifier.                                               | `""`                           |
| `background`    | `bool` | **True** ⇒ run materialisation in background.                               | `False`                        |

**Returns** → `str` – process ID of this operation.

---



## `TableVault` Data Deletion/Modification Methods

### `rename_table()`

Rename an existing table within the TableVault repository.

```python
def rename_table(
    self, new_table_name: str, table_name: str, process_id: str = ""
) -> str:
```

| Parameter        | Type  | Description                   | Default |
| ---------------- | ----- | ----------------------------- | ------- |
| `new_table_name` | `str` | New table name.               | –       |
| `table_name`     | `str` | Current table name.           | –       |
| `process_id`     | `str` | Generated process identifier. | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_table()`

Permanently delete a table and all its instances from the repository. Only the dataframes and artifacts are removed; table metadata is retained.

```python
def delete_table(self, table_name: str, process_id: str = "") -> str:
```

| Parameter    | Type  | Description                    | Default |
| ------------ | ----- | ------------------------------ | ------- |
| `table_name` | `str` | Name of the table to delete.   | –       |
| `process_id` | `str` | Generated process identifier.  | `""`    |

**Returns** → `str` – process ID of this operation.

---


### `delete_instance()`

```python
def delete_instance(
    self, instance_id: str, table_name: str, process_id: str = ""
) -> str:
```
Delete a materialised table instance from the vault. Only the dataframe is removed and artifacts; instance metadata is retained.

| Parameter     | Type  | Description                        | Default |
| ------------- | ----- | ---------------------------------- | ------- |
| `instance_id` | `str` | ID of the instance to delete.      | –       |
| `table_name`  | `str` | Name of the table owns instance.   | –       |
| `process_id`  | `str` | Generated process identifier.      | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_code_module()`

Delete a Python module file from the repository.

```python
def delete_code_module(self, module_name: str, process_id: str = "") -> str:
```

| Parameter     | Type  | Description                     | Default |
| ------------- | ----- | ------------------------------- | ------- |
| `module_name` | `str` | Name of the module to delete.   | –       |
| `process_id`  | `str` | Generated process identifier.   | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_builder_file()`

```python
def delete_builder_file(
    self,
    builder_name: str,
    table_name: str,
    version: str = "base",
    process_id: str = "",
) -> str:
```

Delete a builder file from a temporary table instance.

| Parameter      | Type  | Description                          | Default                        |
| -------------- | ----- | ------------------------------------ | ------------------------------ |
| `builder_name` | `str` | Name of the builder file to delete.  | –                              |
| `table_name`   | `str` | Owning table name.                   | –                              |
| `version`      | `str` | Version of the table.                | `"base"`                         |
| `process_id`   | `str` | Generated process identifier.        | `""`                           |

**Returns** → `str` – process ID of this operation.

---

## `TableVault` Process Methods


### `generate_process_id()`

```python
def generate_process_id(self) -> str:
```

Generate and return a unique process ID. If a process ID is supplied to an operation, that operation persists on errors and can be restarted with the same ID

**Returns** → `str` – A unique process identifier.

---


### `stop_process()`

```python
def stop_process(
    self,
    to_stop_process_id: str,
    force: bool = False,
    materialize: bool = False,
    process_id: str = "",
) -> str:

```
Stop an active process and optionally terminate it forcefully.

| Parameter            | Type   | Description                                                   | Default |
| -------------------- | ------ | ------------------------------------------------------------- | ------- |
| `to_stop_process_id` | `str`  | ID of the process to stop.                                    | –       |
| `force`              | `bool` | **True** ⇒ forcibly stop; **False** ⇒ raise if still running. | `False` |
| `materialize`        | `bool` | **True** ⇒ materialise partial instances if relevant.         | `False` |
| `process_id`         | `str`  | Generated process identifier.                                 | `""`    |

**Returns** → `str` – process ID of this *stop\_process* call.

---

## `TableVault` Data Fetching Methods

---

### `get_dataframe()`

```python
def get_dataframe(
    self,
    table_name: str,
    instance_id: str = "",
    version: str = "base",
    active_only: bool = True,
    successful_only: bool = False,
    rows: Optional[int] = None,
    full_artifact_path: bool = True,
) -> tuple[pd.DataFrame, str]:
```

Retrieve a pandas `DataFrame` for a table instance.

| Parameter            | Type            | Description                                                            | Default                        |
| -------------------- | --------------- | ---------------------------------------------------------------------- | ------------------------------ |
| `table_name`         | `str`           | Name of the table.                                                     | –                              |
| `instance_id`        | `str`           | Specific instance ID; empty ⇒ latest of *version*.                     | `""`                           |
| `version`            | `str`           | Version when *instance\_id* omitted.                                   | `"base"`                         |
| `active_only`        | `bool`          | **True** ⇒ consider only active instances.                             | `True`                         |
| `successful_only`    | `bool`          | **True** ⇒ consider only *successful* runs.                            | `False`                        |
| `rows`               | `Optional[int]` | Row limit (`None` = no limit).                                         | `None`                         |
| `full_artifact_path` | `bool`          | **True** ⇒ prefix `"artifact_string"` columns with the repository path | `True`                         |

**Returns** → `tuple[pd.DataFrame, str]` – *(dataframe, resolved\_instance\_id)*.

---


### `get_file_tree()`


```python
def get_file_tree(
    self,
    instance_id: str = "",
    table_name: str = "",
    code_files: bool = True,
    builder_files: bool = True,
    metadata_files: bool = False,
    artifact_files: bool = False,
) -> rich.tree.Tree:
```
Retrieves a RichTree object representation of the repository.

| Parameter        | Type   | Description                                | Default |
| ---------------- | ------ | ------------------------------------------ | ------- |
| `instance_id`    | `str`  | Retrieve partial instance tree.            | `""`    |
| `table_name`     | `str`  | Retrieve partial table tree.               | `""`    |
| `code_files`     | `bool` | Include stored Python modules.             | `True`  |
| `builder_files`  | `bool` | Include builder files.                     | `True`  |
| `metadata_files` | `bool` | Include metadata files.                    | `False` |
| `artifact_files` | `bool` | Include artifact directory contents.       | `False` |

**Returns** → `rich.tree.Tree` – printable file-tree representation.

---

### `get_table_instances()`

```python
def get_table_instances(
    self,
    table_name: str,
    version: str = "base",
) -> list[str]:
```

Retrieves a list of table names, or instance IDs for a specific table and version.

| Parameter    | Type  | Description                         | Default                        |
| ------------ | ----- | ----------------------------------- | ------------------------------ |
| `table_name` | `str` | Name of the table.                  | –                              |
| `version`    | `str` | Version of the table.               | `"base"`                       |
| `version`    | `str` | Include non-materialized instances. | `False`                        |

**Returns** → `list[str]` – instance IDs or table names.

---

### `get_active_processes()`

```python
def get_active_processes(self) -> ActiveProcessDict:
```

Retrieves a dictionary of currently active processes in the vault.
Each key is a process ID and each value is metadata about that process.

**Returns** → `ActiveProcessDict` – alias `dict[str, Mapping[str, Any]]`.

---


### `get_process_completion()`

```python
def get_process_completion(self, process_id: str) -> bool:
```

Retrieves the completion status of a specific process.

| Parameter    | Type  | Description                |
| ------------ | ----- | -------------------------- |
| `process_id` | `str` | Identifier of the process. |

**Returns** → `bool` – **True** if the process has completed, **False** otherwise.

---

### `get_descriptions()`

```python
def get_descriptions(
    self,
    instance_id: str = "",
    table_name: str = "",
) -> dict:
```

Retrieves the stored description metadata.

| Parameter     | Type  | Description                                                          | Default |
| ------------- | ----- | -------------------------------------------------------------------- | ------- |
| `instance_id` | `str` | Instance ID to describe (empty ⇒ DB-level or *table\_name* level).   | `""`    |
| `table_name`  | `str` | Table whose description is requested (ignored if `instance_id` set). | `""`    |

**Returns** → `dict` – description dictionary for the requested entity.

---

### `get_artifact_folder()`

```python
def get_artifact_folder(
    self,
    table_name: str,
    instance_id: str = "",
    version: str = "base",
    is_temp: bool = True,
) -> str:
```

Retrieves the path to the artifact folder for a given table instance.
If `allow_multiple_artifacts` is **False** for the table, the instance is not temporary, *and* the instance was successfully executed, the folder for the whole table is returned.

| Parameter     | Type   | Description                                                                     | Default                        |
| ------------- | ------ | ------------------------------------------------------------------------------- | ------------------------------ |
| `table_name`  | `str`  | Name of the table.                                                              | –                              |
| `instance_id` | `str`  | Table‑instance ID.                                                              | `""`                           |
| `version`     | `str`  | Version string. When *instance\_id* is omitted, fetches latest of this version. | `"base"`                         |
| `is_temp`     | `bool` | **True** ⇒ path to temporary instance; **False** ⇒ last materialised instance.  | `True`                         |

**Returns** → `str` – path to the requested artifact folder.

---


### `get_builders_list()`

```python
def get_builders_list(
    self,
    table_name: str,
    instance_id: str = '',
    version: str = "base",
    is_temp: bool = True,
) -> list[str]:
```
Retrieve a list of builder names contained in an instance.

| Parameter     | Type   | Description                                                      | Default                        |
| ------------- | ------ | ---------------------------------------------------------------- | ------------------------------ |
| `table_name`  | `str`  | Target table name.                                               | –                              |
| `instance_id` | `str`  | Specific instance (empty ⇒ latest of *version*).                 | `''`                           |
| `version`     | `str`  | Version used when *instance\_id* omitted.                        | `"base"`                         |
| `is_temp`     | `bool` | **True** ⇒ look at temporary instance; **False** ⇒ materialised. | `True`                         |

**Returns** → `list[str]` – names of builder scripts in the instance.

---

### `get_builder_str()`

```python
def get_builder_str(
    self,
    table_name: str,
    builder_name: str = "",
    instance_id: str = "",
    version: str = "base",
    is_temp: bool = True,
) -> str:
```

Retrieve a builder file as plain text.

| Parameter      | Type   | Description                                      | Default                          |
| -------------- | ------ | ------------------------------------------------ | -------------------------------- |
| `table_name`   | `str`  | Table that owns the builder.                     | –                                |
| `builder_name` | `str`  | Name of the builder file (empty ⇒ inferred).     | `{table_name}_index` (converted) |
| `instance_id`  | `str`  | Specific instance (empty ⇒ latest of *version*). | `""`                             |
| `version`      | `str`  | Version used when *instance\_id* omitted.        | `"base"`                         |
| `is_temp`      | `bool` | **True** ⇒ read from temporary instance.         | `True`                           |

**Returns** → `str` – full source code of the builder.

---


### `get_code_modules_list()`

```python
def get_code_modules_list(self) -> list[str]:
```

Retrieves a list of module names contained in this repository.

**Returns** → `list[str]` – Python module names.

---

### `get_code_module_str()`

```python
def get_code_module_str(self, module_name: str) -> str:
```

Retrieve a code module file as plain text.

| Parameter     | Type  | Description                  |
| ------------- | ----- | ---------------------------- |
| `module_name` | `str` | Module name (without “.py”). |

**Returns** → `str` – module source code.

---


## Utility Functions

---

These functions help transport and delete a TableVault repository.

### `compress_vault()`

```python
def compress_vault(db_dir: str, preset: int = 6) -> None:
```

| Parameter | Type  | Description                                                            | Default |
| --------- | ----- | ---------------------------------------------------------------------- | ------- |
| `db_dir`  | `str` | Path to the TableVault directory to compress.                          | –       |
| `preset`  | `int` | LZMA compression level (1-9); higher is slower but smaller.            | `6`     |

**Raises** → `FileNotFoundError` – If *db\_dir* does not exist or is not a directory.

---

### `decompress_vault()`

```python
def decompress_vault(db_dir: str) -> None:
```

| Parameter | Type  | Description                                                                                             |
| --------- | ----- | ------------------------------------------------------------------------------------------------------- |
| `db_dir`  | `str` | Path to the TableVault directory (without `.tar.xz` extension, e.g., `my_vault` for `my_vault.tar.xz`). |

**Raises** → `FileNotFoundError` – If the expected archive file (`{db_dir}.tar.xz`) is missing.

---

### `delete_vault()`

```python
def delete_vault(db_dir: str) -> None:
```

| Parameter | Type  | Description                                 |
| --------- | ----- | ------------------------------------------- |
| `db_dir`  | `str` | Base directory of the TableVault to delete. |