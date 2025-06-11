# TableVault Python Interface

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
| `verbose`     | `bool` | If **True**, prints detailed logs of every operation.                     | `True`  |

---

## `TableVault` Data Creation Methods

### `create_table()`

```python
def create_table(
    self,
    table_name: str,
    allow_multiple_artifacts: bool = False,
    has_side_effects: bool = False,
    process_id: str = "",
    description: str = "",
) -> str:
```

| Parameter                  | Type   | Description                                                                    | Default |
| -------------------------- | ------ | ------------------------------------------------------------------------------ | ------- |
| `table_name`               | `str`  | Name of the new table.                                                         | –       |
| `allow_multiple_artifacts` | `bool` | **True** ⇒ instance has own artifact folder; **False** ⇒ one folder, one active. | `False` |
| `has_side_effects`         | `bool` | **True** ⇒ builders have side effects (e.g. API calls).                         | `False` |
| `process_id`               | `str`  | Calling process identifier.                                                    | `""`    |
| `description`              | `str`  | Description for the table.                                                     | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `create_instance()`

```python
def create_instance(
    self,
    table_name: str,
    version: str = "",
    origin_id: str = "",
    origin_table: str = "",
    external_edit: bool = False,
    copy: bool = True,
    builders: Optional[dict[str, str] | list[str]] = None,
    process_id: str = "",
    description: str = "",
) -> str:
```

| Parameter       | Type                                      | Description                                                                              | Default                        |
| --------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------ |
| `table_name`    | `str`                                     | Name of the table.                                                                       | –                              |
| `version`       | `str`                                     | Version of the table; empty ⇒ `BASE_TABLE_VERSION`.                                       | `""`                           |
| `origin_id`     | `str`                                     | If supplied, copy state from this existing instance ID.                                  | `""`                           |
| `origin_table`  | `str`                                     | Table for `origin_id`; empty ⇒ `table_name`.                                             | `""`                           |
| `external_edit` | `bool`                                    | **True** ⇒ instance edited externally, no builders constructed.                          | `False`                        |
| `copy`          | `bool`                                    | **True** (no `origin_id`) ⇒ use latest materialised instance as origin if it exists.     | `True`                         |
| `builders`      | `Optional[dict[str, str] \| list[str]]`   | List of new builder names to generate.                                                   | `None`                         |
| `process_id`    | `str`                                     | Calling process identifier.                                                              | `""`                           |
| `description`   | `str`                                     | Description for this instance.                                                           | `""`                           |

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

| Parameter     | Type  | Description                       | Default |
| ------------- | ----- | --------------------------------- | ------- |
| `module_name` | `str` | Name for the new module.          | `""`    |
| `copy_dir`    | `str` | Directory or Python file to copy. | `""`    |
| `process_id`  | `str` | Calling process identifier.       | `""`    |

**Returns** → `str` – process ID of this operation.

---


### `create_builder_file()`

```python
def create_builder_file(
    self,
    table_name: str,
    builder_name: str = "",
    version: str = constants.BASE_TABLE_VERSION,
    copy_dir: str = "",
    process_id: str = "",
) -> str:
```

| Parameter      | Type  | Description                           | Default                        |
| -------------- | ----- | ------------------------------------- | ------------------------------ |
| `table_name`   | `str` | Name of the table.                    | –                              |
| `builder_name` | `str` | Builder file name; empty ⇒ inferred.  | `{table_name}_index`           |
| `version`      | `str` | Version of the table.                 | `constants.BASE_TABLE_VERSION` |
| `copy_dir`     | `str` | Directory containing builder file(s). | `""`                           |
| `process_id`   | `str` | Calling process identifier.           | `""`                           |

**Returns** → `str` – process ID of this operation.

---

## `TableVault` Instance Materialization Methods

### `write_instance()`

```python
def write_instance(
    self,
    table_df: pd.DataFrame,
    table_name: str,
    version: str = constants.BASE_TABLE_VERSION,
    dependencies: Optional[list[tuple[str, str]]] = None,
    dtypes: Optional[dict[str, str]] = None,
    process_id: str = "",
) -> str:
```

| Parameter      | Type                                  | Description                                                           | Default                        |
| -------------- | ------------------------------------- | --------------------------------------------------------------------- | ------------------------------ |
| `table_df`     | `pd.DataFrame`                        | Data to write.                                                        | –                              |
| `table_name`   | `str`                                 | Target table.                                                         | –                              |
| `version`      | `str`                                 | Target version.                                                       | `constants.BASE_TABLE_VERSION` |
| `dependencies` | `Optional[list[tuple[str, str]]]`     | List of `(table_name, instance_id)` dependencies. None for no deps. | `None`                         |
| `dtypes`       | `Optional[dict[str, str]]`            | `{column: pandas-dtype}`. None for nullable defaults.                 | `None`                         |
| `process_id`   | `str`                                 | Calling process identifier.                                           | `""`                           |

**Returns** → `str` – The process ID of the executed write operation.

---

### `execute_instance()`

```python
def execute_instance(
    self,
    table_name: str,
    version: str = constants.BASE_TABLE_VERSION,
    force_execute: bool = False,
    process_id: str = "",
    background: bool = False,
) -> str:
```

| Parameter       | Type   | Description                                                                 | Default                        |
| --------------- | ------ | --------------------------------------------------------------------------- | ------------------------------ |
| `table_name`    | `str`  | Name of the table to materialise.                                           | –                              |
| `version`       | `str`  | Version of the table.                                                       | `constants.BASE_TABLE_VERSION` |
| `force_execute` | `bool` | **True** ⇒ force full rebuild; **False** ⇒ reuse origin if possible.        | `False`                        |
| `process_id`    | `str`  | Calling process identifier.                                                 | `""`                           |
| `background`    | `bool` | **True** ⇒ run materialisation in background.                               | `False`                        |

**Returns** → `str` – process ID of this operation.

---



## `TableVault` Data Deletion/Modification Methods

### `rename_table()`

```python
def rename_table(
    self, new_table_name: str, table_name: str, process_id: str = ""
) -> str:
```

| Parameter        | Type  | Description                 | Default |
| ---------------- | ----- | --------------------------- | ------- |
| `new_table_name` | `str` | New table name.             | –       |
| `table_name`     | `str` | Current table name.         | –       |
| `process_id`     | `str` | Calling process identifier. | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_table()`

```python
def delete_table(self, table_name: str, process_id: str = "") -> str:
```

| Parameter    | Type  | Description                  | Default |
| ------------ | ----- | ---------------------------- | ------- |
| `table_name` | `str` | Name of the table to delete. | –       |
| `process_id` | `str` | Calling process identifier.  | `""`    |

**Returns** → `str` – process ID of this operation.

---


### `delete_instance()`

```python
def delete_instance(
    self, instance_id: str, table_name: str, process_id: str = ""
) -> str:
```

| Parameter     | Type  | Description                       | Default |
| ------------- | ----- | --------------------------------- | ------- |
| `instance_id` | `str` | ID of the instance to delete.     | –       |
| `table_name`  | `str` | Name of the table owns instance.  | –       |
| `process_id`  | `str` | Calling process identifier.       | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_code_module()`

```python
def delete_code_module(self, module_name: str, process_id: str = "") -> str:
```

| Parameter     | Type  | Description                   | Default |
| ------------- | ----- | ----------------------------- | ------- |
| `module_name` | `str` | Name of the module to delete. | –       |
| `process_id`  | `str` | Calling process identifier.   | `""`    |

**Returns** → `str` – process ID of this operation.

---

### `delete_builder_file()`

```python
def delete_builder_file(
    self,
    builder_name: str,
    table_name: str,
    version: str = constants.BASE_TABLE_VERSION,
    process_id: str = "",
) -> str:
```

| Parameter      | Type  | Description                         | Default                        |
| -------------- | ----- | ----------------------------------- | ------------------------------ |
| `builder_name` | `str` | Name of the builder file to delete. | –                              |
| `table_name`   | `str` | Owning table name.                  | –                              |
| `version`      | `str` | Version of the table.               | `constants.BASE_TABLE_VERSION` |
| `process_id`   | `str` | Calling process identifier.         | `""`                           |

**Returns** → `str` – process ID of this operation.

---

## `TableVault` Process Methods


### `generate_process_id()`

```python
def generate_process_id(self) -> str:
```

**Returns** → `str` – A new, unique process identifier.

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
| `process_id`         | `str`  | ID of the calling process (audit).                            | `""`    |

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
    version: str = constants.BASE_TABLE_VERSION,
    active_only: bool = True,
    successful_only: bool = False,
    safe_locking: bool = True,
    rows: Optional[int] = None,
    full_artifact_path: bool = True,
) -> tuple[pd.DataFrame, str]:
```

| Parameter            | Type            | Description                                                            | Default                        |
| -------------------- | --------------- | ---------------------------------------------------------------------- | ------------------------------ |
| `table_name`         | `str`           | Name of the table.                                                     | –                              |
| `instance_id`        | `str`           | Specific instance ID; empty ⇒ latest of *version*.                     | `""`                           |
| `version`            | `str`           | Version when *instance\_id* omitted.                                   | `constants.BASE_TABLE_VERSION` |
| `active_only`        | `bool`          | **True** ⇒ consider only active instances.                             | `True`                         |
| `successful_only`    | `bool`          | **True** ⇒ consider only *successful* runs.                            | `False`                        |
| `safe_locking`       | `bool`          | **True** ⇒ acquire locks.                                              | `True`                         |
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
    safe_locking: bool = True,
) -> rich.tree.Tree:
```

Return a RichTree object of files contained in the target.

| Parameter        | Type   | Description                                | Default |
| ---------------- | ------ | ------------------------------------------ | ------- |
| `instance_id`    | `str`  | Resolve a specific instance (else latest). | `""`    |
| `table_name`     | `str`  | Limit to a table (optional).               | `""`    |
| `code_files`     | `bool` | Include stored *code* modules.             | `True`  |
| `builder_files`  | `bool` | Include builder scripts.                   | `True`  |
| `metadata_files` | `bool` | Include JSON/YAML metadata.                | `False` |
| `artifact_files` | `bool` | Include artifact directory contents.       | `False` |
| `safe_locking`   | `bool` | Acquire read locks while generating tree.  | `True`  |

**Returns** → `rich.tree.Tree` – printable file-tree representation.

---

### `get_instances()`

```python
def get_instances(
    self,
    table_name: str,
    version: str = constants.BASE_TABLE_VERSION,
) -> list[str]:
```

Return a list of materialised instance IDs for a specific table and version.

| Parameter    | Type  | Description           | Default                        |
| ------------ | ----- | --------------------- | ------------------------------ |
| `table_name` | `str` | Name of the table.    | –                              |
| `version`    | `str` | Version of the table. | `constants.BASE_TABLE_VERSION` |

**Returns** → `list[str]` – materialised instance IDs for this table/version.

---

### `get_active_processes()`

```python
def get_active_processes(self) -> ActiveProcessDict:
```

Return a dictionary of currently active processes in the vault.
Each key is a process ID and each value is metadata about that process.

**Returns** → `ActiveProcessDict` – alias `dict[str, Mapping[str, Any]]`.

---


### `get_process_completion()`

```python
def get_process_completion(self, process_id: str) -> bool:
```

Return the completion status of a specific process.

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

Fetch the stored description metadata.

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
    version: str = constants.BASE_TABLE_VERSION,
    is_temp: bool = True,
) -> str:
```

Return the path to the artifact folder for a given table instance.
If `allow_multiple_artifacts` is **False** for the `Table` *and* the instance is not temporary, the folder for the whole table is returned.

| Parameter     | Type   | Description                                                                     | Default                        |
| ------------- | ------ | ------------------------------------------------------------------------------- | ------------------------------ |
| `table_name`  | `str`  | Name of the table.                                                              | –                              |
| `instance_id` | `str`  | Table‑instance ID.                                                              | `""`                           |
| `version`     | `str`  | Version string. When *instance\_id* is omitted, fetches latest of this version. | `constants.BASE_TABLE_VERSION` |
| `is_temp`     | `bool` | **True** ⇒ path to temporary instance; **False** ⇒ last materialised instance.  | `True`                         |

**Returns** → `str` – path to the requested artifact folder.

---


### `get_builders_list()`

```python
def get_builders_list(
    self,
    table_name: str,
    instance_id: str = "",
    version: str = constants.BASE_TABLE_VERSION,
    is_temp: bool = True,
) -> list[str]:
```

List builder scripts stored within a specific table instance.

| Parameter     | Type   | Description                                                      | Default                        |
| ------------- | ------ | ---------------------------------------------------------------- | ------------------------------ |
| `table_name`  | `str`  | Target table name.                                               | –                              |
| `instance_id` | `str`  | Specific instance (empty ⇒ latest of *version*).                 | `""`                           |
| `version`     | `str`  | Version used when *instance\_id* omitted.                        | `constants.BASE_TABLE_VERSION` |
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
    version: str = constants.BASE_TABLE_VERSION,
    is_temp: bool = True,
) -> str:
```

Retrieve a stored builder script as plain text.

| Parameter      | Type   | Description                                      | Default                          |
| -------------- | ------ | ------------------------------------------------ | -------------------------------- |
| `table_name`   | `str`  | Table that owns the builder.                     | –                                |
| `builder_name` | `str`  | Name of the builder file (empty ⇒ inferred).     | `{table_name}_index` (converted) |
| `instance_id`  | `str`  | Specific instance (empty ⇒ latest of *version*). | `""`                             |
| `version`      | `str`  | Version used when *instance\_id* omitted.        | `constants.BASE_TABLE_VERSION`   |
| `is_temp`      | `bool` | **True** ⇒ read from temporary instance.         | `True`                           |

**Returns** → `str` – full source code of the builder.

---


### `get_code_modules_list()`

```python
def get_code_modules_list(self) -> list[str]:
```

Return the names of code modules saved in the repository.

**Returns** → `list[str]` – Python module names.

---

### `get_code_module_str()`

```python
def get_code_module_str(self, module_name: str) -> str:
```

Return the text of a stored code module.

| Parameter     | Type  | Description                  |
| ------------- | ----- | ---------------------------- |
| `module_name` | `str` | Module name (without “.py”). |

**Returns** → `str` – module source code.

---


## Helper Functions

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

| Parameter | Type  | Description                                                                               |
| --------- | ----- | ----------------------------------------------------------------------------------------- |
| `db_dir`  | `str` | Path to the TableVault directory (without `.tar.xz` extension, e.g., `my_vault` for `my_vault.tar.xz`). |

**Raises** → `FileNotFoundError` – If the expected archive file (`{db_dir}.tar.xz`) is missing.

---

### `delete_vault()`

```python
def delete_vault(db_dir: str) -> None:
```

| Parameter | Type  | Description                             |
| --------- | ----- | --------------------------------------- |
| `db_dir`  | `str` | Base directory of the TableVault to delete. |