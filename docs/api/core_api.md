# Core API Reference

The `Vault` class is the main interface for tracking ML items and their lineage.

---

## Initialization

### `Vault`

```python
Vault(
    user_id: str,
    process_name: str,
    parent_process_name: str = "",
    parent_process_index: int = 0,
    arango_url: str = "http://localhost:8529",
    arango_db: str = "tablevault",
    arango_username: str = "tablevault_user",
    arango_password: str = "tablevault_password",
    new_arango_db: bool = True,
    arango_root_username: str = "root",
    arango_root_password: str = "passwd",
    description_embedding_size: int = 1024,
    log_file_location: str = "~/.tablevault/logs/",
) -> Vault
```

Initialize the Vault singleton. Only one vault can be active per Python process. Once active, all subsequently executed code is tracked in the TableVault repository.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `user_id` | `str` | Unique identifier for the user |
| `process_name` | `str` | Unique name for this process |
| `parent_process_name` | `str` | Name of the generating process (if exists) |
| `parent_process_index` | `int` | Index of the generating process (if exists) |
| `arango_url` | `str` | URL of the ArangoDB server |
| `arango_db` | `str` | Name of the database to use |
| `arango_username` | `str` | Username for database access |
| `arango_password` | `str` | Password for database access |
| `new_arango_db` | `bool` | If True, create a new database (drops existing) |
| `arango_root_username` | `str` | Root username for database creation |
| `arango_root_password` | `str` | Root password for database creation |
| `description_embedding_size` | `int` | Dimension of description embeddings |
| `log_file_location` | `str` | Directory for log files |

**Returns:** `Vault` instance

---

## Create Functions

Functions for creating new item lists.

### `create_file_list`

```python
create_file_list(item_name: str) -> None
```

Create a new file list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Unique name for the file list |

---

### `create_document_list`

```python
create_document_list(item_name: str) -> None
```

Create a new document list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Unique name for the document list |

---

### `create_embedding_list`

```python
create_embedding_list(item_name: str, ndim: int) -> None
```

Create a new embedding list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Unique name for the embedding list |
| `ndim` | `int` | Dimensionality of the embeddings in this list |

---

### `create_record_list`

```python
create_record_list(item_name: str, column_names: List[str]) -> None
```

Create a new record list with specified column names.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Unique name for the record list |
| `column_names` | `List[str]` | List of column names for records in this list |

---

### `create_description`

```python
create_description(
    item_name: str,
    description: str,
    embedding: List[float],
    description_name: str = "BASE"
) -> None
```

Adds a joint text and embedding description to an item list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item to describe |
| `description` | `str` | Text description of the item |
| `embedding` | `List[float]` | Embedding vector for the description |
| `description_name` | `str` | Label for this description (default "BASE") |

---

## Append Functions

Functions for appending content to existing item lists.

### `append_file`

```python
append_file(
    item_name: str,
    location: str,
    input_items: Optional[InputItems] = None,
    index: Optional[int] = None
) -> None
```

Append a file reference to a file list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the file list to append to |
| `location` | `str` | File path or location string |
| `input_items` | `Optional[InputItems]` | Mapping of dependency item key → [start_position, end_position] |
| `index` | `Optional[int]` | Specific index to insert at (appends to end if None) |

---

### `append_document`

```python
append_document(
    item_name: str,
    text: str,
    input_items: Optional[InputItems] = None,
    index: Optional[int] = None,
    start_position: Optional[int] = None
) -> None
```

Append a text chunk to a document list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the document list to append to |
| `text` | `str` | Text content of the document |
| `input_items` | `Optional[InputItems]` | Mapping of dependency item key → [start_position, end_position] |
| `index` | `Optional[int]` | Specific index to insert at (appends to end if None) |
| `start_position` | `Optional[int]` | Character position within the document stream |

!!! note
    Both `index` and `start_position` must be provided together when specifying manual positions.

---

### `append_embedding`

```python
append_embedding(
    item_name: str,
    embedding: List[float],
    input_items: Optional[InputItems] = None,
    index: Optional[int] = None,
    build_idx: bool = True,
    index_rebuild_count: int = 10000
) -> None
```

Append an embedding vector to an embedding list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the embedding list to append to |
| `embedding` | `List[float]` | The embedding vector to store |
| `input_items` | `Optional[InputItems]` | Mapping of dependency item key → [start_position, end_position] |
| `index` | `Optional[int]` | Specific index to insert at (appends to end if None) |
| `build_idx` | `bool` | Whether to rebuild the vector index |
| `index_rebuild_count` | `int` | Threshold for triggering index rebuild |

---

### `append_record`

```python
append_record(
    item_name: str,
    record: Dict[str, Any],
    input_items: Optional[InputItems] = None,
    index: Optional[int] = None
) -> None
```

Append a record (row) to a record list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the record list to append to |
| `record` | `Dict[str, Any]` | Dictionary with column names as keys and values |
| `input_items` | `Optional[InputItems]` | Mapping of dependency item key → [start_position, end_position] |
| `index` | `Optional[int]` | Specific index to insert at (appends to end if None) |

!!! note
    Top-level dictionary keys must match the initial column names defined when the record list was created.

---

## Operation Management

Functions for managing vault operations and cleanup.

### `get_current_operations`

```python
get_current_operations() -> Dict[str, Any]
```

Get all currently active operations.

**Returns:** Dictionary of active operation timestamps

---

### `vault_cleanup`

```python
vault_cleanup(
    interval: int = 60,
    selected_timestamps: Optional[List[int]] = None
) -> None
```

Clean up stale operations that have exceeded the interval.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `interval` | `int` | Time in seconds after which an operation is considered stale |
| `selected_timestamps` | `Optional[List[int]]` | If provided, only clean up these specific timestamps |

---

## Process Control

Functions for controlling process execution lifecycle.

### `checkpoint_execution`

```python
checkpoint_execution() -> None
```

Mark a safe checkpoint in code where stop and pause requests can be executed. This avoids stopping during undesirable conditions (e.g., while waiting for outgoing API calls).

---

### `pause_execution`

```python
pause_execution(process_name: str) -> None
```

Request to pause another process's execution.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `process_name` | `str` | Name of the process to pause |

---

### `stop_execution`

```python
stop_execution(process_name: str) -> None
```

Request to stop another process's execution.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `process_name` | `str` | Name of the process to stop |

---

### `resume_execution`

```python
resume_execution(process_name: str) -> None
```

Resume a paused process by name.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `process_name` | `str` | Name of the process list to resume |

!!! note
    Currently only works when processes are on the same machine or container.

---

## Delete Functions

Functions for deleting item lists.

### `delete_list`

```python
delete_list(item_name: str) -> None
```

Delete an item list's content.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list to delete |

---

## Utility Functions

Helper functions for checking vault state.

### `has_vector_index`

```python
has_vector_index(ndim: int) -> bool
```

Check if a vector index exists for embeddings of a given dimension.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `ndim` | `int` | Dimensionality of the embeddings |

**Returns:** `True` if a vector index exists for this dimension

---

## List Queries

Functions for querying across item lists with filtering and similarity search.

### `query_process_list`

```python
query_process_list(
    code_text: Optional[str] = None,
    parent_code_text: Optional[str] = None,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query process items. Can optionally filter by descriptions and parent process.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `code_text` | `Optional[str]` | Text to search in process code |
| `parent_code_text` | `Optional[str]` | Text to search in parent process code |
| `description_embedding` | `Optional[List[float]]` | Embedding vector for similarity search |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `filtered` | `Optional[List[str]]` | List of process names to restrict search to |

**Returns:** `List[List]` — one 5-element list per matching process run:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Process name |
| `[1]` | `int` | Run index of this process execution |
| `[2]` | `int` | Start position offset of this run in the process stream |
| `[3]` | `List[str]` | Matched description names; empty when no description filter applied |
| `[4]` | `List[[str, int]]` | Matched parent processes as `[process_name, process_index]`; empty when no `parent_code_text` filter applied |

---

### `query_embedding_list`

```python
query_embedding_list(
    embedding: Optional[List[float]] = None,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None,
    use_approx: bool = False
) -> List[Any]
```

Query embedding items. Can optionally filter by descriptions and parent process.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `embedding` | `Optional[List[float]]` | Query embedding vector for similarity search |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in process code |
| `filtered` | `Optional[List[str]]` | List of embedding names to restrict search to |
| `use_approx` | `bool` | Use approximate (faster) similarity search |

**Returns:** `List[List]` — one 5-element list per matching embedding entry:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Embedding list name |
| `[1]` | `int` | Position index of the entry within its embedding list |
| `[2]` | `int` | Numeric start position of the entry |
| `[3]` | `List[str]` | Matched description names; empty when no description filter applied |
| `[4]` | `List[[str, int]]` | Matched processes as `[process_name, process_index]`; empty when no `code_text` filter applied |

---

### `query_record_list`

```python
query_record_list(
    record_text: Optional[str] = None,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query record items. Can optionally filter by descriptions and parent process.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `record_text` | `Optional[str]` | Text to search in record data |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in process code |
| `filtered` | `Optional[List[str]]` | List of record names to restrict search to |

**Returns:** `List[List]` — one 5-element list per matching record entry:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Record list name |
| `[1]` | `int` | Position index of the entry within its record list |
| `[2]` | `int` | Numeric start position of the entry |
| `[3]` | `List[str]` | Matched description names; empty when no description filter applied |
| `[4]` | `List[[str, int]]` | Matched processes as `[process_name, process_index]`; empty when no `code_text` filter applied |

---

### `query_document_list`

```python
query_document_list(
    document_text: Optional[str] = None,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query document items. Can optionally filter by descriptions and parent process.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `document_text` | `Optional[str]` | Text to search in document content |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in process code |
| `filtered` | `Optional[List[str]]` | List of document names to restrict search to |

**Returns:** `List[List]` — one 5-element list per matching document chunk:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Document list name |
| `[1]` | `int` | Position index of the chunk within its document list |
| `[2]` | `int` | Character offset where this chunk begins |
| `[3]` | `List[str]` | Matched description names; empty when no description filter applied |
| `[4]` | `List[[str, int]]` | Matched processes as `[process_name, process_index]`; empty when no `code_text` filter applied |

---

### `query_file_list`

```python
query_file_list(
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query file items. Can optionally filter by descriptions and parent process.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in process code |
| `filtered` | `Optional[List[str]]` | List of file names to restrict search to |

**Returns:** `List[List]` — one 5-element list per matching file entry:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | File list name |
| `[1]` | `int` | Position index of the file entry within its file list |
| `[2]` | `int` | Numeric start position of the entry |
| `[3]` | `List[str]` | Matched description names; empty when no description filter applied |
| `[4]` | `List[[str, int]]` | Matched processes as `[process_name, process_index]`; empty when no `code_text` filter applied |

---

## Basic Queries

Functions for querying individual items and their relationships.

### `query_item_content`

```python
query_item_content(
    item_name: str,
    index: Optional[int] = None,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None
) -> Any
```

Query the content of an item list by index chunk or position range.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list to query |
| `index` | `Optional[int]` | Specific index chunk to retrieve |
| `start_position` | `Optional[int]` | Start of position range (if index not specified) |
| `end_position` | `Optional[int]` | End of position range (if index not specified) |

**Returns:** When `index` is given, a single item whose type depends on the list type:

| List type | Return type | Value |
|-----------|-------------|-------|
| `process_list` | `dict` | `{"text": str, "status": str, "error": str, "start_position": int, "index": int}` |
| `file_list` | `str` | File location/path |
| `embedding_list` | `List[float]` | Embedding vector |
| `document_list` | `str` | Text chunk |
| `record_list` | `Dict[str, Any]` | Record data keyed by column name |

When `index` is `None`, a `List` of the above types for all entries whose position range overlaps `[start_position, end_position)`, sorted by `start_position`.

---

### `query_item_names`

```python
query_item_names(item_type: str) -> List[str]
```

Get all item names of a given collection type.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_type` | `str` | Collection type to filter by: `"process_list"`, `"file_list"`, `"embedding_list"`, `"document_list"`, or `"record_list"` |

**Returns:** `List[str]` — sorted list of item names belonging to that collection type

---

### `query_item_type`

```python
query_item_type(item_list: List[str]) -> Dict[str, str]
```

Get the collection type for each item in a list of item names.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_list` | `List[str]` | List of item names to look up |

**Returns:** `Dict[str, str]` — mapping of item name to its collection type string. Items not found in the vault are omitted.

| Value | Description |
|-------|-------------|
| `"process_list"` | Process list |
| `"file_list"` | File list |
| `"embedding_list"` | Embedding list |
| `"document_list"` | Document list |
| `"record_list"` | Record list |

---

### `query_item_list`

```python
query_item_list(item_name: str) -> Dict[str, Any]
```

Get metadata for an item list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |

**Returns:** `Dict[str, Any]` — the list's metadata document. Common fields:

| Field | Type | Description |
|-------|------|-------------|
| `n_items` | `int` | Number of entries currently in the list |
| `length` | `int` | Total length/size (entry count for file/record/embedding lists; total character count for document lists) |
| `deleted` | `int` | Deletion marker (`-1` = not deleted) |

Additional fields by list type:

| List type | Extra field | Type | Description |
|-----------|-------------|------|-------------|
| `embedding_list` | `n_dim` | `int` | Dimensionality of stored embeddings |
| `record_list` | `column_names` | `List[str]` | Ordered column names |

---

### `query_item_parent`

```python
query_item_parent(
    item_name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None
) -> List[Any]
```

Query input dependencies of an item list. Allows optional position filtering.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |
| `start_position` | `Optional[int]` | Filter by start position |
| `end_position` | `Optional[int]` | Filter by end position |

**Returns:** `List[List]` — one 6-element list per dependency edge in the filtered range:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `int` | Start position of the parent entry that has this dependency |
| `[1]` | `int` | End position of the parent entry |
| `[2]` | `str` | Collection type of the input dependency (e.g. `"file_list"`, `"document_list"`) |
| `[3]` | `str` | Name of the input dependency item list |
| `[4]` | `int` | Start position within the dependency list |
| `[5]` | `int` | End position within the dependency list |

---

### `query_item_child`

```python
query_item_child(
    item_name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None
) -> List[Any]
```

Query items that depend on an item list. Allows optional position filtering.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |
| `start_position` | `Optional[int]` | Filter by start position |
| `end_position` | `Optional[int]` | Filter by end position |

**Returns:** `List[List]` — one 6-element list per outgoing dependency edge in the filtered range:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `int` | Start position of the dependency edge on this item |
| `[1]` | `int` | End position of the dependency edge on this item |
| `[2]` | `str` | Collection type of the dependent (child) item list (e.g. `"embedding_list"`, `"record_list"`) |
| `[3]` | `str` | Name of the child item list |
| `[4]` | `int` | Start position of the child entry |
| `[5]` | `int` | End position of the child entry |

---

### `query_item_description`

```python
query_item_description(item_name: str) -> List[str]
```

Get descriptions associated with an item list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |

**Returns:** `List[List]` — one 2-element list per description attached to this item:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Description label (e.g. `"BASE"`) |
| `[1]` | `str` | Full text of the description |

---

### `query_item_creation_process`

```python
query_item_creation_process(item_name: str) -> List[Dict[str, Any]]
```

Get the process that created an item list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |

**Returns:** `List[Dict[str, Any]]` — one dict per creating process:

| Key | Type | Description |
|-----|------|-------------|
| `process_id` | `str` | ArangoDB document ID of the creating process (e.g. `"process_list/my_process"`) |
| `index` | `int` | Run index at which the item was created |

---

### `query_item_process`

```python
query_item_process(
    item_name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None
) -> List[Dict[str, Any]]
```

Get processes that modified an item list. Can filter by position range within the list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |
| `start_position` | `Optional[int]` | Filter by start position |
| `end_position` | `Optional[int]` | Filter by end position |

**Returns:** `List[Dict[str, Any]]` — one dict per process that wrote entries in the filtered range:

| Key | Type | Description |
|-----|------|-------------|
| `process_id` | `str` | ArangoDB document ID of the process (e.g. `"process_list/my_process"`) |
| `index` | `int` | Run index at which the entries were written |

---

### `query_process_item`

```python
query_process_item(process_name: str) -> List[Dict[str, Any]]
```

Get all items created or modified by a given process name.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `process_name` | `str` | Name of the process list |

**Returns:** `List[Dict[str, Any]]` — one dict per item list touched by this process:

| Key | Type | Description |
|-----|------|-------------|
| `name` | `str` | Name of the item list |
| `start_position` | `int \| None` | Earliest start position written by this process; `None` if not recorded |
| `end_position` | `int \| None` | Latest end position written by this process; `None` if not recorded |

---

## Description Queries

Functions for searching across descriptions attached to any item list type.

### `query_description`

```python
query_description(
    description_text: str,
    k: int = 500,
    text_analyzer: str = "text_en"
) -> List[Any]
```

Search descriptions by token match across all data types. All tokens in `description_text` must match.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `description_text` | `str` | Text to search in descriptions (all tokens must match) |
| `k` | `int` | Maximum number of results to return |
| `text_analyzer` | `str` | ArangoSearch analyzer to use for tokenization |

**Returns:** `List[List]` — one 4-element list per matching description:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Description label (e.g. `"BASE"`) |
| `[1]` | `str` | Full text of the description |
| `[2]` | `str` | Name of the item list this description belongs to |
| `[3]` | `str` | Collection type of the item list (e.g. `"file_list"`, `"embedding_list"`) |

---

### `query_description_embedding`

```python
query_description_embedding(
    embedding: List[float],
    k: int = 500,
    use_approx: bool = False
) -> List[Any]
```

Search descriptions by embedding similarity across all data types.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `embedding` | `List[float]` | Query embedding vector |
| `k` | `int` | Maximum number of results to return |
| `use_approx` | `bool` | Use approximate (faster) nearest-neighbor search when available |

**Returns:** `List[List]` — one 4-element list per matching description, sorted by descending cosine similarity:

| Index | Type | Description |
|-------|------|-------------|
| `[0]` | `str` | Description label (e.g. `"BASE"`) |
| `[1]` | `str` | Full text of the description |
| `[2]` | `str` | Name of the item list this description belongs to |
| `[3]` | `str` | Collection type of the item list (e.g. `"file_list"`, `"embedding_list"`) |
