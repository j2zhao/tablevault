# Core API Reference

The `Vault` class is the main interface for tracking ML items and their lineage.

---

## Initialization

### `Vault`

```python
Vault(
    user_id: str,
    session_name: str,
    parent_session_name: str,
    parent_session_index: str,
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

Initialize the Vault singleton. Only one vault can be active per Python process. Once active, all subsequently executed code is stored in the TableVault repository.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `user_id` | `str` | Identifier of the user (defined by) |
| `session_name` | `str` | Unique name for this session |
| `parent_session_name` | `str` | Name of the parent session  (If|
| `parent_session_index` | `str` | Index of the parent session |
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

Create a new file list item.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Unique name for the file list |

---

### `create_document_list`

```python
create_document_list(item_name: str) -> None
```

Create a new document list item.

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

## Operations & Session Manipulation

Functions for managing vault operations and session lifecycle.

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

### `checkpoint_execution`

```python
checkpoint_execution() -> None
```

Identify a safe checkpoint in code where stop and pause requests can be executed. Using this avoids stopping during undesirable conditions (e.g., while still waiting for outgoing API calls).

---

### `pause_execution`

```python
pause_execution(session_name: str) -> None
```

Request to pause another session list's current execution.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `session_name` | `str` | Name of the session to pause |

---

### `stop_execution`

```python
stop_execution(session_name: str) -> None
```

Request to stop another session list's current execution.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `session_name` | `str` | Name of the session to stop |

---

### `resume_execution`

```python
resume_execution(session_name: str) -> None
```

Resume a paused session list's current session by name.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `session_name` | `str` | Name of the session list to resume |

!!! note
    Only works in single machine/container case currently.

---

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

### `query_session_list`

```python
query_session_list(
    code_text: Optional[str] = None,
    parent_code_text: Optional[str] = None,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query session items. Can optionally filter by descriptions and parent session.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `code_text` | `Optional[str]` | Text to search in session code |
| `parent_code_text` | `Optional[str]` | Text to search in parent session code |
| `description_embedding` | `Optional[List[float]]` | Embedding vector for similarity search |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `filtered` | `Optional[List[str]]` | List of session names to restrict search to |

**Returns:** List of matching session results

---

### `query_embedding_list`

```python
query_embedding_list(
    embedding: List[float],
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None,
    use_approx: bool = False
) -> List[Any]
```

Query embedding items. Can optionally filter by descriptions and parent session.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `embedding` | `List[float]` | Query embedding vector for similarity search |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in session code |
| `filtered` | `Optional[List[str]]` | List of embedding names to restrict search to |
| `use_approx` | `bool` | Use approximate (faster) similarity search |

**Returns:** List of matching embedding results

---

### `query_record_list`

```python
query_record_list(
    record_text: str,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query record items. Can optionally filter by descriptions and parent session.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `record_text` | `str` | Text to search in record data |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in session code |
| `filtered` | `Optional[List[str]]` | List of record names to restrict search to |

**Returns:** List of matching record results

---

### `query_document_list`

```python
query_document_list(
    document_text: str,
    description_embedding: Optional[List[float]] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    filtered: Optional[List[str]] = None
) -> List[Any]
```

Query document items. Can optionally filter by descriptions and parent session.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `document_text` | `str` | Text to search in document content |
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in session code |
| `filtered` | `Optional[List[str]]` | List of document names to restrict search to |

**Returns:** List of matching document item results

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

Query file items. Can optionally filter by descriptions and parent session.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `description_embedding` | `Optional[List[float]]` | Embedding for description similarity |
| `description_text` | `Optional[str]` | Text to search in descriptions |
| `code_text` | `Optional[str]` | Text to search in session code |
| `filtered` | `Optional[List[str]]` | List of file names to restrict search to |

**Returns:** List of matching file item results

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

**Returns:** The item content at the specified index or position range

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

**Returns:** Dictionary with list metadata (n_items, length, etc.)

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

**Returns:** List of item list information

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

**Returns:** List of child item information

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

**Returns:** List of description texts

---

### `query_item_creation_session`

```python
query_item_creation_session(item_name: str) -> List[Dict[str, Any]]
```

Get the session that created an item list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |

**Returns:** List of session information with session_id and index

---

### `query_item_session`

```python
query_item_session(
    item_name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None
) -> List[Dict[str, Any]]
```

Get sessions that modified an item list. Can filter by interval range within the list.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `item_name` | `str` | Name of the item list |
| `start_position` | `Optional[int]` | Filter by start position |
| `end_position` | `Optional[int]` | Filter by end position |

**Returns:** List of session info dicts with session name and index

---

### `query_session_item`

```python
query_session_item(session_name: str) -> List[Dict[str, Any]]
```

Get all items created or modified by a given session name.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `session_name` | `str` | Name of the session list |

**Returns:** List of item dictionaries with name and position range