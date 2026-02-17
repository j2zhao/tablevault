# Repository Setup

This guide covers setting up ArangoDB and initializing a TableVault session.

## Local Setup of ArangoDB

There are various ways to set up an ArangoDB database. For quick development, we use a local Docker container as an example.

### Using Docker

Run the ArangoDB container:

```bash
docker run -d --name tablevault-arango \
    -e ARANGO_ROOT_PASSWORD=passwd \
    -p 8529:8529 \ arangodb/arangodb \
    arangod --experimental-vector-index=true
```

This starts ArangoDB with:

- Root password: `passwd`
- Port: `8529`
- Container name: `arangodb`

You can verify the database is running by visiting `http://localhost:8529` in your browser.

## Setting Up a TableVault Session

To interact with a TableVault repository, you must create a `Vault` object. The Vault represents a session in a Python process or notebook.

### Basic Initialization

```python
from tablevault import Vault

vault = Vault(
    user_id="my_user",
    session_name="experiment_01"
)
```

!!! note "Unique Session Name"
    Sessions are considered an unique type of item lists. The session name is user defined, but must be unique among all items lists in a TableVault repository.

### Full Initialization with Custom Parameters

```python
from tablevault import Vault

vault = Vault(
    user_id="my_user",
    session_name="experiment_01",
    parent_session_name="",           # Name of parent session (if spawned)
    parent_session_index=0,           # Index within parent session
    arango_url="http://localhost:8529",
    arango_db="tablevault",
    arango_username="tablevault_user",
    arango_password="tablevault_password",
    new_arango_db=True,               # Create new database (drops existing)
    arango_root_username="root",
    arango_root_password="passwd",
    description_embedding_size=1024,
    log_file_location="~/.tablevault/logs/"
)
```

### Parameter Reference

| Parameter | Description |
|-----------|-------------|
| `user_id` | Your unique identifier |
| `session_name` | Unique name for this session |
| `parent_session_name` | Parent session name (for spawned sessions) |
| `parent_session_index` | Index in parent session |
| `arango_url` | ArangoDB server URL |
| `arango_db` | Database name to use |
| `arango_username` | Database username |
| `arango_password` | Database password |
| `new_arango_db` | If `True`, creates a fresh database |
| `arango_root_username` | Root username for database creation |
| `arango_root_password` | Root password for database creation |
| `description_embedding_size` | Dimension for description embeddings |
| `log_file_location` | Directory for log files |

!!! warning "Database Creation"
    When `new_arango_db=True`, the existing database with that name will be dropped and recreated. Set this to `False` when connecting to an existing TableVault repository.

### Singleton Behavior

The `Vault` class is a singleton. Only one vault can be active per Python process. Subsequent calls with the same initialization parameters return the same Vault object:

```python
# First call creates the vault
vault1 = Vault(user_id="user1", session_name="session1")

# Second call with same params returns the same object
vault2 = Vault(user_id="user1", session_name="session1")

assert vault1 is vault2  # True
```

Attempting to create a vault with different parameters raises an error:

```python
vault1 = Vault(user_id="user1", session_name="session1")
vault2 = Vault(user_id="user1", session_name="session2")  # RuntimeError!
```

## Automatic Code Tracking

Once a Vault is created, TableVault automatically tracks executed code:

- **In Python scripts**: The entire script is stored as one data item in a session list
- **In Jupyter notebooks**: Each executed cell is stored as a separate data item (in order)

This tracking enables querying items by the code that created them.

## Advanced Information

### Connecting to an Existing Repository

To connect to an existing TableVault database without dropping it:

```python
vault = Vault(
    user_id="my_user",
    session_name="analysis_session",
    new_arango_db=False,  # Don't drop existing database
    arango_db="tablevault",
    arango_url="http://localhost:8529",
    arango_username="tablevault_user",
    arango_password="tablevault_password"
)
```

### Custom Embedding Dimensions

The `description_embedding_size` parameter defines the embedding size of all descriptions over item lists in the TableVault. You should define it based on your chosen embedding model.

```python
vault = Vault(
    user_id="my_user",
    session_name="bert_experiment",
    description_embedding_size=768  # Match your model's output
)
```
