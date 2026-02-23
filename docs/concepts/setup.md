# Repository Setup

This guide covers setting up ArangoDB and initializing a TableVault process.

## Local Setup of ArangoDB

There are various ways to set up an ArangoDB database. For quick development, we use a local Docker container as an example.

### Using Docker

Run the ArangoDB container:

```bash
docker run -d --name tablevault-arango \
    -e ARANGO_ROOT_PASSWORD=passwd \
    -p 8529:8529 \
    arangodb/arangodb \
    arangod --experimental-vector-index=true
```

This starts ArangoDB with:

- Root password: `passwd`
- Port: `8529`
- Container name: `tablevault-arango`

You can verify the database is running by visiting `http://localhost:8529` in your browser.

## Setting Up a TableVault Process

To interact with a TableVault repository, you must create a `Vault` object. The Vault represents a tracked process within a Python script or notebook.

### Basic Initialization

```python
from tablevault import Vault

vault = Vault(
    user_id="my_user",
    process_name="experiment_01"
)
```

!!! note "Unique Process Name"
    Processes are considered a unique type of item list. The process name is user-defined but must be unique among all item lists in a TableVault repository.

### Full Initialization with Custom Parameters

```python
from tablevault import Vault

vault = Vault(
    user_id="my_user",
    process_name="experiment_01",
    parent_process_name="",           # Name of parent process (if spawned)
    parent_process_index=0,           # Index within parent process
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
| `process_name` | Unique name for this process |
| `parent_process_name` | Parent process name (for spawned processes) |
| `parent_process_index` | Index in parent process |
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
vault1 = Vault(user_id="user1", process_name="process1")

# Second call with same params returns the same object
vault2 = Vault(user_id="user1", process_name="process1")

assert vault1 is vault2  # True
```

Attempting to create a vault with different parameters raises an error:

```python
vault1 = Vault(user_id="user1", process_name="process1")
vault2 = Vault(user_id="user1", process_name="process2")  # RuntimeError!
```

## Automatic Code Tracking

Once a Vault is created, TableVault automatically tracks executed code:

- **In Python scripts**: The entire script is stored as one data item in a process list
- **In Jupyter notebooks**: Each executed cell is stored as a separate data item (in order)

This tracking enables querying items by the code that created them.

## Advanced Information

### Connecting to an Existing Repository

To connect to an existing TableVault database without dropping it:

```python
vault = Vault(
    user_id="my_user",
    process_name="analysis_process",
    new_arango_db=False,  # Don't drop existing database
    arango_db="tablevault",
    arango_url="http://localhost:8529",
    arango_username="tablevault_user",
    arango_password="tablevault_password"
)
```

### Custom Embedding Dimensions

The `description_embedding_size` parameter defines the embedding size for all descriptions of item lists in the TableVault repository. Set it based on your chosen embedding model.

```python
vault = Vault(
    user_id="my_user",
    process_name="bert_experiment",
    description_embedding_size=768  # Match your model's output
)
```
