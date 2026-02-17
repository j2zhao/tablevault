# Session Management

TableVault provides mechanisms for coordinating execution between multiple Python sessions. This enables workflows where one session can request another to stop or pause at safe checkpoints.

## Session Overview

When you create a `Vault` object, TableVault automatically:

1. Creates a session record in the database
2. Tracks the process ID (PID) of the running Python process
3. Records all executed code (cells in notebooks, full script in scripts)
4. Monitors for interrupt requests from other sessions

### Session Types

TableVault distinguishes between two execution types:

- **Notebook sessions**: Each cell execution is recorded as a separate item
- **Script sessions**: The entire script is recorded as a single item

The session type is automatically detected based on your execution environment.

## Cross-Session Communication

Sessions can send control requests to other running sessions. This is useful for:

- Stopping long-running experiments
- Pausing data processing pipelines
- Coordinating parallel ML workflows

### Requesting Stop

Stop a session by name:

```python
# In session A
vault.stop_execution("experiment_session")
```

The target session will terminate at its next checkpoint.

### Requesting Pause

Pause a session (can be resumed later):

```python
# In session A
vault.pause_execution("data_pipeline")
```

The target session will suspend at its next checkpoint.

### Resuming a Paused Session

Resume a previously paused session:

```python
# In session A
vault.resume_execution("data_pipeline")
```

!!! note "Single Machine Limitation"
    Resume functionality currently only works when all sessions are running on the same machine/container, as it uses process signals.

## Checkpoints

Control requests (stop/pause) are only executed at **checkpoints**. This ensures that:

- Operations complete atomically
- API calls aren't interrupted mid-flight
- Database transactions finish properly

### Defining Checkpoints

Add checkpoints in your code where it's safe to stop:

```python
for batch in data_batches:
    # Process batch
    results = process_batch(batch)
    vault.append_record("results", results)

    # Safe point to check for stop/pause requests
    vault.checkpoint_execution()
```

### Checkpoint Behavior

When a checkpoint is reached:

1. TableVault checks for pending interrupt requests
2. If a **pause** request exists: the process is suspended
3. If a **stop** request exists: the process is terminated
4. If no requests exist: execution continues normally

## Example: Coordinated ML Workflow

### Main Controller Session

```python
from tablevault import Vault

vault = Vault(
    user_id="researcher",
    session_name="controller",
    new_arango_db=False
)

# Check status of running experiments
operations = vault.get_current_operations()
print(f"Active operations: {operations}")

# Stop an experiment that's taking too long
vault.stop_execution("slow_experiment")

# Pause data ingestion while we analyze
vault.pause_execution("data_ingestion")

# ... do analysis ...

# Resume data ingestion
vault.resume_execution("data_ingestion")
```

### Worker Session

```python
from tablevault import Vault

vault = Vault(
    user_id="researcher",
    session_name="slow_experiment",
    new_arango_db=False
)

for epoch in range(1000):
    # Training loop
    loss = train_epoch(model, data)

    # Record results
    vault.append_record("training_logs", {
        "epoch": epoch,
        "loss": loss
    })

    # Checkpoint: safe to stop/pause here
    vault.checkpoint_execution()
```

## Parent-Child Session Relationships

Sessions can be linked in a parent-child hierarchy. This is useful when one script spawns others:

```python
# Parent session
parent_vault = Vault(
    user_id="researcher",
    session_name="hyperparameter_search"
)

# ... spawn child processes ...
```

```python
# Child session (spawned by parent)
child_vault = Vault(
    user_id="researcher",
    session_name="experiment_run_1",
    parent_session_name="hyperparameter_search",
    parent_session_index=0  # Index in parent's code
)
```

This relationship enables:

- Querying sessions by parent code (`parent_code_text` parameter)
- Understanding experiment provenance
- Tracking which parent spawned which experiments

## Cleanup Operations

If sessions crash or exit unexpectedly, operations may remain in an incomplete state. Use cleanup to recover:

```python
# Clean up operations older than 60 seconds
vault.vault_cleanup(interval=60)

# Clean up specific timestamps
vault.vault_cleanup(selected_timestamps=[1234567890, 1234567891])
```

## Querying Sessions

Find sessions based on various criteria:

```python
# Find sessions by code content
sessions = vault.query_session_list(code_text="import pandas")

# Find sessions by parent code
sessions = vault.query_session_list(parent_code_text="spawn_worker")

# Find sessions by description
sessions = vault.query_session_list(description_text="training pipeline")

# Combine filters
sessions = vault.query_session_list(
    code_text="model.fit",
    parent_code_text="hyperparameter_search",
    filtered=["exp_1", "exp_2", "exp_3"]
)
```

## Error Handling

Sessions automatically record errors:

- **In scripts**: Uncaught exceptions are captured
- **In notebooks**: Cell errors are captured

This information is stored with the session code and can be queried for debugging.
