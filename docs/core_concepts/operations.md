# Operations

TableVault manages every write operation through a multi-step process designed for safety and recoverability. This process involves distinct **setup**, **execution**, and **takedown** phases to ensure the repository remains in a consistent state, even if an operation is interrupted.

For simple examples of how to use operations, check out [Basic Workflow](../workflows/workflow.md) and [Worflow with Artifacts](../workflows/workflow_artifacts.md).

For a detailed list of available operations, please read the [Core API](../api/core_api.md).


---

## Process IDs, Pauses, and Restarts

Operations can include an optional `process_id`:

* **Without a provided `process_id`**: Unexpected interruptions cause a safe revert to the pre-operation state.
* **With a provided `process_id`**: Interruptions pause the operation, maintaining locks. Users can restart exactly where paused by reusing the same `process_id` or setting `restart` to `True`.

Explicitly stopping active processes uses `stop_process()`. Logs of operations reside in `TABLEVAULT_NAME/metadata/logs.txt`.

For detailed use scenarios of `process_id` and basic error handling procedures, check ou [Handling Execution Errors](../workflows/errors.md).

---

## The Core Transactional Steps

Each write operation follows a series of universal, transactional steps:

1.  **Process Initialization**: TableVault generates a unique internal `process_id` if one is not provided.

2.  **Setup Phase**: An operation-specific setup function prepares for the main task. It is responsible for validation (e.g., checking for illegal arguments), acquiring **exclusive and shared locks** on resources, creating a temporary backup of data to be modified, and preparing arguments for the execution phase.

3.  **Execution Phase**: If setup is successful, the main operation function is called. The system supports **background execution** for `execute_instance()` calls by spawning a new Python process.

4.  **Takedown Phase**: After execution, a takedown function cleans up the process. On success, it removes the temporary backup and releases locks. On failure, it uses the backup to **restore the original data**, effectively rolling back any changes before releasing locks.

5.  **Logging**: The operation's status is logged to persistent storage throughout the entire process to ensure system resiliency.

For a list of available operations, please read our [core API](../api/core_api.md).

---

## Example: The Delete Operation

The `setup_delete_instance()` and `takedown_delete_instance()` internal functions provide a concrete example of this process.

  * **Setup (`setup_delete_instance()`)**: Validates that the table and instance exist, acquires an exclusive lock, copies the instance to a temporary directory, and logs the `table_name` and `instance_id` for cleanup.
  * **Takedown (`takedown_delete_instance()`)**: If the main logic failed, it restores the instance from the temporary directory. If it succeeded, it deletes the temporary backup. Finally, it releases all locks.

TableVault ensures operations are transactional, leaving repositories either fully updated or reverted safely.
