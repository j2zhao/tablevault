# Operations

Every write operation to the TableVault repository follows a multi-step, managed process designed for safety and recoverability. Orchestrated by the main `tablevault_operation` function, this process involves distinct **setup**, **execution**, and **takedown** phases. This structure ensures the database remains in a consistent state, even if an operation is interrupted by errors or external factors.

All operations that follow this pattern allow the user to include an optional `process_id` string, which uniquely identifies the operation to the system.

---

## Process IDs, Pauses, and Restarts

Each write operation in TableVault is identified by a unique `process_id` string. This ID is either provided by the user or generated internally by the system. A user can provide their own `process_id` by first creating one with the `generate_process_id` function from the core API. You can observe active operations using the `get_active_processes()` function.

The behavior of an interrupted operation depends on whether a `process_id` was provided:

* **Without a user-provided `process_id`**, if an operation encounters an unexpected error or is externally interrupted (e.g., by killing a Jupyter notebook cell), it is **safely reverted**. The system state will be as if the operation never started.

* **With a user-provided `process_id`**, an interruption only **pauses** the operation. It maintains its system locks, preventing other operations from accessing the same resources. Users can restart the *exact same* operation by re-running the function call with the same `process_id` or by setting `restart=True` in a new TableVault object. This is especially useful for long-running `execute_instance` operations that might be stopped. Note that the operation restarts from its last checkpoint, and its input arguments cannot be changed.

To explicitly stop and revert an active process, you can call the `stop_process` function. Internal TableVault errors, which typically indicate invalid user inputs, will always cause the operation to revert.

A record of all completed processes can be found in the `TABLEVAULT_NAME/metadata/logs.txt` file.

---

## The Core Transactional Steps

Each write operation follows a series of universal, transactional steps:

1.  **Process Initialization**: TableVault generates a unique `process_id` if one is not provided. If an ID is given, the system checks for existing logs associated with it. This allows a failed or paused operation to be resumed from where it left off.

2.  **Setup Phase**: An operation-specific setup function runs to prepare for the main task. It is responsible for:
    * **Validation**: Checking for illegal arguments, such as attempting to modify a protected table.
    * **Locking**: Acquiring **exclusive and shared locks** on the necessary resources to prevent conflicts during multi-processing.
    * **Creating a Backup**: Copying the data to be modified into a temporary location. This is critical for recovery.
    * **Passing Arguments**: Preparing and returning the arguments needed for the main execution function.

3.  **Execution Phase**: If the setup is successful, the main operation function is called. The system supports **background execution** for `execute_instance()` calls by spawning a new Python process, which allows the main program to continue without waiting.

4.  **Takedown Phase**: After execution finishes, an operation-specific takedown function cleans up the process:
    * **On Success**: The function removes the locks, signifying that the operation is complete and the data is in its new, consistent state. It then removes the temporary backup.
    * **On Failure**: The function uses the temporary backup to **restore the original data**, effectively rolling back any changes.
    * **Cleanup**: In either case, it releases all locks held by the process and deletes temporary data.

5.  **Logging**: Throughout this entire process, the operation's status (e.g., start, success, errors) is logged to persistent storage, making the system resilient.

---

## Example: The Delete Operation

The `setup_delete_instance` and `takedown_delete_instance` functions provide a concrete example of this process.

* **Setup (`setup_delete_instance`)**:
    1.  Validates that the table and the specific instance to be deleted both exist.
    2.  Acquires an exclusive lock on that table and instance ID.
    3.  Copies the instance to a temporary directory associated with the `process_id`.
    4.  Updates the process log with the `table_name` and `instance_id` so the takedown function knows what to clean up.

* **Takedown (`takedown_delete_instance`)**:
    1.  If the main deletion logic **failed**, it copies the data from the temporary directory back to its original location, undoing any partial changes.
    2.  If the main deletion logic **succeeded**, it simply deletes the temporary backup and lock files.
    3.  Finally, it releases all of its locks.

In essence, TableVault's write operations are **transactional**. Through a combination of **locking**, **temporary data backups**, and **detailed process logging**, they ensure that an operation either completes successfully, leaving the repository in a new consistent state, or it fails and the repository is returned to the state it was in before the operation began.

---