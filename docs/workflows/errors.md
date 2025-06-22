# Handling Execution Errors

This document explains the typical procedure when TableVault encounters execution errors. To understands processes in TableVault in general, please visit [Core Concepts: Operations](../core_concepts/operations.md).

---

## 1. External Errors Without a User-Provided `process_id` and Internal TableVault Errors

If an operation encounters an unexpected external error, it is **safely reverted**. The system state will be as if the operation never started.

Internal TableVault-generated errors, regardless of `process_id` status, will always cause the operation to revert. These errors typically indicate that some user input is invalid and needs to be fixed.

!!! note "Executing Instances"
    Partially executed dataframes are not reverted for debugging purposes. However, if you rerun the `execute_instance()` operation, the full dataframe is rebuilt. After an error, in this case, you can still directly edit the builder and Python module files as if the instance had not executed.

#### Example External Error Code

=== "1. Initial Code"

    ```python
    tablevault.execute_instance(table_name='openai_responses')
    ```

=== "2. Example Error"

    ```python
    openai.error.APIConnectionError: HTTPSConnectionPool(
        host='api.openai.com',
        port=443
    ): Max retries exceeded with url: /v1/chat/completions
    (Caused by NewConnectionError('Temporary failure in name resolution'))
    ```

=== "3. Rerun Code"

    ```python
    # re-execute the command
    tablevault.execute_instance(table_name='openai_responses')
    # restarts from the beginning

    ```

#### Example TableVault Error Code

=== "1. Initial Code"

    ```python
    tablevault.make_table(table_name='artifacts')
    ```

=== "2. Example Error"

    ```python
    tv_errors.TVArgumentError: Forbidden Table Name: artifacts
    ```

=== "3. Rerun Code"

    ```python
    tablevault.make_table(table_name='short_stories_store')

    ```

---

## 2. External Errors with `process_id` and System Interrupts

With a user-provided `process_id`, an error only **pauses** the operation. It maintains its system locks, preventing other operations from accessing the same resources. This is especially useful for long-running `execute_instance()` operations that might be stopped.

Note that the operation restarts from its last checkpoint, and its input arguments cannot be changed.

#### Example Code

=== "1. Initial Code"

    ```python
    # generate process_id
    process_id = tablevault.generate_process_id()
    # execute process
    tablevault.execute_instance(table_name='openai_responses', process_id=process_id)
    ```

=== "2. Example Error"

    ```python
    openai.error.APIConnectionError: HTTPSConnectionPool(
        host='api.openai.com',
        port=443
    ): Max retries exceeded with url: /v1/chat/completions
    (Caused by NewConnectionError('Temporary failure in name resolution'))
    ```

=== "3. Rerun Code"

    ```python
    # re-execute the command with the SAME process_id
    tablevault.execute_instance(table_name='openai_responses', process_id=process_id)
    # restarts from checkpoint
    ```

### Restart Process

You can restart the *exact same* operation by rerunning the function call with the same `process_id` or by setting `restart` to `True` in a new TableVault object. In the latter case, we assume a system crash, and all currently active processes are restarted.

#### Example Code

```python
tablevault = TableVault(db_dir='stories_tv', restart=True)
```

### Stop Process
To explicitly stop and revert an active process, you can call the `stop_process()` function. If you want to materialize the partially generated dataframe, you can set the `materialize` boolean parameter to `True`. The dataframe becomes an active materialized instance, but its artifacts remain instance-specific and don't overwrite pre-existing table artifacts.

The `get_active_processes()` function is useful for finding a list of all currently active processes.

#### Example Code

```python
# materialize only applies to `execute_instance()` operations
tablevault.stop_process(process_id=process_id, materialize=True)
```