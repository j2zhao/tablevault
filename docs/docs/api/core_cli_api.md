# TableVault Command Line Interface

A convenient command-line interface for *TableVault* operations.

Most sub-commands require a *TableVault* instance. You can provide the `--db-dir` and `--author` global options once, and they will be reused by every command invoked.

**Global Options:**

These options can be used before any subcommand.

| Option      | Type   | Description                                               |
| ----------- | ------ | --------------------------------------------------------- |
| `--db-dir`  | `PATH` | Path to the TableVault directory (required by most commands). |
| `--author`  | `TEXT` | Author name used for audit logging.                       |
| `-h, --help`|        | Show the help message and exit.                           |

**Example:**

```bash
tablevault --db-dir ./my_vault --author jinjin get-active-processes
```

---

## Data Creation Commands

These commands require the global `--db-dir` and `--author` options to be set.

---

### `create-table`

```bash
tablevault create-table <TABLE_NAME> [OPTIONS]
```

Create a new table definition in the vault.

**Argument:**

| Argument     | Type   | Description           |
| ------------ | ------ | --------------------- |
| `TABLE_NAME` | `TEXT` | Name of the new table. |

**Options:**

| Option                   |        | Description                                                                                           | Default                 |
| ------------------------ | ------ | ----------------------------------------------------------------------------------------------------- | ----------------------- |
| `--multiple-artifacts`   |        | If set, each materialised instance gets its own artifact folder.                                      | Flag (False by default) |
| `--side-effects`         |        | If set, builder files are assumed to have side effects (e.g., external API calls).                  | Flag (False by default) |
| `-h, --help`             |        | Show this message and exit.                                                                           |                         |

**Output:**
Prints the process ID of the executed operation.

---

### `create-instance`

```bash
tablevault create-instance <TABLE_NAME> [OPTIONS]
```

Create a new temporary instance of a table.

**Argument:**

| Argument     | Type   | Description        |
| ------------ | ------ | ------------------ |
| `TABLE_NAME` | `TEXT` | Name of the table. |

**Options:**

| Option            | Type   | Description                                                                         | Default                 |
| ----------------- | ------ | ----------------------------------------------------------------------------------- | ----------------------- |
| `--version`       | `TEXT` | Table version.                                                                      | `""`                    |
| `--origin-id`     | `TEXT` | If supplied, copy state from this existing instance ID.                             | `""`                    |
| `--origin-table`  | `TEXT` | Table associated with `origin-id`. Defaults to `table_name` if empty.               | `""`                    |
| `--external-edit` |        | If set, this instance will be edited externally (no builder files constructed).     | Flag (False by default) |
| `--no-copy`       |        | If set, do not copy from the latest materialised instance (if `origin-id` not provided). | Flag (False by default) |
| `--builder`       | `TEXT` | Add builder names. Can be repeated for multiple builders.                           | None (multiple)         |
| `-h, --help`      |        | Show this message and exit.                                                         |                         |

**Output:**
Prints the process ID of the executed operation.

---

### `create-code-module`

```bash
tablevault create-code-module [OPTIONS]
```

Copy (or create) a code-module file or directory into the vault.

**Options:**

| Option          | Type   | Description                                                                 | Default |
| --------------- | ------ | --------------------------------------------------------------------------- | ------- |
| `--module-name` | `TEXT` | Name for the new module. If empty, inferred from `copy-dir`.                 | `""`    |
| `--copy-dir`    | `PATH` | Local directory or Python file to copy. If empty, a new file is created.    | `""`    |
| `-h, --help`    |        | Show this message and exit.                                                 |         |

**Output:**
Prints the process ID of the executed operation.

---

### `create-builder-file`

```bash
tablevault create-builder-file <TABLE_NAME> [OPTIONS]
```

Add or update a builder (YAML) file for a temporary table instance.

**Argument:**

| Argument     | Type   | Description        |
| ------------ | ------ | ------------------ |
| `TABLE_NAME` | `TEXT` | Name of the table. |

**Options:**

| Option           | Type   | Description                                                                 | Default                        |
| ---------------- | ------ | --------------------------------------------------------------------------- | ------------------------------ |
| `--builder-name` | `TEXT` | File name of the builder. If empty, inferred from `copy-dir`.                 | `""`                           |
| `--version`      | `TEXT` | Version of the table.                                                       | `constants.BASE_TABLE_VERSION` |
| `--copy-dir`     | `PATH` | Local directory containing the builder file(s) to copy.                     | `""`                           |
| `-h, --help`     |        | Show this message and exit.                                                 |                                |

**Output:**
Prints the process ID of the executed operation.

---


## Instance Materialization Commands


### `execute-instance`

```bash
tablevault execute-instance <TABLE_NAME> [OPTIONS]
```

Materialise an existing temporary table instance.

**Argument:**

| Argument     | Type   | Description                        |
| ------------ | ------ | ---------------------------------- |
| `TABLE_NAME` | `TEXT` | Name of the table to materialise.  |

**Options:**

| Option         | Type   | Description                                                                 | Default                        |
| -------------- | ------ | --------------------------------------------------------------------------- | ------------------------------ |
| `--version`    | `TEXT` | Version of the table.                                                       | `constants.BASE_TABLE_VERSION` |
| `--force`      |        | Force a full rebuild; otherwise, attempts to reuse an origin instance.      | Flag (False by default)        |
| `--background` |        | Run materialisation in a background process.                                | Flag (False by default)        |
| `-h, --help`   |        | Show this message and exit.                                                 |                                |

**Output:**
Prints the process ID of the executed operation.

---


### `write-instance`

```bash
tablevault write-instance <TABLE_NAME> --csv <CSV_PATH> [OPTIONS]
```

Write data from a CSV file as a materialized instance of a table. The table must already have a temporary instance of the same version open for external edits.

**Argument:**

| Argument     | Type   | Description   |
| ------------ | ------ | ------------- |
| `TABLE_NAME` | `TEXT` | Target table. |

**Options:**

| Option      | Type   | Description                                   | Default                        |
| ----------- | ------ | --------------------------------------------- | ------------------------------ |
| `--csv`     | `PATH` | **Required.** CSV file containing the dataframe (must exist). | –                              |
| `--version` | `TEXT` | Target version.                               | `constants.BASE_TABLE_VERSION` |
| `-h, --help`|        | Show this message and exit.                   |                                |

**Output:**
Prints the process ID of the executed write operation.

---


## Data Deletion/Modification Commands


These commands require the global `--db-dir` and `--author` options to be set.

---

### `rename-table`

```bash
tablevault rename-table <OLD_NAME> <NEW_NAME> [OPTIONS]
```

Rename an existing table within the vault.

**Arguments:**

| Argument   | Type   | Description                 |
| ---------- | ------ | --------------------------- |
| `OLD_NAME` | `TEXT` | Current name of the table.  |
| `NEW_NAME` | `TEXT` | New name for the table.     |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints the process ID of the executed operation.

---

### `delete-table`

```bash
tablevault delete-table <TABLE_NAME> [OPTIONS]
```

Permanently delete a table and all its instances (dataframes) from the vault. Metadata is retained.

**Argument:**

| Argument     | Type   | Description                  |
| ------------ | ------ | ---------------------------- |
| `TABLE_NAME` | `TEXT` | Name of the table to delete. |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints the process ID of the executed operation.

---

### `delete-instance`

```bash
tablevault delete-instance <TABLE_NAME> <INSTANCE_ID> [OPTIONS]
```

Delete a materialised table instance (dataframe) from the vault. Instance metadata is retained.

**Arguments:**

| Argument      | Type   | Description                       |
| ------------- | ------ | --------------------------------- |
| `TABLE_NAME`  | `TEXT` | Name of the table owns the instance. |
| `INSTANCE_ID` | `TEXT` | ID of the instance to delete.     |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints the process ID of the executed operation.

---

### `delete-code-module`

```bash
tablevault delete-code-module <MODULE_NAME> [OPTIONS]
```

Delete a code-module file (`{MODULE_NAME}.py`) from the vault.

**Argument:**

| Argument      | Type   | Description                   |
| ------------- | ------ | ----------------------------- |
| `MODULE_NAME` | `TEXT` | Name of the module to delete. |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints the process ID of the executed operation.

---

### `delete-builder-file`

```bash
tablevault delete-builder-file <TABLE_NAME> <BUILDER_NAME> [OPTIONS]
```

Remove a builder file from a temporary table instance.

**Arguments:**

| Argument       | Type   | Description                         |
| -------------- | ------ | ----------------------------------- |
| `TABLE_NAME`   | `TEXT` | Name of the table owns the builder. |
| `BUILDER_NAME` | `TEXT` | Name of the builder file to delete. |

**Options:**

| Option      | Type   | Description           | Default                        |
| ----------- | ------ | --------------------- | ------------------------------ |
| `--version` | `TEXT` | Version of the table. | `constants.BASE_TABLE_VERSION` |
| `-h, --help`|        | Show this message and exit. |                                |

**Output:**
Prints the process ID of the executed operation.

---


## Process Commands


These commands require the global `--db-dir` and `--author` options to be set.

---

### `generate-process-id`

```bash
tablevault generate-process-id [OPTIONS]
```

Generate and return a unique process ID. This ID can be used in other operations that accept a `process_id` for persistence and restart capabilities.

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints a new, unique process identifier.

---

### `stop-process`

```bash
tablevault stop-process <PROCESS_ID> [OPTIONS]
```

Stop an active process.

**Argument:**

| Argument     | Type   | Description                |
| ------------ | ------ | -------------------------- |
| `PROCESS_ID` | `TEXT` | ID of the process to stop. |

**Options:**

| Option          |        | Description                                             | Default                 |
| --------------- | ------ | ------------------------------------------------------- | ----------------------- |
| `--force`       |        | Force-kill the running process.                         | Flag (False by default) |
| `--materialize` |        | Materialise partial instances if possible.              | Flag (False by default) |
| `-h, --help`    |        | Show this message and exit.                             |                         |

**Output:**
Prints the process ID of the *stop\_process* operation.

---

## Data Fetching Commands


These commands require the global `--db-dir` and `--author` options to be set.

---


### `get-dataframe`

```bash
tablevault get-dataframe <TABLE_NAME> --output <OUTPUT_CSV> [OPTIONS]
```

Fetch a table instance and write its contents to a CSV file.

**Argument:**

| Argument     | Type   | Description        |
| ------------ | ------ | ------------------ |
| `TABLE_NAME` | `TEXT` | Name of the table. |

**Options:**

| Option                 | Type   | Description                                                            | Default                        |
| ---------------------- | ------ | ---------------------------------------------------------------------- | ------------------------------ |
| `--output`             | `PATH` | **Required.** Destination CSV file path.                               | –                              |
| `--instance-id`        | `TEXT` | Specific instance ID. If empty, latest of `version` is used.         | `""`                           |
| `--version`            | `TEXT` | Table version (used if `instance-id` is omitted).                      | `constants.BASE_TABLE_VERSION` |
| `--rows`               | `INT`  | Limit the number of rows fetched (None = no limit).                    | `None`                         |
| `--include-inactive`   |        | If set, consider inactive instances as well.                           | Flag (False by default)        |
| `--no-artifact-path`   |        | If set, skip prepending artifact folder path to "artifact\_string" columns. | Flag (False by default)        |
| `-h, --help`           |        | Show this message and exit.                                            |                                |

**Output:**
Writes the DataFrame to the specified `OUTPUT_CSV` file and prints a JSON object containing the `instance_id`, number of `rows`, and the `csv` path.

---


### `get-instances`

```bash
tablevault get-instances <TABLE_NAME> [OPTIONS]
```

Return a list of materialised instance IDs for a specific table and version.

**Argument:**

| Argument     | Type   | Description        |
| ------------ | ------ | ------------------ |
| `TABLE_NAME` | `TEXT` | Name of the table. |

**Options:**

| Option      | Type   | Description           | Default                        |
| ----------- | ------ | --------------------- | ------------------------------ |
| `--version` | `TEXT` | Version of the table. | `constants.BASE_TABLE_VERSION` |
| `-h, --help`|        | Show this message and exit. |                                |

**Output:**
Prints a JSON list of materialised instance IDs.

---

### `get-active-processes`

```bash
tablevault get-active-processes [OPTIONS]
```

List all currently active processes in the vault.

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints a JSON object where keys are process IDs and values are metadata about each process.

---


### `get-artifact-folder`

```bash
tablevault get-artifact-folder <TABLE_NAME> [OPTIONS]
```

Return the path to the artifact folder for a given table instance.

**Argument:**

| Argument     | Type   | Description        |
| ------------ | ------ | ------------------ |
| `TABLE_NAME` | `TEXT` | Name of the table. |

**Options:**

| Option                  | Type   | Description                                                       | Default                        |
| ----------------------- | ------ | ----------------------------------------------------------------- | ------------------------------ |
| `--instance-id`         | `TEXT` | Specific instance ID (optional).                                  | `""`                           |
| `--version`             | `TEXT` | Table version.                                                    | `constants.BASE_TABLE_VERSION` |
| `--temp` / `--materialised` |        | Return the temporary (`--temp`) or last materialised (`--materialised`) folder. | `--temp`                       |
| `-h, --help`            |        | Show this message and exit.                                       |                                |

**Output:**
Prints the path to the requested artifact folder.

---


### `get-process-completion`

```bash
tablevault get-process-completion <PROCESS_ID> [OPTIONS]
```

Return **True** if the specified `PROCESS_ID` has finished, **False** otherwise.

**Argument:**

| Argument     | Type   | Description                |
| ------------ | ------ | -------------------------- |
| `PROCESS_ID` | `TEXT` | Identifier of the process. |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints `true` or `false`.

---


## Vault-Independent Utilities

These commands operate on vault directories but do not require an initialized `TableVault` instance in the same way as other commands (i.e., they don't use the global `--author` for their direct operation but might be used in conjunction with a vault managed by an author).

---

### `compress`

```bash
tablevault compress <DB_DIR> [OPTIONS]
```

Create a `DB_DIR.tar.xz` archive from the specified `DB_DIR`.

**Argument:**

| Argument | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `DB_DIR` | `PATH` | Path to the TableVault directory to compress (must exist). |

**Options:**

| Option   | Type  | Description                        | Default |
| -------- | ----- | ---------------------------------- | ------- |
| `--preset` | `INT` | LZMA compression level (1-9).      | `6`     |
| `-h, --help`|       | Show this message and exit.        |         |

**Output:**
Prints a confirmation message with the name of the created archive, e.g., "Compressed → my\_vault.tar.xz".

---

### `decompress`

```bash
tablevault decompress <ARCHIVE> [OPTIONS]
```

Extract a `*.tar.xz` archive (created by `tablevault compress`) into a directory named after the archive (without the `.tar.xz` extension).

**Argument:**

| Argument  | Type   | Description                                     |
| --------- | ------ | ----------------------------------------------- |
| `ARCHIVE` | `PATH` | Path to the `*.tar.xz` archive file (must exist). |

**Options:**

| Option      |        | Description                 |
| ----------- | ------ | --------------------------- |
| `-h, --help`|        | Show this message and exit. |

**Output:**
Prints a confirmation message with the name of the directory where files were decompressed, e.g., "Decompressed → my\_vault/".

---

### `delete-vault`

```bash
tablevault delete-vault <DB_DIR> [OPTIONS]
```

**Irreversibly** delete an entire TableVault directory. This command will ask for confirmation before proceeding.

**Argument:**

| Argument | Type   | Description                                            |
| -------- | ------ | ------------------------------------------------------ |
| `DB_DIR` | `PATH` | Path to the TableVault directory to delete (must exist). |

**Options:**

| Option                       |        | Description                                       |
| ---------------------------- | ------ | ------------------------------------------------- |
| `--yes`                      |        | Skip the confirmation prompt.                     |
| `-h, --help`                 |        | Show this message and exit.                       |

**Output:**
Prints "Vault deleted ✔" upon successful deletion.

---

