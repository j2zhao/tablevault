# Repository Structure

The TableVault repository stores and organizes all data and code transparently within your local file system. This design allows you to view files directly and simply transfer in-progress workflows across devices.

---

## TableVault Repository


The following diagram illustrates the internal storage structure of a TableVault repository:

![Internal TableVault Structure](../assets/storage_files.png){width=60%}
/// caption
**TableVault File Structure**
///

!!! note "Direct File Edits"
    While the files are transparently exposed, directly editing files is generally discouraged - with two exceptions: Builder (YAML) and Python code files may be modified using an external editor. To help prevent accidental modifications on Unix systems, TableVault sets certain files to read-only. While this does not stop intentional changes, it serves as a safeguard against common mistakes.


---

## TableVault Instance

A TableVault instance represents a dataframe with its associated metadata and optional artifacts. All files relevant to an instance are stored in a dedicated instance folder. There are two types of instances:

  * **Temporary Instance**: An instance that has not yet been executed and cannot be queried. It is created with `create_instance()` and contains metadata, a description, and builder files. The `external_edit` parameter specifies whether the instance will be built internally by TableVault (`False`) or populated by an external process (`True`).
  * **Materialized Instance**: An instance that has been executed and is indexed by TableVault. Its dataframe is read-only. If the instance is active, its data and metadata can be fetched via the API. The code functions and builder files used at execution are recorded. Materialized instances are versioned by a timestamp and an optional `version` string.

A free-form description can be added via the `create_instance()` function.

---

## Table Folder

In TableVault, a "table" is a semantic collection of instances that can be referenced by the same name. All instances of a table are stored together in the same folder. In many API functions, the latest instance of a table can be retrieved using only the `table_name` reference.

Instances under the same table can share additional properties set during table initialization:

  * If `allow_multiple_artifacts` is set to `False`, only the latest instance can be queried, and only its artifacts are stored.
  * If `has_side_effects` is set to `True`, all other instances of the table become un-queryable the moment one instance begins execution. This is useful if an operation invalidates previous versions.

Examples of tables include a collection of instances testing different model prompts, a repository of scientific papers, or evolving versions of an embedding model. A free-form description can be added via the `create_table()` function.

-----

## Metadata

A TableVault repository stores various metadata files. A detailed list of these files and their functions can be found in [Advanced](../advanced/metadata.md).