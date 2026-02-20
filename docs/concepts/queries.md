# Basic Queries

TableVault provides a set of query functions for exploring individual item lists and their relationships. These queries enable you to trace data lineage, understand how items were created, and retrieve specific portions of data.

## Querying Item Content

Retrieve the actual data stored in an item list using `query_item_content`. You can fetch by index (chunk number) or by position range.

```python
# Get content at a specific index
content = vault.query_item_content("document_chunks", index=0)

# Get content within a position range
content = vault.query_item_content("document_chunks", start_position=0, end_position=500)

# Get all content (no filters)
all_content = vault.query_item_content("document_chunks")
```

### Item Metadata

Get metadata about an item list using `query_item_list`:

```python
# Get metadata for an item list
metadata = vault.query_item_list("experiment_results")
# Returns: {"n_items": 50, "length": 1000, ...}
```

## Python Lineage Tracking

Every operation in TableVault is automatically tied to a Python process. You can query the relationship between items and processes.

### Finding the Creation Process

Get the process that originally created an item list:

```python
# Find which process created this item
creation_info = vault.query_item_creation_process("experiment_results")
# Returns: [{"process_id": "training_run_01", "index": 3}]
```

### Finding All Modifying Processes

Get all processes that have modified an item list, optionally filtered by position:

```python
# Get all processes that modified this item
processes = vault.query_item_process("document_chunks")

# Get processes that modified only a specific range
processes = vault.query_item_process(
    "document_chunks",
    start_position=0,
    end_position=500
)
# Useful when different processes appended different portions
```

### Finding Items from a Python Process

Get all items that a process created or modified:

```python
# Get all items touched by a process
items = vault.query_process_item("data_pipeline_process")
# Returns: [{"name": "raw_data", "start": 0, "end": 1000}, ...]
```

## Data Lineage Tracking

When you specify `input_items` during append operations, these dependencies are also stored and can be queried later.

### Querying Dependencies

Find what items a given item depends on using `query_item_parent`:

```python
# Get all input dependencies for an embedding list
parents = vault.query_item_parent("document_embeddings")
# Returns items that "document_embeddings" was derived from

# Filter by position range within the item
# Find inputs for only positions 0-100 of the embedding list
parents = vault.query_item_parent(
    "document_embeddings",
    start_position=0,
    end_position=100
)
```

### Querying Child Items

Find what items were derived from a given item using `query_item_child`:

```python
# Get all items that depend on this document list
children = vault.query_item_child("raw_documents")
# Returns items like embeddings, summaries, etc. derived from raw_documents

# Filter by position range
# Find items derived from only positions 50-150 of the source
children = vault.query_item_child(
    "raw_documents",
    start_position=50,
    end_position=150
)
```

## Item List Descriptions

You can also retrieve descriptions associated with an item list (which are items themselves):

```python
# Get all descriptions for an item
descriptions = vault.query_item_description("trained_models")
# Returns: ["Random forest classifier for sentiment analysis", ...]
```


## Range-Based Filtering

Many query functions support `start_position` and `end_position` parameters to filter results to specific ranges within a list. This is useful when:

- Different processes appended different portions of data
- You want to trace lineage for only part of an item
- You need to understand which code created specific chunks

```python
# Get content for positions 100-200
content = vault.query_item_content(
    "document_chunks",
    start_position=100,
    end_position=200
)

# Find parents for only the first 50 items
parents = vault.query_item_parent(
    "embeddings",
    start_position=0,
    end_position=50
)

# Find which processes modified positions 500-1000
processes = vault.query_item_process(
    "training_data",
    start_position=500,
    end_position=1000
)

# Find children derived from positions 0-100
children = vault.query_item_child(
    "source_documents",
    start_position=0,
    end_position=100
)
```

