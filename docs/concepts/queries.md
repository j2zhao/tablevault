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

## Lineage Tracking

TableVault automatically tracks relationships between items. When you specify `input_items` during append operations, these dependencies are stored and can be queried later.

### Querying Parent Items (Inputs)

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

### Querying Child Items (Outputs)

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

### Example: Tracing Data Flow

```python
# Scenario: Understanding how a model was trained

# 1. Find what data the model file depends on
model_inputs = vault.query_item_parent("trained_models")
# Might return: ["training_features", "training_labels"]

# 2. Trace further back - what created the training features?
feature_inputs = vault.query_item_parent("training_features")
# Might return: ["raw_data", "document_embeddings"]

# 3. Find everything derived from the raw data
derived_items = vault.query_item_child("raw_data")
# Returns all downstream items
```

## Session Relationships

Every operation in TableVault is tied to a session. You can query the relationship between items and sessions to understand provenance.

### Finding the Creation Session

Get the session that originally created an item list:

```python
# Find which session created this item
creation_info = vault.query_item_creation_session("experiment_results")
# Returns: [{"session_id": "training_run_01", "index": 3}]
```

### Finding All Modifying Sessions

Get all sessions that have modified an item list, optionally filtered by position:

```python
# Get all sessions that modified this item
sessions = vault.query_item_session("document_chunks")

# Get sessions that modified only a specific range
sessions = vault.query_item_session(
    "document_chunks",
    start_position=0,
    end_position=500
)
# Useful when different sessions appended different portions
```

### Finding Items from a Session

Get all items that a session created or modified:

```python
# Get all items touched by a session
items = vault.query_session_item("data_pipeline_session")
# Returns: [{"name": "raw_data", "start": 0, "end": 1000}, ...]
```

### Example: Audit Trail

```python
# Scenario: Auditing what a specific session did

# 1. Find all items this session touched
session_items = vault.query_session_item("nightly_batch_run")

# 2. For each item, see what range was affected
for item in session_items:
    print(f"Item: {item['name']}, Range: {item['start']}-{item['end']}")

# 3. Check if any other sessions also modified these items
for item in session_items:
    all_sessions = vault.query_item_session(item['name'])
    print(f"{item['name']} modified by: {[s['session_id'] for s in all_sessions]}")
```

## Range-Based Filtering

Many query functions support `start_position` and `end_position` parameters to filter results to specific ranges within a list. This is useful when:

- Different sessions appended different portions of data
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

# Find which sessions modified positions 500-1000
sessions = vault.query_item_session(
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

## Item Descriptions

Retrieve text descriptions associated with an item list:

```python
# Get all descriptions for an item
descriptions = vault.query_item_description("trained_models")
# Returns: ["Random forest classifier for sentiment analysis", ...]
```