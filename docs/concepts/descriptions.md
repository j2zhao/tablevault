# Descriptions

Each item list can store multiple descriptions with semantic metadata. Each description must include both text and an embedding vector. The embedding dimension must match the value configured when the TableVault repository was created.

## Creating Descriptions

```python
description = "Full novel of Frankenstein by Mary Shelley in paragraphs"
embedding = MODEL(description)

vault.create_description("frankenstein_novel", description, embedding)
```

A single item list can hold multiple descriptions, each identified by a unique `description_name` label (defaults to `"BASE"`):

```python
vault.create_description(
    "frankenstein_novel",
    description="Frankenstein paragraphs for semantic search",
    embedding=MODEL("Frankenstein paragraphs for semantic search"),
    description_name="SEMANTIC"
)
```

When querying item types, you can filter by token search over description text or vector similarity over description embeddings.

## Retrieving Descriptions for an Item

Get all descriptions attached to a specific item list with `query_item_description`:

```python
descriptions = vault.query_item_description("frankenstein_novel")
# Returns: [["BASE", "Full novel of Frankenstein..."], ["SEMANTIC", "Frankenstein paragraphs..."]]
# Each element is [description_name, description_text]
```

## Searching Descriptions Across All Item Lists

TableVault can also search descriptions globally across all item lists and types, without targeting a specific list.

### Token Search

Find item lists whose descriptions contain all specified tokens:

```python
results = vault.query_description("machine learning research")
# Returns: [["BASE", "Collection of ML research papers", "research_papers", "document_list"], ...]
# Each element is [description_name, description_text, list_name, list_type]
```

Use the `k` parameter to control the maximum number of results (default 500), and `text_analyzer` to select the ArangoSearch tokenizer (default `"text_en"`).

### Embedding Similarity Search

Find item lists whose descriptions are semantically closest to a query embedding:

```python
query_embedding = MODEL("neural network papers")

results = vault.query_description_embedding(query_embedding, k=10)
# Returns top-10 matches sorted by descending cosine similarity
# Each element is [description_name, description_text, list_name, list_type]

# Use approximate search for faster results on large vaults
results = vault.query_description_embedding(query_embedding, k=10, use_approx=True)
```

## AI Generated Descriptions

Automatic description and embedding generation may be added in a future release. If this is useful for your workflow, please feel free to reach out!
