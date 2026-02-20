# Basic Example

This tutorial demonstrates the core concepts of TableVault through a practical example: building a document processing pipeline with searchable embeddings.

## Prerequisites

Before starting, ensure you have:

1. ArangoDB running (see [Repository Setup](../concepts/setup.md))
2. TableVault installed: `pip install tablevault`
3. An embedding model (we'll use a simple mock for demonstration)

## Scenario

We'll build a pipeline that:

1. Stores document chunks from a text file
2. Generates embeddings for each chunk
3. Tracks lineage between documents and embeddings
4. Queries documents by similarity and text content

## Step 1: Initialize the Vault

```python
from tablevault import Vault

# Create a new TableVault process
vault = Vault(
    user_id="tutorial_user",
    process_name="document_pipeline",
    arango_url="http://localhost:8529",
    arango_db="tutorial_db",
    new_arango_db=True,  # Start fresh
    arango_root_password="passwd"
)
```

## Step 2: Create Item Lists

```python
# Create a document list for storing text chunks
vault.create_document_list("research_papers")

# Create an embedding list (using 384-dim for this example)
vault.create_embedding_list("paper_embeddings", ndim=384)

# Create a record list for metadata
vault.create_record_list("paper_metadata", column_names=["title", "author", "chunk_id"])
```

## Step 3: Add Documents and Track Lineage

```python
# Sample document chunks
documents = [
    "Machine learning is a subset of artificial intelligence.",
    "Neural networks are inspired by biological neurons.",
    "Deep learning has revolutionized computer vision.",
    "Transformers have changed natural language processing.",
]

# Mock embedding function (replace with your actual model)
def get_embedding(text):
    import hashlib
    # Simple mock: hash text to create reproducible "embedding"
    h = hashlib.sha384(text.encode()).digest()
    return [float(b) / 255.0 for b in h]

# Add documents and their embeddings
for idx, doc in enumerate(documents):
    # Add document
    vault.append_document("research_papers", doc)

    # Generate and add embedding with lineage tracking
    embedding = get_embedding(doc)
    vault.append_embedding(
        "paper_embeddings",
        embedding,
        input_items={"research_papers": [idx, idx + 1]}  # Links to source document
    )

    # Add metadata
    vault.append_record("paper_metadata", {
        "title": f"Paper Section {idx + 1}",
        "author": "Tutorial Author",
        "chunk_id": idx
    })
```

## Step 4: Add Descriptions

```python
# Add semantic descriptions for queryability
vault.create_description(
    "research_papers",
    description="Collection of machine learning research paper excerpts",
    embedding=get_embedding("machine learning research papers")
)

vault.create_description(
    "paper_embeddings",
    description="Vector embeddings of research paper text chunks",
    embedding=get_embedding("document embeddings vectors")
)
```

## Step 5: Query Content

```python
# Get all documents
all_docs = vault.query_item_content("research_papers")
print("All documents:", all_docs)

# Get specific document by index
first_doc = vault.query_item_content("research_papers", index=0)
print("First document:", first_doc)

# Get item metadata
metadata = vault.query_item_list("research_papers")
print("Document list info:", metadata)
```

## Step 6: Query Lineage

```python
# Find what the embeddings were derived from
parents = vault.query_item_parent("paper_embeddings")
print("Embedding parents:", parents)

# Find what was derived from the documents
children = vault.query_item_child("research_papers")
print("Document children:", children)

# Get specific range lineage
first_embedding_source = vault.query_item_parent(
    "paper_embeddings",
    start_position=0,
    end_position=1
)
print("First embedding came from:", first_embedding_source)
```

## Step 7: Similarity Search

```python
# Search by embedding similarity
query_text = "artificial intelligence and deep learning"
query_embedding = get_embedding(query_text)

# Find similar embeddings
similar = vault.query_embedding_list(
    embedding=query_embedding,
    use_approx=False  # Use exact search for small datasets
)
print("Similar embeddings:", similar)

# Search documents by text
results = vault.query_document_list(
    document_text="neural networks"
)
print("Documents matching 'neural networks':", results)
```

## Step 8: Process Queries

```python
# Find which process created these items
creation_process = vault.query_item_creation_process("research_papers")
print("Created by process:", creation_process)

# Find all items created in this process
process_items = vault.query_process_item("document_pipeline")
print("Items in process:", process_items)
```

## Step 9: Safe Checkpointing (for Long Workflows)

```python
# In a long-running workflow, add checkpoints
for i in range(100):
    # Process data
    result = process_iteration(i)
    vault.append_record("iteration_results", result)

    # Allow safe stop/pause at this point
    vault.checkpoint_execution()
```

## Full Script

Here's the complete script:

```python
from tablevault import Vault
import hashlib

def get_embedding(text):
    h = hashlib.sha384(text.encode()).digest()
    return [float(b) / 255.0 for b in h]

# Initialize
vault = Vault(
    user_id="tutorial_user",
    process_name="document_pipeline",
    new_arango_db=True,
    arango_root_password="passwd"
)

# Create lists
vault.create_document_list("research_papers")
vault.create_embedding_list("paper_embeddings", ndim=384)

# Add data with lineage
documents = [
    "Machine learning is a subset of artificial intelligence.",
    "Neural networks are inspired by biological neurons.",
    "Deep learning has revolutionized computer vision.",
    "Transformers have changed natural language processing.",
]

for idx, doc in enumerate(documents):
    vault.append_document("research_papers", doc)
    vault.append_embedding(
        "paper_embeddings",
        get_embedding(doc),
        input_items={"research_papers": [idx, idx + 1]}
    )

# Add descriptions
vault.create_description(
    "research_papers",
    "ML research paper excerpts",
    get_embedding("machine learning research")
)

# Query
print("Documents:", vault.query_item_content("research_papers"))
print("Lineage:", vault.query_item_parent("paper_embeddings"))
print("Similar:", vault.query_embedding_list(get_embedding("AI deep learning")))
```