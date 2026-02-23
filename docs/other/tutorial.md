# Basic Example

This tutorial demonstrates the core concepts of TableVault through a practical example: building a document processing pipeline with searchable embeddings.

## Step 1: Install Dependencies

First, install TableVault.

```python
!pip install tablevault
```

## Step 2: Setup ArangoDB

Run ArangoDB locally using Docker with vector index support enabled.

```python
import subprocess
subprocess.run([
    "docker", "run", "-d",
    "--name", "arangodb",
    "-e", "ARANGO_ROOT_PASSWORD=rootpassword",
    "-p", "8529:8529",
    "arangodb:3.12",
    "arangod", "--experimental-vector-index=true"
], check=True)
```

Once ArangoDB is running, you can explore your database using the built-in web UI at [http://localhost:8529](http://localhost:8529). Log in with username `root` and password `rootpassword` to browse collections, run queries, and inspect your data like a typical database.

If the command fails with a port binding error, port 8529 is already in use. Find and stop the conflicting process before continuing:

```bash
lsof -i :8529          # find what is using the port
docker stop <name>     # if it is a Docker container
```

Verify ArangoDB is running:

```python
from arango import ArangoClient
from arango.exceptions import ArangoError

client = ArangoClient(hosts="http://localhost:8529")

try:
    sys_db = client.db("_system", username="root", password="rootpassword")
    info = sys_db.version()
    version = info.get("version") if isinstance(info, dict) else info
    print(f"ArangoDB is ready: {version}")
except ArangoError as exc:
    raise RuntimeError("ArangoDB started, but auth failed. Check root password setup.") from exc
```

## Step 3: Initialize the Vault

Create a TableVault instance connected to your ArangoDB. The `process_name` identifies this run: all data written through this vault is attributed to the `document_pipeline` process, making it easy to trace where each item came from later.

```python
from tablevault import Vault

# Create a new TableVault process
vault = Vault(
    user_id="tutorial_user",
    process_name="document_pipeline",
    arango_url="http://localhost:8529",
    arango_db="tutorial_db",
    new_arango_db=True,  # Start fresh
    arango_root_password="rootpassword"
)

print("Vault initialized successfully!")
```

## Step 4: Create Item Lists

TableVault organizes data into typed lists:

- **Document lists**: Store text content
- **Embedding lists**: Store vector embeddings
- **Record lists**: Store structured metadata

Each list stores items at sequential integer positions. Items across lists can be linked by position range to track lineage. For example, recording that embedding position 2 was derived from document positions 2–3.

```python
# Create a document list for storing text chunks
vault.create_document_list("research_papers")

# Create an embedding list (using 384-dim for this example)
EMBEDDING_DIM = 384
vault.create_embedding_list("paper_embeddings", ndim=EMBEDDING_DIM)

# Create a record list for metadata
vault.create_record_list("paper_metadata", column_names=["title", "author", "chunk_id"])

print("Item lists created!")
```

## Step 5: Add Documents and Track Lineage

We'll add sample documents and their embeddings, tracking the lineage between them. The `input_items` argument on `append_embedding` records which source positions the embedding was derived from, forming an explicit link that can be queried later.

```python
# Sample document chunks
documents = [
    "Machine learning is a subset of artificial intelligence.",
    "Neural networks are inspired by biological neurons.",
    "Deep learning has revolutionized computer vision.",
    "Transformers have changed natural language processing.",
]

# Mock embedding function (replace with your actual model like sentence-transformers)
def get_embedding(text):
    import hashlib
    import random

    seed = int.from_bytes(hashlib.sha256(text.encode()).digest(), "big")
    rng = random.Random(seed)
    return [rng.random() for _ in range(EMBEDDING_DIM)]

print(f"Mock embedding dimension: {len(get_embedding('test'))}")
```

```python
# Add documents and their embeddings with lineage tracking
for idx, doc in enumerate(documents):
    # Add document
    vault.append_document("research_papers", doc)

    # Generate and add embedding with lineage tracking
    embedding = get_embedding(doc)
    vault.append_embedding(
        "paper_embeddings",
        embedding,
        input_items={"research_papers": [idx, idx + 1]},  # Links to source document
        index_rebuild_count=max(0, len(documents) - 1),  # Force index build for small demo sets
    )

    # Add metadata
    vault.append_record("paper_metadata", {
        "title": f"Paper Section {idx + 1}",
        "author": "Tutorial Author",
        "chunk_id": idx
    })

    print(f"Added document {idx + 1}: {doc[:50]}...")

    # All writes for this item are complete — safe to stop or pause the process here
    vault.checkpoint_execution()

has_index = vault.has_vector_index(EMBEDDING_DIM)
print(f"\nVector index created: {has_index}")
if not has_index:
    print("Vector index was not created; approximate search may be unavailable on this ArangoDB setup.")
print("All documents added with lineage tracking!")
```

### Why `checkpoint_execution` matters

`vault.checkpoint_execution()` marks a safe boundary at the end of each loop iteration.

- Stop or pause requests only take effect at a checkpoint, never mid-write or during a pending API call.
- Resume also happens at a checkpoint, which keeps pipeline state consistent.

## Step 6: Add Descriptions

Each item list can have a description: a short text and optional embedding that annotates what the list contains. Descriptions serve two purposes: they make lists self-documenting, and they act as a semantic filter when querying. In Step 9 you will see how `description_text` narrows a search to only the lists relevant to your query.

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

print("Descriptions added!")
```

## Step 7: Query Content

Now let's query the stored content.

```python
# Get all documents
all_docs = vault.query_item_content("research_papers")
print("All documents:")
for i, doc in enumerate(all_docs):
    print(f"  [{i}]: {doc}")
```

```python
# Get specific document by index
first_doc = vault.query_item_content("research_papers", index=0)
print(f"First document: {first_doc}")
```

```python
# Get item metadata
metadata = vault.query_item_list("research_papers")
print(f"Document list info: {metadata}")
```

## Step 8: Query Lineage

Lineage lets you trace exactly which source data produced each derived item. This is useful for debugging data quality issues, reproducing results, and auditing how your pipeline transformed data over time. You can traverse in either direction: from a derived item back to its sources, or from a source forward to everything derived from it.

```python
# Find what the embeddings were derived from
parents = vault.query_item_parent("paper_embeddings")
print(f"Embedding parents: {parents}")
```

```python
# Find what was derived from the documents
children = vault.query_item_child("research_papers")
print(f"Document children: {children}")
```

```python
# Get specific range lineage
first_embedding_source = vault.query_item_parent(
    "paper_embeddings",
    start_position=0,
    end_position=1
)
print(f"First embedding came from: {first_embedding_source}")
```

## Step 9: Similarity Search

TableVault supports both vector similarity search over embeddings and full-text search over documents. Both query types accept optional filters to narrow the search scope. `description_text` restricts results to lists whose description matches, and `code_text` restricts to lists created by processes whose source code contains the given string.

```python
# Search by embedding similarity
query_text = "artificial intelligence and deep learning"
query_embedding = get_embedding(query_text)

# Find similar embeddings
similar = vault.query_embedding_list(
    embedding=query_embedding,
    use_approx=False,
)
print(f"Similar embeddings: {similar}")
```

```python
# Search documents by text
results = vault.query_document_list(
    document_text="neural networks"
)
print(f"Documents matching 'neural networks': {results}")
```

Combining these filters is especially useful in large vaults with many lists. You can target exactly the data that is semantically relevant and was produced by the right pipeline stage.

```python
# Filter embedding search by description text
# Only searches within lists whose description contains "document embeddings"
similar_filtered = vault.query_embedding_list(
    embedding=query_embedding,
    description_text="document embeddings",
    use_approx=False,
)
print(f"Embeddings filtered by description: {similar_filtered}")
```

```python
# Filter document search by description text and code text
# description_text: restricts to lists whose description mentions "research paper"
# code_text: restricts to lists created by processes whose code contains "append_document"
results_filtered = vault.query_document_list(
    document_text="neural networks",
    description_text="research paper",
    code_text="append_document",
)
print(f"Documents filtered by description and code: {results_filtered}")
```

## Step 10: Process Queries

Every item in TableVault is attributed to the process that created it. Process queries let you audit the full output of a given pipeline run, or find which process was responsible for a particular item. This is useful when you have multiple pipelines writing to the same vault.

```python
# Find which process created these items
creation_process = vault.query_item_creation_process("research_papers")
print(f"Created by process: {creation_process}")
```

```python
# Find all items created in this process
process_items = vault.query_process_item("document_pipeline")
print(f"Items in process: {process_items}")
```
