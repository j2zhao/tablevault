# Item Lists

Each data item in TableVault is stored in an ordered collection called an item list. Multiple processes may incrementally append data to a list as long as the data matches the list constraints. Data items within a list can be identified by their position and index values, or found through search matches.

For example, we can store the text of a novel, such as "Frankenstein", as a document list. If we store each paragraph as a data item, we can find the third paragraph by paragraph index (e.g., 2), character/position offset (e.g., 2494), or text match (e.g., "These reflections have dispelled the agitation").

We can also store a list of vector embeddings, such as embeddings generated from "ImageNet", as an embedding list. We can find an embedding by index/position (e.g., 2) in the list, or by vector similarity match (e.g., an embedding that matches the vector).

## Item List Creation

Once a Vault object has been created (see [Repository Setup](setup.md)), each item list can be created by defining the item type and a unique name.

!!! note "Unique Names"
    Note that names must be unique across all item types and processes. Once a name is used as an item list, it currently cannot be reused, even if the data is later deleted.

```python
# Example: Create a document list
vault.create_document_list("frankenstein_novel")

# Example: Create an embedding list
vault.create_embedding_list("image_net_embeddings", ndim=1024)

```
An embedding list has an extra constraint that the length of all embeddings must be the same.

## Appending Items

Once a list has been defined in a TableVault repository, *any* process can append data to the list.

```python
# Example: Add a Frankenstein paragraph to frankenstein_novel
vault.append_document(
    "frankenstein_novel",
    text="These reflections have dispelled the agitation...",
)

# Example: Add a generated embedding to image_net_embeddings
embedding = MODEL(image)
vault.append_embedding("image_net_embeddings", embedding)

```

Opportunistic locking ensures that each item is appended atomically. In a single process, the stored order is the same as the appending order. With concurrent processes, you can use the `index` parameter to define absolute ordering if necessary.

## Input Item Parameters

When you are appending a data item, you can define the input items that contributed to the generation of that item. This allows for more powerful queries, since you can subsequently query for lineage between items.

For example, if you have previously stored an index of ImageNet images as a file list, you can link that index to the generated embedding list.

```python
image_files = vault.query_item_content("image_net_files")

for index, image_file in enumerate(image_files):
    image = GET_IMAGE(image_file)  # placeholder function
    embedding = MODEL(image)  # placeholder model
    input_items = {"image_net_files": [index, index + 1]}
    vault.append_embedding(
        "image_net_embeddings",
        embedding,
        input_items=input_items,
    )

```

Here `[index, index + 1]` represents the start and end position of the relevant data inside `image_net_files`.

## Basic Item List Queries

See Item List Queries for more information.

## Basic Item List Types

Currently, we support the following types: **files**, **documents**, **embeddings**, and **records**. See Item List Types for more information.
