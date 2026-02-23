# Descriptions

Each item list can store multiple descriptions with semantic metadata. Each description must include both text and an embedding vector. The embedding dimension must match the value configured when the TableVault repository was created.


```python
description = "Full novel of Frankenstein by Mary Shelley in paragraphs"
embedding = MODEL(description)

vault.create_description("frankenstein_novel", description, embedding)
```

When querying item types, you can filter by token search over description text or vector similarity over description embeddings.

## AI Generated Descriptions

Automatic description and embedding generation may be added in a future release. If this is useful for your workflow, feel free to reach out.
