For each item list, you can store multiple descriptions that contain semantic information about that particular list. Each description must contain both text and an embedding vector. The size of the embedding vector must match the value defined during the creation of the TableVault repository.


```python

description = "Full novel of Frankenstein by Mary Shelley in paragraphs"
embedding = MODEL(description)

vault.create_description("frankenstein_novel", description, embedding)

```

When querying over item types, you can filter the query by token search over the text or vector search over the embedding.

## AI Generated Descriptions

We would like to support automatic description/embedding generation in TableVault in the future with AI models. If you have any interest in this area, please feel free to reach out!