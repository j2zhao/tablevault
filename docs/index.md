# Welcome to TableVault

TableVault is a centralized data repository backed by ArangoDB that enables cross-process data filtering between multiple Python scripts and notebooks.

TableVault can search over stored data items (e.g., dataframe rows, embeddings, documents) by name, exact matches, token matches, or vector similarity. These items can be filtered by the original generating code, text and embedding descriptions, or upstream and downstream items.

Using TableVault, Python processes can send requests to other ongoing processes to "stop", "pause", and "continue". Communication is backed by the centralized data repository, and "stop" and "pause" actions are guaranteed to occur only at user-defined checkpoints.

Applications include fast comparisons of agentic and ML workflows across many experiments. For example, across variations of a RAG workflow, TableVault can perform longitudinal queries to compare accuracy scores when using different language model prompts.

## Installation

Install via pip:

```bash
pip install tablevault
```
