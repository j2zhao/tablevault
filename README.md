# TableVault

TableVault is a Python package for storing and querying workflow data with lineage tracking across scripts and notebooks.

It uses ArangoDB as the backend and gives you a single API (`Vault`) to:

- Store typed data lists (`file`, `document`, `embedding`, `record`)
- Track upstream/downstream dependencies between items
- Search by text, code provenance, and embedding similarity
- Coordinate long-running processes with safe pause/stop checkpoints

## Documentation

You can find the full documentation at [tablevault.org](https://tablevault.org).

## Installation

Install from PyPI:

```bash
pip install tablevault
```

## Citation

If you use TableVault in research, cite:

- Zhao, J. and Krishnan, S. (2025). *TableVault: Managing Dynamic Data Collections for LLM-Augmented Workflows*. NOVAS @ SIGMOD.  
  ArXiv: <https://arxiv.org/abs/2506.18257>

## License

MIT License. See `LICENSE`.
