# About TableVault

## Overview

TableVault is a centralized data repository designed for tracking ML experiment artifacts and enabling cross-process data sharing between Python scripts and notebooks.

## Key Features

### Centralized Data Storage

TableVault stores all experiment data in ArangoDB, providing:

- Persistent storage of documents, embeddings, files, and records
- Automatic versioning and lineage tracking
- Cross-session data access and querying

### Cross-Process Communication

Multiple Python processes can:

- Read and write to shared item lists
- Query across all stored data
- Send stop/pause/resume signals to each other
- Coordinate long-running ML workflows

### Lineage Tracking

Every data item maintains connections to:

- The session that created it
- The code that generated it
- Input items it was derived from
- Output items derived from it

### Flexible Querying

Search data by:

- Text content and token matches
- Vector similarity (for embeddings)
- Code that created the items
- Descriptions and metadata
- Parent/child relationships

## Use Cases

### Experiment Tracking

Track ML experiments with full provenance:

- Store model artifacts, metrics, and hyperparameters
- Query across experiments by performance or configuration
- Trace data lineage from raw data to final models

### RAG Pipelines

Build searchable document stores:

- Store document chunks and embeddings
- Link embeddings to source documents
- Query by semantic similarity

### Parallel Processing

Coordinate distributed workflows:

- Share data between workers
- Control execution with stop/pause/resume
- Track which processes created which data

### Longitudinal Analysis

Compare experiments over time:

- Query experiments by code patterns
- Find all data created by specific pipelines
- Analyze trends across experiment variations

## Architecture

TableVault uses:

- **ArangoDB**: Multi-model database for documents, graphs, and vectors
- **Python Sessions**: Track code execution in scripts and notebooks
- **Item Lists**: Ordered collections of typed data (documents, embeddings, files, records)
- **Descriptions**: Semantic metadata for queryability

## Contributing

TableVault is open source. Contributions are welcome.

- **Repository**: [github.com/j2zhao/tablevault](https://github.com/j2zhao/tablevault)
- **Issues**: Report bugs and request features via GitHub Issues
- **Pull Requests**: Submit improvements via GitHub PRs

## License

TableVault is released under an open source license. See the repository for details.

## Contact

For questions, feature requests, or collaboration opportunities, please open an issue on the GitHub repository.
