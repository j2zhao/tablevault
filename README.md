# MLVault

Version 2.0 of TableVault. Retains basic functionality from v1.0 but with more robust backend storage and simplier API.

You can view the previous website at: www.tablevault.org.

# Changes from v1.0

- Data is stored by data type rather than by instance.

- Tables are stored in ArangoDB rather than as DataFrames.

- First level support for arrays, embeddings, and text files.

- API changes to match industry standards.

# New Core Feature: Data Queries

The main reason for the redesign is that we want to design the metadata layer to support robust queries from the ground up.

The API supports:

- Vector search over data descriptions

- Reproducible search (with timestamps)

- Direct and indirect data lineage (based on code traces)

I would also like to include:

# Planned Features

- Experimental LLM Layer to support natural language search over context

