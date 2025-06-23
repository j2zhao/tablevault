# Welcome to TableVault

TableVault is designed to manage data tables and artifacts in **complex and dynamic data workflows**. It promotes **data reusability** by capturing the full context of data transformations and ensuring atomic and transactional data states—transformations either clearly succeed or fail. It enhances **data interoperability** by easily connecting previous results with the configuration and input variables of subsequent transformations.

TableVault integrates with Python and can be used with popular data science libraries and tools, including Jupyter Notebooks, Pandas, NumPy, Transformers, and many others. The tool is particularly effective for workflows involving multiple dataframes, external artifacts (e.g., images, videos, documents), and **large language model executions**. TableVault is suited for **agentic pipelines** since one data-generating process can easily spin additional subprocesses.

Installation via pip:

```bash
pip install "git+https://github.com/j2zhao/tablevault.git"
```

This library is fully compatible with `Python>=3.11`.

**Note:** An official release to the Python Package Index is scheduled for July 2025.

## Quick Start

Check out [Basic Workflow](./workflows/workflow.md) for a simple generic setup, and our Colab examples [(1) Short Stories Q&A with OpenAI](https://colab.research.google.com/drive/1vHg5Vb8r1Zax2pKLOX6phPEuIDVhFctC?usp=sharing) and  [(2) GritLM Embeddings from Scientific Abstracts](https://colab.research.google.com/drive/1X4tFpPSfMnQ_Ch0nSNUTmiEcT0Eo40Uj?usp=sharing) for concrete use cases.

To understand the basics of TableVault, read through the [Core Concepts](./core_concepts/structure.md).


