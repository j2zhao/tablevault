# Welcome to TableVault

TableVault is designed to manage data tables and artifacts in **complex and dynamic data workflows**. It promotes **data reusability** by capturing the full context of data transformations and ensuring atomic and transactional data states—transformations either clearly succeed or fail. It enhances **data interoperability** by easily connecting previous results with the configuration and input variables of subsequent transformations.

TableVault integrates with Python and can be used with popular data science libraries and tools, including Jupyter Notebooks, Colab, Pandas, NumPy, Transformers, and many others. The tool is particularly effective for workflows involving multiple dataframes, external artifacts (e.g., images, videos, documents), and **large language model executions**.

Installation via pip:

```bash
pip install "git+https://github.com/j2zhao/tablevault.git"
```

This library is fully compatible with `Python>=3.11`.

**Note:** An official release to the Python Package Index is scheduled for July 2025.