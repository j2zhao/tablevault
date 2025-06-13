# Comparisons

To understand the use cases of TableVault further, we compare it to some popular data science tools.

---

## TableVault vs. SQLite

### Key Differences

| ***TableVault*** | ***SQLite*** |
| :--- | :--- |
| **Native Support for Unstructured Files with Artifacts** | **Only Supports Database Tables** |
| **Transparent Data Storage Using the OS File System** | **All Tables are Stored Within a Single SQLite File** |
| **Focus on Python Execution** | **Only Supports SQL Execution** |
| **Performance Optimizations Are Up to the User** | **Internal Performance Optimizations** |

### Summary

TableVault is geared towards Python operations over complex, versioned datasets and artifacts, while SQLite and other traditional databases are primarily focused on SQL execution on database tables. Both TableVault and SQLite maintain data integrity and reliability by enforcing ACID principles and techniques.

If your workflow primarily deals with SQL and tables, SQLite might be preferred. If you work in data science or machine learning, deal with heterogeneous data, or want exact control over execution, TableVault might be better suited for your application.

---

## TableVault vs. Apache Airflow

### Key Differences

| ***TableVault*** | ***Airflow*** |
| :--- | :--- |
| **Native Support for Unstructured Files with Artifacts** | **Does Not Store Data Artifacts** |
| **Can Query Data Artifacts from Different Tables** | **DAGs are Treated as Independent** |
| **Built-in Logging for All Data Operations** | **Only Logs DAG Execution** |
| **Execution Scheduling Is Up to the User** | **Controls Scheduling of Pipelines** |

### Summary

TableVault is a lightweight execution system designed to ensure data integrity and transparency and improve data reusability across different workflows. Apache Airflow is a platform to programmatically author, schedule, and monitor workflows (data pipelines). Both TableVault and Airflow track and version data transformation executions.

If you need a tool to organize recurring executions with a rich ecosystem of custom operators, Airflow might be the right choice. If you want a Python execution system that organizes data outputs and manages metadata to improve data explainability, TableVault might make more sense for your workflow.

---

## TableVault vs. LangChain

### Key Differences

| ***TableVault*** | ***LangChain*** |
| :--- | :--- |
| **Every LLM Execution and Output Is Logged** | **No Record of Executed LLM Calls** |
| **Allows Versioning of Data Artifacts** | **No Explicit Versioning** |
| **Agents Interact Safely with a Persistent Data Store** | **Agents Don't Directly Write to Persistent Data** |
| **General, User-Defined Python Functions** | **Specialized Suite of Custom LLM Operations** |

### Summary

Large language models can be used with TableVault by calling the relevant API (including the `LangChain` library) or by locally running the model. TableVault is complementary to libraries such as LangChain and can be used in conjunction to organize multiple model calls, inputs, and outputs.

TableVault enables more complex language model workflows by explicitly tracking execution versions and allowing models to safely interact with persistent artifacts that all conform to the same organizational structure.