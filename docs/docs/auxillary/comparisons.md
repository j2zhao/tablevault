# Comparisons

## 1. TableVault vs. SQLite

### Key Differences

| ***TableVault***                                            | ***SQLite***                                          |
| ----------------------------------------------------------- | ----------------------------------------------------- |
| **Native Support for Unstructured Files with Artifacts**    | **Only Supports Database Tables**                     |
| **Transparent Data Storage Using OS File System**           | **All Tables are Stored within a single SQLite File** | 
| **Focus on Python Execution**                               | **Only Supports SQL Executions**                      |
| **Performance Optimizations Up to the User**                | **Internal Performance Optimizations**                |

### Summary

TableVault is geared towards Python operations over complex, versioned datasets and artifacts, while SQLite and other traditional databases are primarily focused on SQL execution on database tables. Both TableVault and SQLite maintains data integrity and reliability by enforcing ACID principles and techniques.

If your workflow is primarily dealing with SQL and tables, SQLite might be preferred. If you are working in data science or machine learning, you are primarily dealing with heterogeneous data, or you want exact control over execution, TableVault might be better suited for your application.

---

## 2. TableVault vs. Apache Airflow

### Key Differences


| ***TableVault***                                            | ***Airflow***                                         |
| ----------------------------------------------------------- | ----------------------------------------------------- |
| **Native Support for Unstructured Files with Artifacts**    | **Does not Store Data Artifacts**                     |
| **Can Query Data Artifacts from Different Tables**          | **DAGs are Treated as Independent**                   | 
| **Built in Logging for All Data Operations**                | **Only Logs DAG Execution**                           |
| **Execution Scheduling up to the User**                     | **Controls Scheduling of Pipelines**                  |

### Summary

TableVault is a lightweight execution system that is designed to ensure data integrity and transparency, and improve data reusability across different workfows. Apache Airflow is a platform to programmatically author, schedule, and monitor workflows (data pipelines). Both TableVault and Airflow track and version data transformation executions.

If you need a tool to organize recurring executions with a rich ecosystem of custom operators, Airflow might be the right choice. If you want a Python execution system that organizes data outputs and manages metadata to improve data explainability, TableVault might make more sense for your workflow.

---

## 3. TableVauilt vs. LangChain

### Key Differences

| ***TableVault***                                            | ***LangChain***                                       |
| ----------------------------------------------------------- | ----------------------------------------------------- |
| **Every LLM Execution and Output is Logged**                | **No Record of Executed LLM Calls**                   |
| **Allows Versioning of Data Artifacts**                     | **No Explict Versioning**                             |
| **Agents Interact Safely with Persistant Data Store**       | **Agents Don't Directly Write to Persistant Data**    | 
| **General User-Defined Python Functions**                   | **Specialized Suite of Custom LLM Operations**        |


### Summary

Large Language models can be used with TableVault by calling the relevant API (including the `LangChain` library) or locally running the model. TableVault is complimentary to libraries such as LangChain and can be used in conjunction to organize multiple model calls, inputs and outputs. 

TableVault enables more complex language model workflows by explicitly tracking execution versions, and allowing models to safely interact with persistant artifacts that all conform to the same organization structure.

---