Prompt Fields
===============

Populate the fields using a yaml file. 

Base Prompt
---------------

.. automodule:: tablevault.col_builders.base_ptype
   :members:


Generator Prompt
------------------

.. automodule:: tablevault.col_builders.gen_code_ptype
   :members:

Code Prompt
---------------

.. automodule:: tablevault.col_builders.code_execution_ptype
   :members:

LLM Prompt
---------------


.. automodule:: tablevault.col_builders.open_ai_threads_ptype
   :members:

DataTable
------------------


.. autoclass:: tablevault.col_builders.table_string.DataTable
    :members:
    :undoc-members:
    :show-inheritance:

Table Reference
-------------------------


.. autoclass:: tablevault.col_builders.table_string.TableReference
    :members:
    :undoc-members:
    :show-inheritance:

TableString Type
-------------------------

The ``TableString`` type alias is defined as a union of three types:

- **DataTable**
- **TableReference**
- **str**

As an entry to a yaml file, the data type is parsed 