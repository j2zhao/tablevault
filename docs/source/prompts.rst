Prompt Fields
===============

Populate the fields using a yaml file. 

Base Prompt
---------------

.. automodule:: tablevault.prompts.base_ptype
   :members:


Generator Prompt
------------------

.. automodule:: tablevault.prompts.gen_code_ptype
   :members:

Code Prompt
---------------

.. automodule:: tablevault.prompts.code_execution_ptype
   :members:

LLM Prompt
---------------


.. automodule:: tablevault.prompts.open_ai_threads_ptype
   :members:

DataTable
------------------


.. autoclass:: tablevault.prompts.table_string.DataTable
    :members:
    :undoc-members:
    :show-inheritance:

Table Reference
-------------------------


.. autoclass:: tablevault.prompts.table_string.TableReference
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