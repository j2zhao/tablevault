# Basic Workflow

This is a basic sample workflow for a TableVault repository.

The full function API can be found in [Core API](../api/core_api.md). To understand the structures and files being created, please read through [Core Concepts: Repository](../core_concepts/structure.md)  and [Core Concepts: Builders](../core_concepts/execution.md).

---

## 1. Make a Repository

```python
tablevault = TableVault(db_dir="test_tv", author="kong", create=True,
    description="This is an example repository.")
```

---

## 2. Make a Table

```python
tablevault.create_table(table_name="fruits_table", 
    description="This is an example table.")
```

---

## 3. Make a Table Instance

```python
tablevault.create_instance(table_name="fruits_table")
```

---

## 4. Write the Code Files

```python
tablevault.create_code_module(module_name="example_code")
```

**Example Code**

You can fill out the code file with the following code:

```python
import pandas as pd

def create_data_table_from_list(vals: list[str]):
    return pd.DataFrame({"temp_name": vals})
```

If you do not have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_code_module()`.


## 5. Write the Builder Files

```python
tablevault.create_builder_file(table_name="fruits_table", builder_name="fruits_table_index")
```

**Example Builder**

You can fill out the builder file with the following text:

```yaml
builder_type: IndexBuilder

changed_columns: ['fruits']        
primary_key: ['fruits']            

python_function: create_data_table_from_list       
code_module: example_code                

arguments:                               
    vals: ['pineapples', 'watermelons', 'coconuts']
is_custom: true                         

```

If you do not have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_builder_file()`.

---

## 6. Materialize the Instance

```python
tablevault.execute_instance(table_name="fruits_table")
```

---

## 7. Create a Second Instance

You can create a second instance of the `fruits_table` table in two different ways.

### (V1) Copying Previous Instances 

To make building the dataframe easier, you can copy the metadata of the last materialized instance:

```python
tablevault.create_instance(table_name="fruits_table", copy=True)
```

You simply need to change one line in the `fruits_table_index.YAML` file:

```yaml
arguments:                              
    vals: ['bananas']
```

You can then execute normally:

```python
tablevault.execute_instance(table_name="fruits_table")
```

---

### (V2) Externally Writing Instances 

If you want to edit the dataframe outside of the TableVault library, you can explicitly declare this when generating the new instance: 

```python
tablevault.create_instance(table_name="fruits_table", external_edit=True,
    description="Externally created dataframe.")
```

You can now write a new dataframe directly into our table:

```python
import pandas as pd

df = pd.DataFrame({'fruits': ['bananas']})

tablevault.write_instance(df, table_name="fruits_table")
```


!!! note "External Execution is Untracked"
    Edits to the dataframe made outside of the TableVault system are untracked. It is recommended that you fill out the `description` field in your instance to explain the edits, and keep your edits small and intuitive.

---


## 8. Query for a Dataframe

You can easily retrieve the dataframes of both instances: 

```python

instances = tablevault.get_table_instances(table_name="fruits_table")

df_1 = tablevault.get_dataframe(table_name="fruits_table", instance_id=instances[0])
df_2 = tablevault.get_dataframe(table_name="fruits_table")

```

The dataframes should have the expected values:

=== "**df_1**"
    ```
        fruits
    0   pineapples
    1   watermelons
    2   coconuts
    ```

=== "**df_2**"

    ```
        fruits
    0   bananas

    ```

---