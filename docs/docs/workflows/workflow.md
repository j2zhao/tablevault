# Basic Workflow

The TableVault API allows users to generate and execute instances of data tables through a straightforward workflow. The process begins with establishing a central repository, followed by defining the structure of a table, creating specific instances of that table, and then using builder files to populate and materialize the data.

---
## 1. Make a Repository

```python
tablevault = TableVault(db_dir = "test_tv", author = "kong", create = True,
    description = "this is an example repository.")
```

---

## 2. Make a Table

```python
tablevault.create_table(table_name = "fruits_table", 
    description = "this is an example table.")
```

---

## 3. Make a Table Instance

```python
tablevault.create_instance(table_name = "fruits_table")
```

---

## 4. Write the Code Files

```python
tablevault.create_code_module(module_name = "example_code")
```

**Example Code**

You can fill out the code file with the following code:

```python
import pandas as pd

def create_data_table_from_list(vals: list[str]):
    return pd.DataFrame({"temp_name": vals})
```

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_code_module`.


## 4. Write the Builder Files

```python
tablevault.create_builder_file(table_name = "fruits_table", builder_name = "fruits_table_index")
```

**Example Builder**

You can fill out the builder file with the following text:

```yaml
builder_type: IndexBuilder

changed_columns: ['fruits']        # Output columns
primary_key: ['fruits']            # DataFrame primary key (optional)

python_function: create_data_table_from_list       # Function to execute
code_module: example_code                 # Module containing the function

arguments:                               # Arguments passed to the function
    vals: ['pineapples', 'watermelons', 'coconuts']
is_custom: true                         #using a user-supplied function in code_module

```

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_builder_file`.

---

## 5. Materialize the Instance

```python
tablevault.execute_instance(table_name = "fruits_table")
```

---

## 6. Create a Second Instance

There are two different ways, you can create a second instance of the `fruits_table` table.

### 1. Copying Previous Instances

To make building the dataframe easier, you can copy the metadata of the last materialized instance:

```python
tablevault.create_instance(table_name = "fruits_table", copy = True)
```

You simply need to change one line in the `fruits_table_index.YAML` file:

```yaml
arguments:                               # Arguments passed to the function
    vals: ['bananas']
```

You then can execute normally:

```python
tablevault.execute_instance(table_name = "fruits_table")
```

---

### 2. Externally Writing Instances

If you want to edit the dataframe outside of the TableVault library (not generally recommended), you can explicitly declare this when generating the new instance: 

```python
tablevault.create_instance(table_name = "fruits_table", external_edit = True,
    description="externally created dataframe")
```

You can now write a new dataframe directly into our table:

```python
import pandas as pd

df = pd.DataFrame({'fruits': ['bananas']})

tablevault.write_instance(df, table_name = "fruits_table")
```

---


## 7. Query for A Dataframe

You can easily retrieve the dataframe of both instances: 

```python

instances = tablevault.get_instances(table_name = "fruits_table")

df_1 = tablevault.get_dataframe(table_name = "fruits_table", instance_id = instances[0])
df_2 = tablevault.get_dataframe(table_name = "fruits_table")

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