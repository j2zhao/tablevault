# Workflow with Artifacts

In TableVault, an **artifact** is any file that isn't a dataframe. Every artifact must be linked to a specific table instance and indexed with a reference within the corresponding dataframe. This connection is established by the user through builders and Python functions.

## 1. Creating an Artifact Table and Instance

You can start with the `TableVault` repository generated with the [Basic Workflow](workflow.md).

```python
tablevault = TableVault(db_dir = "test_tv", author = "dixie")
tablevault.create_table(table_name = "fruit_images", allow_multiple_artifacts = False)
tablevault.create_instance(table_name = "fruit_images")
```

Setting `allow_multiple_artifacts` to `False` tells the system that we will only have one artifact repository for the whole folder.

## 2. A Code Function that Generates Artifacts

```python
tablevault.create_code_module(module_name = "fetch_images")
```

You can fill out the code file to import an image, given a type of fruit:

```python
import shutil

def fetch_image_from_string(fruit: str, artifact_dir:str ):
    file_path = f'./all_images/{fruit}.png' # pre-existing file
    new_file_path = f'{artifact_dir}/{fruit}.png'
    
    shutil.copy(file_path, new_file_path)
    
    return f'{fruit}.png' # return relative path
```

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_code_module`.

!!! note "Executing the Example"
    In order for your code to actually execute, an actual image needs to exist in the  `file_path` location.

## 3. A Builder with `~ARTIFACT_STRING~`
```python
tablevault.create_builder_file("fruit_images_index")
tablevault.create_builder_file("fetch_image_artifact")
```

=== "fruit_images_index.yaml"
    ```yaml
    builder_type: IndexBuilder

    changed_columns: ['fruits']        # Output columns
    primary_key: ['fruits']            # DataFrame primary key (optional)

    python_function: create_data_table_from_table           # Function to execute
    code_module: table_generation                 # Module containing the function

    arguments:                               # Arguments passed to the function
        df: <<fruit_table.fruits>>                  
    ```
=== "fetch_image_artifact.yaml"
    ```yaml
    builder_type: ColumnBuilder

    changed_columns: ['fruit_image']                        # Output columns

    python_function: create_data_table_from_table           # Function to execute
    code_module: table_generation                           # Module containing the function
    
    is_custom: true                                         # Mark as user-supplied (searches in code_functions)
    return_type: row-wise                                       # Specifies if the function processes row by row
    
    arguments:                                              # Arguments passed to the function
        fruit: <<self.fruits[index]>> 
        artifact_dir: ~ARTIFACT_FOLDER~ 

    dtypes:                                                 # Column Data Types 
        fruit_image: artifact_string            
    ```

The `~ARTIFACT_FOLDER~` keyword is automatically replaced with the correct folder path at runtime. The `artifact_string` data type tells the system that the `fruit_image` column is linked to artifacts. 

Each artifact file needs a corresponding `artifact_string` value in the dataframe. This value is the file path of the artifact, relative to the temporary artifact folder. This is user-generated (as seen in the `fetch_image_from_string()` function).

You can see examples of `TableReference` strings with `<<fruit_table.fruits>> ` and `<<self.fruits[index]>> `. The first string returns a column in the lastest `fruit_table` dataframe. The second string returns a string corresponding to the current index value from the `fruits` column of the **executing dataframe**.

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text`, in `create_code_module`.

## 4. Execute and Materialize Instance

```python
tablevault.execute_instance("fruit_image")
```

!!! note "**Strict Checks**"
    Various checks are performed before the table is materialized to ensure everything is configured correctly. Most importantly, each `artifact_string` value must have a corresponding artifact file and vice versa.

Once materialized, the artifact is moved to a permanent location associated with the materialized instance.

## 5. Query for An Artifact Dataframe

You can easily retrieve the dataframe with the full or partial artifact path: 

```python

df_1 = tablevault.get_dataframe(table_name = "fruits_table", full_artifact_path = True)
df_2 = tablevault.get_dataframe(table_name = "fruits_table", full_artifact_path = False)

```

The dataframes should have the expected values:

=== "**df_1**"
    ```
       fruits     fruit_image
    0  bananas    test_tv/fruit_image/artifacts/bananas.png
    ```

=== "**df_2**"

    ```
       fruits     fruit_image
    0  bananas    bananas.png
    ```

---
