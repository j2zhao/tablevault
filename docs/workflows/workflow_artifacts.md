# Workflow with Artifacts

This is a basic workflow for handling artifacts in TableVault. To understand the basics of builders and artifacts, please read [Core Concepts: Artifacts](../core_concepts/execution.md#artifacts) and the [Builders Guide](../api/builders.md).

---

## 1. Creating an Artifact Table and Instance

You can continue from the TableVault repository generated from the [Basic Workflow](workflow.md).

```python
tablevault = TableVault(db_dir = "test_tv", author = "dixie")
tablevault.create_table(table_name = "fruit_images", allow_multiple_artifacts = False)
tablevault.create_instance(table_name = "fruit_images")
```

Setting `allow_multiple_artifacts` to `False` tells the system that there will only be one artifact repository for the entire folder.


## 2. A Code Function that Generates Artifacts

```python
tablevault.create_code_module(module_name = "fetch_images")
```

You can then populate the code file to import an image, given a type of fruit:

```python
import shutil

def fetch_image_from_string(fruit: str, artifact_dir:str ):
    file_path = f'./all_images/{fruit}.png' # pre-existing file
    new_file_path = f'{artifact_dir}/{fruit}.png'
    
    shutil.copy(file_path, new_file_path)
    
    return f'{fruit}.png' # return relative path
```

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text` in `create_code_module()`.

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

    changed_columns: ['fruits']        
    primary_key: ['fruits']           

    python_function: create_data_table_from_table       
    code_module: table_generation                 

    arguments:                               
        df: <<fruit_table.fruits>>                  
    ```

=== "fetch_image_artifact.yaml"

    ```yaml
    builder_type: ColumnBuilder

    changed_columns: ['fruit_image']                      

    python_function: create_data_table_from_table      
    code_module: table_generation                          
    
    is_custom: true                  
    return_type: row-wise            
    
    arguments:                                 
        fruit: <<self.fruits[index]>> 
        artifact_dir: ~ARTIFACT_FOLDER~ 

    dtypes:                           
        fruit_image: artifact_string            
    ```

If you don't have direct access to a text editor on your platform, you can add the code as a string argument, `text` in `create_code_module()`.

## 4. Execute and Materialize Instance

```python
tablevault.execute_instance("fruit_image")
```

!!! note "**Strict Checks**"
    Various checks are performed before the table is materialized to ensure everything is configured correctly. Most importantly, each `artifact_string` value must have a corresponding artifact file and vice versa.

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
