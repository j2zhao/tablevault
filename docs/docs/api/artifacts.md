# Artifacts in TableVault

In TableVault, an **artifact** is any file that isn't a dataframe. Every artifact must be linked to a specific table instance and indexed with a reference within the corresponding dataframe. This connection is established by the user through builders and Python functions.

---

## Moving an Artifact into TableVault

Because an artifact can be any type of file, you have complete flexibility in how you generate it. It's your responsibility to implement the process for moving the generated artifact into the TableVault repository.

Each temporary instance includes a designated artifact folder. Your program can find this folder in one of two ways:
1.  Using the TableVault API.
2.  Specifying the `~ARTIFACT_FOLDER~` keyword in a Builder file. This keyword is automatically replaced with the correct folder path at runtime.

---

### Linking Artifacts to a DataFrame

To link artifacts to a table, the dataframe you generate must include a column with the `artifact_string` data type. You can declare this in a Builder file using the `dtype` argument or within a `dtype` dictionary when using the `write_instance` function.

Each artifact file needs a corresponding `artifact_string` value in the dataframe. This value is the file path of the artifact, relative to the temporary artifact folder.

For example, if you have an image artifact located at `TEMP_INSTANCE/artifacts/raining_cats_and_dogs.png`, its corresponding value in the `artifact_string` column would simply be `raining_cats_and_dogs.png`.

> **Strict Checks**
> Various checks are performed before the table is materialized to ensure everything is configured correctly.

Once materialized, the artifact is moved to a permanent location associated with the materialized instance.

---

## Accessing Stored Artifacts

When you fetch a dataframe using the TableVault API, you can convert all `artifact_string` values into full relative paths from where the API was called. To do this, set the `full_artifact_path` parameter to `true`.

Following the previous example, the value `raining_cats_and_dogs.png` might be converted to `gen_ai_images_tv/image_store/artifacts/raining_cats_and_dogs.png`. This allows you to directly access the file without needing to know the underlying directory structure.