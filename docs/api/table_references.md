# `TableReference` String Guide

`TableReference` strings provide a powerful way to dynamically fetch and use data from other table instances, or the current table instance, directly within most string-based fields of your builder YAML files. This allows for highly dynamic and data-driven configurations.

---

## Core Concept: The `<< ... >>` Wrapper

The fundamental syntax for a `TableReference` string is to enclose the reference string within double angle brackets: `<< ... >>`.

The `<< ... >>` syntax can be applied to any singular string, boolean or numeric entry in your builder definition, such as values in the `arguments` block, items in the `changed_columns` list, or even the `python_function` and `code_module` names themselves. 

The primary exception is the `dependencies` field, which does not support this dynamic resolution (`dependencies` inform the system which tables are loaded and must be resolved before `TableReference` strings are parsed).


---

**Example of broad usage in a builder YAML:**

```yaml
# In an IndexBuilder or ColumnBuilder
builder_type: IndexBuilder
python_function: "<<config_table.builder_function_name[use_case::'main_index']>>" # Dynamic function name
code_module: "<<config_table.builder_module_name>>" # Dynamic module name

changed_columns:
  - "<<config_table.primary_output_column>>"       # Dynamic column name
  - "fixed_secondary_column"
  - "<<self.another_dynamic_column_name_source[index]>>" # Column name derived from self

arguments:
  source_file_path: "/data/raw/<<config_table.file_name[source_id::'source_A']>>.csv"
  lookup_value: "<<lookup_table.value[key::<<self.current_key[index]>>]>>"
  static_text_with_ref: "Report for ID: <<self.id[index]>>"
```

If a field value is entirely a `TableReference` string (e.g., `python_function: "<<config.func_name>>"`), the resolved value of the reference will be used directly. If the reference is part of a larger string (e.g., in `arguments`), the resolved value will be converted to a string and substituted into place.

---

## Anatomy of a `TableReference` String

Inside the `<< ... >>` wrapper, a `TableReference` string follows a specific structure to identify the table, an optional instance_id, specific columns, and optional filtering conditions:

```
tableName(instance_id).{column1,column2,...}[condition1,condition2,...]
```

All parts (instance_id, columns, conditions) are optional.

---

**Components:**

1.  **`tableName`**: The name of the table to query.
2.  **`(instance_id)`**: (Optional) Specifies a particular instance of the table.
3.  **`.{columns}` or `.COLUMN`**: (Optional) Selects specific columns.
4.  **`[conditions]`**: (Optional) Filters the rows of the table.

---

### 1. Table Name (`tableName`)

* **Syntax**: A string of alphanumeric characters, underscores (`_`), or hyphens (`-`).
* **Special Keyword `self`**: The keyword `self` refers to the current table instance being processed by the builder.

* **Dynamic Table Name**: The table name itself can be a nested `TableReference` string.
    * Example in a field: `code_module: "<< <<table_map.module_column[type::'etl']>> >>"`
    * Reference string example: `<< <<another_table.config_key[type::'source']>>.data_column >>`


!!! note "`self` Keyword Restrictions"
    The `self` keyword can only be used in the `argument` field.

---

### 2. Version (`(instance_id)`)

* **Syntax**: Instance in parentheses, e.g., `(base_1748113624_d049944b-8548-46d2-a247-bbf3769fbadc)`.
* **Optional**: If omitted, TableVault will typically use the latest available instance of the table based on its internal logic.
* **Dynamic Version**: The version string can be a nested `TableReference`.
    * Example reference string: `my_table(<<version_control_table.active_version[table_name::'my_table']>>)`

---

### 3. Columns (`.{columns}` or `.COLUMN`)

* **Syntax**:
    * **Single Column**: Preceded by a dot (`.`), e.g., `.user_id`.
    * **Multiple Columns**: Preceded by a dot (`.`) and enclosed in curly braces `{}`, with column names separated by commas, e.g., `.{name,email,age}`.
* **Optional**:
    * If omitted, and conditions are present, all columns are available for filtering, and the selected columns depend on the output simplification (see below).
    * If omitted, and no conditions are present, the entire DataFrame (or its simplified form) is returned.
* **Dynamic Column Names**: Column names within the list (or the single column name) can be nested `TableReferences`. This is highly relevant for fields like `changed_columns` or `primary_key`.
    * Example in `changed_columns`: `changed_columns: ["id", "<<config_table.main_data_field_name>>"]`
    * Example reference string for a column name: `my_table.<<config.target_column>>`
    * Example reference string with multiple dynamic columns: `my_table.{id,<<audit_table.tracked_field[user::'admin']>>,status}`
* **Special Case: `self.index`**:
    If you use `<<self.index>>` this specifically resolves to the current row's physical index value during row-wise operations.

---

### 4. Conditions (`[conditions]`)

* **Syntax**: Enclosed in square brackets `[...]`. Multiple conditions are separated by commas `,`.
* **Optional**: If omitted, all rows (of the selected version and columns) are considered.
* Each condition specifies a column to filter on and the criteria.

**Condition Types:**

1.  **Equality (`columnName::value`)**:
    * Filters rows where `columnName` equals `value`.
    * Example reference string: `orders.product_id[customer_id::'cust123',status::'shipped']`
    * The `value` is automatically quoted for string comparisons if not already quoted (e.g., `status::shipped` becomes `status == 'shipped'`). Numerical values are used directly.
    * `value` can be a nested a `TableReference` string: `orders.items[user_id::<<user_table.id[username::'jdoe']>>]`

2.  **Range (`columnName::start_value:end_value`)**:
    * Filters rows where `columnName` is greater than or equal to `start_value` AND less than `end_value`.
    * Example reference string: `events.timestamp[timestamp::'2023-01-01T00:00:00':'2023-01-01T23:59:59']`
    * `start_value` and `end_value` can be literals or nested `TableReference` strings. Values are formatted appropriately for comparison based on the column's data type.

3.  **Implicit Index/Contextual Value (`columnName`)**:
    * Filters rows where `columnName` equals a contextually provided `index` value (the physical index of the row currently being processed by the builder).
    * Example: If processing row `101` of `self`, then the reference `<<other_table.data_column[join_key_in_other_table]>>` (within an argument) would attempt to find rows in `other_table` where `join_key_in_other_table == 101`.
    * This is particularly useful for lookups related to the current item in `self`.
    * If `self.some_column[key_column]` is used, and `index` is defined, it implies `self.some_column` where `key_column == index`.

* **Dynamic Keys and Values**: All parts of a condition (the column name, the value, start/end values) can be nested `TableReference` strings.
    * Example reference string: `my_table[<<config.filter_column>>::<<config.filter_value>>]`

!!! note "`index` Condition"

    The `index` keyword can only be used in the `arguments` key of a row-wise function (when `row-wise` is set to `true`).

---

## Nested References

As shown in examples above, any component of a `TableReference` string —the table name, version string, column names, condition keys, or condition values—can itself be another `TableReference` stringenclosed in `<< ... >>`. TableVault will resolve the innermost references first and use their results to construct the outer reference before resolving it.

**Complex Example (from code, used in an argument):**
`<<stories.artifact_name[paper_name::<<self.paper_name[index]>>]>>`

1.  `<<self.paper_name[index]>>`: Resolves first. It fetches the `paper_name` from the current row (`index`) of the `self` table.

2.  Let's say the above yields `'my_research_paper'`.

3.  The outer reference becomes: `<<stories.artifact_name[paper_name::'my_research_paper']>>`.

4.  This then fetches `artifact_name` from the `stories` table where `paper_name` is `'my_research_paper'`.

---

## Resolution and Data Retrieval

* When a builder is executed, TableVault parses these reference strings from the relevant YAML fields.
* It uses an internal cache of DataFrames (for already loaded tables and versions) to retrieve data efficiently.
* For references involving `self` or implicit index conditions, the context of the current row being processed (often an integer `index`) is crucial for resolving the correct data.
* The recursive parsing handles references within lists, dictionaries, and other nested structures in the YAML, as long as they ultimately resolve to strings or collections of strings where references are found.

---

## Examples of Reference Strings

These examples illustrate the reference string syntax itself. These strings would be placed inside `<< >>` within a suitable YAML field.

1.  **Fetch a single column from another table:**
    `my_data_table.user_email`
    - **Result:** A dataframe with a single `user_email` column *(might be converted to a list contextually)*.

2.  **Get a specific value using a filter:**
    `users_table.full_name[user_id::'user-007']`
    - **Result:** A single string representing `full_name` for `user_id`.

3.  **Get a value from `self` based on the current row's context (implicit index):**
    `self.status[id_column_of_self]`
    - **Result:** A single boolean representing `self.status` where `id_column_of_self == index`.

4.  **Reference with a specific version:**
    `app_settings(base_1748275064_5782ef5b-4023-4618-a419-cf921c365c64).timeout_ms`
    - **Result:** A dataframe with a single `user_email` column that is from  the `base_1748275064_5782ef5b-4023-4618-a419-cf921c365c64` instance.


5.  **Using a range condition:**
    `transactions.amount[transaction_date::'2024-01-01':'2024-01-31']` 
    -  **Result:** A dataframe with a single `amount` column with transactions between `2024-01-01` and `2024-01-31` (if properly sorted).
    -  **Note** the range is calculated by the physical index.

6.  **Nested reference for dynamic filtering:**
    `preferences.setting_value[user_id::<<self.user_identifier[index]>>, setting_key::'theme']`
    - **Result:** A single string representing `setting_value` for `user_id` at current index with the `theme` key.
---

## Error Handling

If a `TableReference` string is malformed (e.g., unbalanced brackets, illegal characters) or if a reference cannot be resolved at runtime (e.g., table not found, column missing, nested reference fails), a `TableReferenceError` will typically be raised, halting the builder process. Ensure your references are correct and the data they point to exists.

---