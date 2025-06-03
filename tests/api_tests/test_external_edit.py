from tablevault.core import TableVault
from . import helper
import pandas as pd


def test_write_table_basic(tablevault: TableVault):
    ids = []
    id = tablevault.create_table("stories", allow_multiple_artifacts=False)
    ids.append(id)
    id = tablevault.create_instance("stories", external_edit=True)
    ids.append(id)
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.0],
        }
    )
    df["id"] = df["id"].astype("Int64")
    df["score"] = df["score"].astype("Float64")
    id = tablevault.write_instance(df, "stories")
    ids.append(id)
    df2, _ = tablevault.get_dataframe("stories")
    # df2.drop(columns=["index"], inplace=True)
    assert df.equals(df2)
    helper.evaluate_operation_logging(ids)


def test_write_table_copy(tablevault: TableVault):
    ids = []
    id = tablevault.create_table("stories", allow_multiple_artifacts=False)
    ids.append(id)
    id = tablevault.create_instance("stories", external_edit=True)
    ids.append(id)
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.0],
        }
    )
    df["id"] = df["id"].astype("Int64")
    df["score"] = df["score"].astype("Float64")
    id = tablevault.write_instance(df, "stories")
    ids.append(id)
    id = tablevault.create_instance(table_name="stories", copy=True, external_edit=True)
    ids.append(id)
    tablevault.write_instance(df, "stories")
    df2, _ = tablevault.get_dataframe("stories")
    # df2.drop(columns=["index"], inplace=True)
    assert df.equals(df2)
    helper.evaluate_operation_logging(ids)


# if __name__ == "__main__":
#     test_write_table_basic()
#     test_write_table_copy()
