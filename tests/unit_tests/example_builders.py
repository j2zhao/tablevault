import os
import yaml
from tablevault._builders.load_builder import load_builder


def load_yaml(file_name: str):
    name = os.path.basename(file_name)
    name = name.split(".")[0]
    with open(file_name, "r") as file:
        builder = yaml.safe_load(file)
        builder["name"] = name
    return builder


def test_load_builder(file_name):
    builder = load_yaml(file_name)
    builder = load_builder(builder)
    print(builder.arguments)


if __name__ == "__main__":
    # test_table_string()
    # test_table_reference()
    file_name = "../test_data/test_data_db/llm_storage/upload_openai.yaml"
    test_load_builder(file_name)
    # test_table_values()
