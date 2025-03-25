import os
import yaml
from tablevault.defintions import constants
from tablevault.prompts.load_prompt import load_prompt
from tablevault.prompts.table_string import DataTable, TableReference, parse_table_string
        
def load_yaml(file_name:str):
    name = os.path.basename(file_name)
    name = name.split(".")[0]
    with open(file_name, "r") as file:
        prompt = yaml.safe_load(file)
        prompt[constants.PNAME] = name
    return prompt


def test_load_prompt(file_name):
    prompt = load_yaml(file_name)
    print(prompt)
    prompt = load_prompt(prompt)
    print(prompt)

def test_data_table():
    datatable = DataTable('stories.paper_name')
    print(datatable)

def test_table_reference():
    table_str = "<<stories.paper_path[paper_name:self.paper_name]>>"
    table_ref = TableReference(table_str)
    print(table_ref)

def test_table_string():
    table_str = "<<stories.paper_path[paper_name:self.paper_name]>>"
    table_str = parse_table_string(None, table_str)
    print(table_str)

if __name__ == '__main__':
    #test_table_string()
    #test_table_reference()
    file_name = '../test_data/test_data_db/stories/gen_stories.yaml'
    test_load_prompt(file_name)