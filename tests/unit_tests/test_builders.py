import os
import yaml
from tablevault.defintions import constants
from tablevault.builders.load_builder import load_builder
from tablevault.builders.code_builder_type import CodeBuilder
import pandas as pd
from tablevault.builders.utils import table_string

def load_yaml(file_name:str):
    name = os.path.basename(file_name)
    name = name.split(".")[0]
    with open(file_name, "r") as file:
        builder = yaml.safe_load(file)
        builder[constants.BUILDER_NAME] = name
    return builder


def test_load_builder(file_name):
    builder = load_yaml(file_name)
    print(builder)
    builder = load_builder(builder)
    print(builder.dependencies)

def test_table_values():
    data = {
    'x': list(range(10)),       # 0 to 9 for column x
    'y': list(range(10))      # 10 to 19 for column y
    }
    df = pd.DataFrame(data, index=range(10))
    df.index.name = 'index'
    df = df.reset_index()
    test_strs = ['self.x[index]', 
                 'self.{x, y}[index]', 
                 'self.x[y::\'2\':\'4\']',
                 'self.x[index::\'0\':self.index]',
                 'self.x[index::\'0\':self.index, y::\'6\']',
                 'self.x[index::\'0\':"<<self.index>> + 1", y::\'4\']']
    for tstr in test_strs:
        print('BUILDER')
        print(tstr)
        test_val = table_string.TableValue.from_string(tstr)
        print(test_val)
        output = test_val.parse({'self':df}, index = 4)
        print(output)
        print(type(output))


if __name__ == '__main__':
    #test_table_string()
    #test_table_reference()
    file_name = '../test_data/test_data_db/llm_storage/upload_openai.yaml'
    test_load_builder(file_name)
    #test_table_values()