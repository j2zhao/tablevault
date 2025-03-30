import os
import yaml
from tablevault.defintions import constants
from tablevault.prompts.load_prompt import load_prompt
from tablevault.prompts.code_execution_ptype import CodePrompt
import pandas as pd
from tablevault.prompts.utils import table_string

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
    print(prompt.dependencies)

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
        print('PROMPT')
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
    test_load_prompt(file_name)
    #test_table_values()