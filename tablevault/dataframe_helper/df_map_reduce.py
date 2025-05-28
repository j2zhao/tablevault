import pickle
from typing import Any
import os
from tablevault.helper.utils import gen_tv_id
from tablevault.dataframe_helper import table_operations

def write_df_entry(value:Any, 
                   index:int, 
                   col_name:str,
                   instance_id:str,
                   table_name:str,
                   db_dir:str):
    
    file_name = gen_tv_id() + '.df.pkl'
    file_name = os.path.join(db_dir, table_name, instance_id, file_name)
    pkf = {"value": value, "index": index, "col_name": col_name}
    with open(file_name, "w") as f:
        pickle.dump(pkf, f)

def make_df(instance_id:str,table_name:str,db_dir:str):
    file_dir = os.path.join(db_dir, table_name, instance_id)
    df = table_operations.get_table(instance_id, table_name, db_dir,artifact_dir=False, get_index=False)
    has_pkf = False
    for file_name in os.listdir(file_dir):
        if file_name.endswith(".df.pkl"):
            has_pkf = True
            file_path = os.path.join(file_dir, file_name)
            with open(file_path, "r") as f:
                pkf = pickle.load(f)
                df.at[pkf["index"], pkf["col_name"]] = pkf["value"]
    if has_pkf:
        table_operations.write_table(df, instance_id, table_name, db_dir)