import openai
import pandas as pd
from auto_data_table.llm_functions.open_ai_thread import add_open_ai_secret
from tqdm import tqdm


def delete_files(key_file):
    with open(key_file, 'r') as f:
        secret = f.read()
        add_open_ai_secret(secret)
    
    client = openai.OpenAI()
    files = list(client.files.list())
    print(files)
    vector_stores = list(client.beta.vector_stores.list())
    print(vector_stores)
    my_assistants = list(client.beta.assistants.list())
    print(my_assistants)
    for store in tqdm(vector_stores):
        try:
          client.beta.vector_stores.delete(
            vector_store_id=store.id
          )
        except:
            pass
    for f in tqdm(files):
        try:
          client.files.delete(
            file_id=f.id
          )
        except:
          pass
    
    for assistant in tqdm(my_assistants):
        try:
            client.beta.assistants.delete(assistant.id)
        except:
            pass
    
    print(client.beta.vector_stores.list())
    print(client.files.list())
    print(client.beta.assistants.list())
    df = pd.DataFrame(columns=["paper_name", "paper_path"])
    return df

def upload_file_from_table(file_path, key_file):
    with open(key_file, 'r') as f:
        secret = f.read()
        add_open_ai_secret(secret)
    
    client = openai.OpenAI()
        
    file = client.files.create(
                      file=open(file_path, "rb"), purpose="assistants"
                    )
    return (file.id,)
  