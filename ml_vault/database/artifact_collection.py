from ml_vault.database.create_views import ensure_vector_index
from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
import os
import signal
import time
from ml_vault.database import artifact_collection_helper as helper


def create_file_list(db, name, session_name, line_num): 
    timestamp = timestamp_utils.get_new_timestamp(db)
    creation = helper.add_artifact_name(db, name, "file_list", timestamp) # assume success
    if creation:
        collection =  db.collection("file_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "last_timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "locked": False
        }
        collection.insert(doc)

    helper.add_write_artifact_edge(db, name, "file_list", session_name, timestamp, line_num)    
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_document_list(db, name, session_name, line_num):
    timestamp = timestamp_utils.get_new_timestamp(db)
    creation = helper.add_artifact_name(db, name, "document_list", timestamp) # assume success
    if creation:
        collection =  db.collection("document_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "last_timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "locked": False
        }
        collection.insert(doc)

    helper.add_write_artifact_edge(db, name, "document_list", session_name, timestamp, line_num)    
    timestamp_utils.commit_new_timestamp(db, timestamp)
   
def create_embedding_list(db, name, session_name, line_num, n_dim = None, view = True):
    timestamp = timestamp_utils.get_new_timestamp(db)
    creation = helper.add_artifact_name(db, name, "embedding_list", timestamp) # assume success
    if creation:
        collection =  db.collection("embedding_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "last_timestamp": timestamp,
            "n_dim": n_dim,
            "n_items": 0,
            "length": 0,
            "locked": False
        }
        collection.insert(doc)
        if view:
            ensure_vector_index(db, n_dim)
    if not creation and n_dim != None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Can only vector dimension at collection creation time. Consider using a new collection.")
    helper.add_write_artifact_edge(db, name, "embedding_list", session_name, timestamp, line_num)    
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_record_list(db, name, session_name, line_num, column_names):
    timestamp = timestamp_utils.get_new_timestamp(db)
    creation = helper.add_artifact_name(db, name, "record_list", timestamp) # assume success
    if creation:
        check = validate_parents_creation(db, parents, timestamp)
        if not check:
            raise ValueError("Some parent artifact not found.")
        collection =  db.collection("record_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "last_timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "column_names": column_names,
            "locked": False,
        }
        collection.insert(doc)

    if not creation and len(parents) != 0:
        raise ValueError("Can only define parents at collection creation time. Consider using a new collection.")
    helper.add_write_artifact_edge(db, name, "record_list", session_name, timestamp, line_num)    
    timestamp_utils.commit_new_timestamp(db, timestamp)


def append_file(db, 
    name,
    location,
    session_name,
    line,
    index = None, 
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    
    timestamp = timestamp_utils.get_new_timestamp(db)
    check = helper.validate_parent_value(db, parents, timestamp)
    if not check:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Parents field issue. Format needs to be in dictionary: {PARENT_NAME: 'start': INDEX, 'end':INDEX}.")
    
    doc = helper.lock_artifact(db, name, "file_list", timeout, wait_time)
    if doc == None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append file.")
    if index == None:
        index = n_items
    try:
        collection = db.collection("file")
        artifact = {
            "_key": f"{name}_{index}",
            "name": name,
            "index": index,
            "session_name": session_name,
            "line_num": line_num,
            "timestamp": timestamp,
            "position_start": index,
            "position_end": index + 1,
            "location": location,
        }
        collection.insert(artifact)
    except DocumentInsertError:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append file. Possible index error if specified.")
    for parent in parents:
        parent_edge = helper.add_parent_edge(db, name, "file", parents['parent']['start_position'], parents['parent']['end_position'], parent, timestamp)
        if false:
            raise ValueError("Could not add parrent edge. Possible name error.")
    timestamp_utils.commit_new_timestamp(db, timestamp)

    
    
def append_document(
    name, 
    text,
    session_name,
    line_num,
    index = None, 
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    timestamp = timestamp_utils.get_new_timestamp(db)
    check = helper.validate_parent_value(db, parents, timestamp)
    if not check:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Parents field issue. Format needs to be in dictionary: {PARENT_NAME: 'start': INDEX, 'end':INDEX}.")
    
    doc = helper.lock_artifact(db, name, "document_list", timeout, wait_time)
    if doc == None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append document.")
    if index == None:
        index = n_items
    try:
        collection = db.collection("document")
        artifact = {
            "_key": f"{name}_{index}",
            "name": name,
            "index": index,
            "session_name": session_name,
            "line_num": line_num,
            "timestamp": timestamp,
            "position_start": index,
            "position_end": index + 1,
            "text": text,
        }
        collection.insert(artifact)
    except DocumentInsertError:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append document. Possible index error if specified.")
    for parent in parents:
        parent_edge = helper.add_parent_edge(db, name, "document", parents['parent']['start_position'], parents['parent']['end_position'], parent, timestamp)
        if false:
            raise ValueError("Could not add parrent edge. Possible name error.")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def append_embedding(
    name, 
    embedding,
    session_name,
    line_num,
    index = None, 
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    timestamp = timestamp_utils.get_new_timestamp(db)
    check = helper.validate_parent_value(db, parents, timestamp)
    if not check:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Parents field issue. Format needs to be in dictionary: {PARENT_NAME: 'start': INDEX, 'end':INDEX}.")
    
    doc = helper.lock_artifact(db, name, "embedding_list", timeout, wait_time)
    if doc == None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append embedding.")
    if len(embedding) != doc["n_dim"]:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Embedding length doesn't match collection.")
    
    embedding_name = "embedding_" + str(doc["n_dim"])
    if index == None:
        index = n_items
    try:
        collection = db.collection("embedding")
        artifact = {
            "_key": f"{name}_{index}",
            "name": name,
            "index": index,
            "session_name": session_name,
            "line_num": line_num,
            "timestamp": timestamp,
            "position_start": index,
            "position_end": index + 1,
            embedding_name: embedding,
        }
        collection.insert(artifact)
    except DocumentInsertError:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append embedding. Possible index error if specified.")
    for parent in parents:
        parent_edge = helper.add_parent_edge(db, name, "document", parents['parent']['start_position'], parents['parent']['end_position'], parent, timestamp)
        if false:
            raise ValueError("Could not add parrent edge. Possible name error.")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def append_record(
    name, 
    record,
    session_name,
    line_num,
    index = None, 
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    timestamp = timestamp_utils.get_new_timestamp(db)
    check = helper.validate_parent_value(db, parents, timestamp)
    if not check:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Parents field issue. Format needs to be in dictionary: {PARENT_NAME: 'start': INDEX, 'end':INDEX}.")
    
    doc = helper.lock_artifact(db, name, "document_list", timeout, wait_time)
    if doc == None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append document.")
    check = helper.validate_record(record, doc["column_names"])
    if not check:
        raise ValueError("Record not in correct format.")
    if index == None:
        index = n_items
    try:
        collection = db.collection("document")
        artifact = {
            "_key": f"{name}_{index}",
            "name": name,
            "index": index,
            "session_name": session_name,
            "line_num": line_num,
            "timestamp": timestamp,
            "position_start": index,
            "position_end": index + 1,
            "data": record,
            "data_text": str(record),
        }
        collection.insert(artifact)
    except DocumentInsertError:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append document. Possible index error if specified.")
    for parent in parents:
        parent_edge = helper.add_parent_edge(db, name, "document", parents['parent']['start_position'], parents['parent']['end_position'], parent, timestamp)
        if false:
            raise ValueError("Could not add parrent edge. Possible name error.")
    timestamp_utils.commit_new_timestamp(db, timestamp)



