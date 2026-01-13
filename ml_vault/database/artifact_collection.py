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
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_file_list", name, session])
    creation = helper.add_artifact_name(db, name, "file_list", timestamp)
    if creation:
        collection =  db.collection("file_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "locked": -1
        }
        try:
            collection.insert(doc)
        except Exception as e:
            helper.delete_artifact_name(db, name)
            timestamp_utils.commit_new_timestamp(db, timestamp)
            raise e
    else:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        return False
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_document_list(db, name, session_name, line_num):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_document_list", name, session_name, line_num])
    creation = helper.add_artifact_name(db, name, "document_list", timestamp) # assume success
    if creation:
        collection =  db.collection("document_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "locked": -1
        }
        try:
            collection.insert(doc)
        except Exception as e:
            helper.delete_artifact_name(db, name)
            timestamp_utils.commit_new_timestamp(db, timestamp)
            raise e
    timestamp_utils.commit_new_timestamp(db, timestamp)
   
def create_embedding_list(db, name, session_name, line_num, n_dim = None, view = True):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_embedding_list", name, session_name, line_num])
    creation = helper.add_artifact_name(db, name, "embedding_list", timestamp) # assume success
    if creation:
        collection =  db.collection("embedding_list")
        doc = {
            "_key": name,
            "name": name,
            "timestamp": timestamp,
            "n_dim": n_dim,
            "n_items": 0,
            "length": 0,
            "locked": -1
        }
        collection.insert(doc)
        if view:
            ensure_vector_index(db, n_dim)
    if not creation and n_dim != None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Can only vector dimension at collection creation time. Consider using a new collection.")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_record_list(db, name, session_name, line_num, column_name):
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
            "n_items": 0,
            "length": 0,
            "column_name": column_name,
            "locked": -1,
        }
        collection.insert(doc)

    if not creation and len(parents) != 0:
        raise ValueError("Can only define parents at collection creation time. Consider using a new collection.")
    timestamp_utils.commit_new_timestamp(db, timestamp)


def _append_artifact(
    db,
    name,
    session_name,
    parents,
    artifact,
    dtype,
    length,
    index = None,
    position_start = None,
    position_end = None,
    timeout = 60, 
    wait_time = 0.1):
    timestamp = timestamp_utils.get_new_timestamp(db, [f"append_{dtype}", name, session_name, parents])
    doc = helper.lock_artifact(db, name, f"{dtype}_list", timestamp, timeout, wait_time)
    if doc == None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append item.")
    if index == None:
        index = doc["n_items"]
    if position_start == None:
        position_start = doc["length"]
        position_end = doc["length"] + length
    artifact["index"] = index
    artifact["position_start"] = position_start
    artifact["position_end"] = position_end
    artifact["_key"] = f"{name}_{index}"
    artifact["name"] = name
    artifact["session_name"] = session_name
    artifact["timestamp"] = timestamp
    try:
        collection = db.collection(dtype)
        collection.insert(artifact)
    except DocumentInsertError:
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append item. Possible index error if specified.")
    success = helper.add_parent_edge(db, f"{name}_{index}", dtype, name, f"{dtype}_list", position_start, position_end,  True, timestamp)
    if not success:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add base parent edge.")
    success = helper.add_session_parent_edge(db, f"{name}_{index}", dtype, session_name, timestamp)
    if not success:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.delete_parent_edge(db, f"{name}_{index}", name, f"{dtype}_list", position_start, position_end)
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add base parent edge.")
    parents_success = True
    for parent in parents:
        success = helper.add_parent_edge(db, f"{name}_{index}", dtype, parent, "", parents['parent']['start_position'], parents['parent']['end_position'], False, timestamp)
        if not success:
            parents_sucess = False
            break
    if not parents_sucess:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.delete_parent_edge(db, f"{name}_{index}", name, f"{dtype}_list", start_position, end_position)
        helper.delete_session_parent_edge(db, f"{name}_{index}")
        for parent in parents:
            helper.delete_parent_edge(db, f"{name}_{index}", parent, "",  parents['parent']['start_position'], parents['parent']['end_position'])
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add parent edge. Possible name error.")
    helper.unlock_artifact(db, name, f"{dtype}_list", index, end_position)
    timestamp_utils.commit_new_timestamp(db, timestamp)
        
def append_file(db:StandardDatabase, 
    name,
    location,
    session_name,
    index = None, 
    position_start = None,
    position_end = None,
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    
    artifact = {
        "location": location,
    }
    _append_artifact(
        db,
        name,
        session_name,
        parents,
        artifact,
        "file",
        1,
        index,
        position_start,
        position_end,
        timeout, 
        wait_time)
    

def append_document(
    db,
    name, 
    text,
    session_name,
    index = None, 
    position_start = None,
    position_end = None,
    parents = {},
    timeout = 60, 
    wait_time = 0.1):


    artifact = {
        "text": text,
    }
    _append_artifact(
        db,
        name,
        session_name,
        parents,
        artifact,
        "document",
        len(text),
        index,
        position_start,
        position_end,
        timeout, 
        wait_time)
    
def append_embedding(
    db,
    name, 
    embedding,
    session_name,
    index = None, 
    position_start = None,
    position_end = None,
    parents = {},
    timeout = 60, 
    wait_time = 0.1):
    embedding_name = "embedding_" + str(len(embedding))

    artifact = {
        embedding_name: embedding,
    }
    _append_artifact(
        db,
        name,
        session_name,
        parents,
        artifact,
        "embedding",
        1,
        index,
        position_start,
        position_end,
        timeout, 
        wait_time)

    # TODO: ADD EMBEDDING VIEW

def append_record(
    db,
    name, 
    record,
    session_name,
    index = None, 
    parents = {},
    timeout = 60, 
    wait_time = 0.1):

    doc = db.collection("record_list").get(name)
    if not isinstance(record, dict) or set(doc["column_names"]) != set(record.keys()):
        raise ValueError("Record not in correct format.")

    artifact = {
        "data": record,
        "data_text": str(record),
        "column_names": list(record.keys()),
    }
    _append_artifact(
        db,
        name,
        session_name,
        parents,
        artifact,
        "record",
        1,
        index,
        position_start,
        position_end,
        timeout, 
        wait_time)