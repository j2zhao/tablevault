from ml_vault.database.create_views import ensure_vector_index
from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
import os
import signal
import time

def add_file_list(db, name):
    collection =  db.collection("file_list")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_key": name,
        "name": name,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "n_items": 0,
        "length": 0,
        "locked": False
    }
    try:
        collection.insert(doc)
    except DocumentInsertError as e:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        if collection.has(name):
            raise ValueError(f"{name} already exists")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def add_document(db, name):
    collection =  db.collection("document")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_key": name,
        "name": name,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "n_items": 0,
        "length": 0,
        "locked": False
    }
    try:
        collection.insert(doc)
    except DocumentInsertError as e:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        if collection.has(name):
            raise ValueError(f"{name} already exists")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def add_embeddings_list(db, name, collection_name, n_dim, view = True):
    collection =  db.collection("document")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_key": name,
        "name": name,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "n_items": 0,
        "length": 0,
        "column_names": column_names,
        "locked": False
    }
    try:
        collection.insert(doc)
    except DocumentInsertError as e:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        if collection.has(name):
            raise ValueError(f"{name} already exists")
    ensure_vector_index(db, n_dim)
    timestamp_utils.commit_new_timestamp(db, timestamp)

def add_record_list(db, name, collection_name, column_names):
    collection =  db.collection("document")
    timestamp = timestamp_utils.get_new_timestamp(db)
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
    try:
        collection.insert(doc)
    except DocumentInsertError as e:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        if collection.has(name):
            raise ValueError(f"{name} already exists")
    timestamp_utils.commit_new_timestamp(db, timestamp)

def lock_artifact(db:StandardDatabase, name, collection_name, timeout, wait_time):
    start = time.time()
    end = time.time()
    collection =  db.collection(collection_name)
    while end - start < timeout:
        doc = collection.get({"_key":name})
        if not doc["locked"]:
            doc["locked"] = True
            try:
                collection.update(doc, check_rev=True)
                return doc
            except:
                pass
        time.sleep(wait_time)
    return None

def add_write_artifact_edge(db:StandardDatabase, artifact_name, artifact_type, session_name, timestamp, line_num):
    edge = db.collection("write_artifact")
    doc = {
        "line_num": line_num,
        "timestamp": timestamp, 
        "_from": f"session_code/{session_name}",
        "_to": f"artifact_type/{artifact_name}"
    }
    edge.insert(doc)

def add_dependent_edge(db:StandardDatabase, artifact_name, artifact_type, parent_name, timestamp):
    edge = db.collection("dependent_edge")
    doc = {
        "timestamp": timestamp, 
        "_from": f"session_code/{session_name}",
        "_to": f"artifact_type/{artifact_name}"
    }
    edge.insert(doc)




def append_file_list(db, 
    name, 
    collection_name, 
    file, 
    index = None, 
    timeout = 60, 
    wait_time = 0.1):
    doc = lock_artifact(db, name, collection_name, timeout, wait_time)
    if doc == None:
        raise ValueError("Could not append file.")
    if index == None:
        index = n_items
    timestamp = timestamp_utils.get_new_timestamp(db)
    artifact = {
        "_key": f"{name}_{index}",
        "name": name,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "n_items": 0,
        "length": 0,
        "column_names": column_names,
        "locked": False
    }

def append_document():
    pass

def append_embeddings():
    pass

def append_record_list():
    pass



