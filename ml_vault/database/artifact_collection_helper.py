from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError
import time

def delete_artifact_name(db:StandardDatabase, artifact_name):
    artifacts = db.collection("artifacts")
    artifacts.delete(artifact_name, ignore_missing=True)

def add_artifact_name(db:StandardDatabase, artifact_name, artifact_type, timestamp):
    artifacts = db.collection("artifacts")
    doc = {
        "_key": artifact_name,
        "name": artifact_name, 
        "collection": artifact_type,
        "timestamp": timestamp
    }
    try: 
        artifacts.insert(doc)
    except DocumentInsertError:
        return False
    return True

def lock_artifact(db:StandardDatabase, name, collection_name, timestamp, timeout, wait_time):
    start = time.time()
    end = time.time()
    collection =  db.collection(collection_name)
    while end - start < timeout:
        doc = collection.get({"_key":name})
        if doc["locked"] == -1:
            doc["locked"] = timestamp
            try:
                collection.update(doc, check_rev=True, merge=False)
                return doc
            except Exception:
                pass
        time.sleep(wait_time)
        end = time.time()
    return None


def unlock_artifact(db:StandardDatabase, name, collection_name, index = None, length= None):
    collection =  db.collection(collection_name)
    doc = collection.get({"_key":name})
    doc["locked"] = -1
    if index is not None:
        if doc["n_items"] < index + 1:
            doc["n_items"] = index + 1
    if length is not None:
        if doc["length"] < length:
            doc["length"] = length
    collection.update(doc, merge=False)


def add_parent_edge(db:StandardDatabase, artifact_key, parent_name, collection, start_position, end_position, timestamp):
    edge = db.collection("parent_edge")
    name_key = f"{artifact_key}_{parent_name}_{start_position}_{end_position}"
    doc = {
        "_key": name_key,
        "timestamp": timestamp, 
        "start_position": start_position,
        "end_position": end_position,
        "_from": f"{collection}_list/{parent_name}",
        "_to": f"{collection}/{artifact_key}"
    }
    try: 
        edge.insert(doc)
        return True
    except DocumentInsertError:
        return False

def delete_parent_edge(db, artifact_key, parent_name, start_position, end_position):
    edge = db.collection("parent_edge")
    name_key = f"{artifact_key}_{parent_name}_{start_position}_{end_position}"
    edge.delete(name_key, ignore_missing=True)

def add_session_parent_edge(db, artifact_key, artifact_collection, session_name, session_index, timestamp):
    edge = db.collection("session_parent_edge")
    doc = {
        "_key": artifact_key,
        "timestamp": timestamp, 
        "index": session_index,
        "_from": f"session_list/{session_name}",
        "_to": f"{artifact_collection}/{artifact_key}"
    }
    edge.insert(doc)
    return True
    

def delete_session_parent_edge(db, artifact_key):
    edge = db.collection("session_parent_edge")
    edge.delete(artifact_key, ignore_missing=True)


def add_dependency_edge(db:StandardDatabase, artifact_key, artifact_collection, parent_name, parent_collection, start_position, end_position, timestamp):
    artifacts = db.collection("artifacts")
    if parent_collection == "":
        parent = artifacts.get({"_key": parent_name})
        parent_collection = parent["collection"]
    
    edge = db.collection("dependency_edge")
    name_key = f"{artifact_key}_{parent_name}_{start_position}_{end_position}"
    doc = {
        "_key": name_key,
        "timestamp": timestamp, 
        "start_position": start_position,
        "end_position": end_position,
        "_from": f"{parent_collection}/{parent_name}",
        "_to": f"{artifact_collection}/{artifact_key}"
    }
    try: 
        edge.insert(doc)
        return True
    except DocumentInsertError:
        return False

def delete_dependency_edge(db, artifact_key, parent_name, start_position, end_position):
    edge = db.collection("parent_edge")
    name_key = f"{artifact_key}_{parent_name}_{start_position}_{end_position}"
    edge.delete(name_key, ignore_missing=True)