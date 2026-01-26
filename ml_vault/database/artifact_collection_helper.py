from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError
import time
from ml_vault.database.log_helper import timestamp_utils


def add_parent_edge(db:StandardDatabase, artifact_key, parent_name, collection, start_position, end_position, timestamp):
    edge = db.collection("parent_edge")
    name_key = f"{artifact_key}_{parent_name}_{start_position}_{end_position}"
    doc = {
        "_key": str(timestamp),
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

def add_session_parent_edge(db, artifact_key, artifact_collection, session_name, session_index, timestamp):
    edge = db.collection("session_parent_edge")
    doc = {
        "_key": str(timestamp),
        "timestamp": timestamp, 
        "index": session_index,
        "_from": f"session_list/{session_name}",
        "_to": f"{artifact_collection}/{artifact_key}"
    }
    edge.insert(doc)
    return True


def add_dependency_edge(db:StandardDatabase, artifact_key, artifact_collection, parent_name, parent_collection, start_position, end_position, timestamp):
    artifacts = db.collection("artifacts")
    if parent_collection == "":
        parent = artifacts.get({"_key": parent_name})
        parent_collection = parent["collection"]
    
    edge = db.collection("dependency_edge")
    doc = {
        "_key": str(timestamp) + '_' + parent_name,
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
