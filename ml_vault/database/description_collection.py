from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper
import os
import signal
import time

def add_description_edge(db:StandardDatabase, description_key, artifact_name, artifact_collection, timestamp):
    edge = db.collection("description_edge")
    doc = {
        "timestamp": timestamp, 
        "_from": f"{artifact_collection}/{artifact_name}",
        "_to": description_key
    }
    d_edge.insert(doc)


def add_description(db, artifact_name, session_name,  description, embedding):
    artifacts = db.collection("artifacts")
    art = artifacts.get({"_key": artifact_name})
    artifact_collection = art["collection"]
    
    description = db.collection("description")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "artifact_name": artifact_name, 
        "collection": artifact_collection,
        "timestamp": timestamp,
        "text": description,
        "embedding": embedding,
    }
    meta = description.insert(doc)
    add_description_edge(db, meta["_key"], artifact_name, artifact_collection, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    

