from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
import os
import signal
import time

def add_description_edge(description_key, item_name, collection, timestamp):
    edge = db.collection("description_edge")
    doc = {
        "timestamp": timestamp, 
        "_from": f"collection/{item_name}",
        "_to": f"description/{description_key}"
    }
    edge.insert(doc)

def add_description(db, description, embedding, session_name, item_name, collection, start_position, end_position, dtype):
    description = db.collection("description")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "session_name": session_name,
        "item_name": item_name, 
        "timestamp": timestamp,
        "collection": collection,
        "text": description,
        "type": dtype,
        "start_position": start_position,
        "end_position": end_position,
        "embedding": embedding,
    }
    meta = description.insert(doc)
    add_description_edge(meta["_key"], item_name, collection, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    

