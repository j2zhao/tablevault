from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper
import os
import signal
import time

def add_description_edge(db:StandardDatabase, description_id, artifact_name, artifact_collection, timestamp):
    edge = db.collection("description_edge")
    doc = {
        "timestamp": timestamp, 
        "_from": f"{artifact_collection}/{artifact_name}",
        "_to": description_id
    }
    try: 
        d_edge.insert(doc)
        return True
    except DocumentInsertError as e:
        if d_edge.has(description_id):
            return False
        else:
            raise


def add_description(db, description, embedding, artifact_name, artifact_collection, start_position, end_position):
    description = db.collection("description")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "timestamp": timestamp,
        "text": description,
        "embedding": embedding,
    }
    meta = description.insert(doc)
    add_description_edge(db, meta["_id"], artifact_name, artifact_collection, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    

