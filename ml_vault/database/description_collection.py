# change description format

from arango.database import StandardDatabase
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper

def add_description_edge(db:StandardDatabase, description_key, artifact_name, artifact_collection, timestamp):
    edge = db.collection("description_edge")
    doc = {
        "timestamp": timestamp, 
        "_from": f"{artifact_collection}/{artifact_name}",
        "_to": f"description/{description_key}"
    }
    edge.insert(doc)


def add_description(db, name, artifact_name, session_name, session_index, description, embedding):
    artifacts = db.collection("artifacts")
    art = artifacts.get({"_key": artifact_name})
    artifact_collection = art["collection"]
    key_ = artifact_name + "_" + name + "_" + "DESCRIPT"
    timestamp = timestamp_utils.get_new_timestamp(db, ["add_description", artifact_name, session_name, description, embedding])
    helper.add_artifact_name(db, key_, "description", timestamp)
    descript = db.collection("description")
    doc = {
        "_key": key_,
        "name": key_,
        "artifact_name": artifact_name,
        "session_name": session_name, 
        "session_index": session_index,
        "collection": artifact_collection,
        "timestamp": timestamp,
        "text": description,
        "embedding": embedding,
    }
    meta = descript.insert(doc)
    add_description_edge(db, meta["_key"], artifact_name, artifact_collection, timestamp)
    helper.add_session_parent_edge(db, meta["_key"], "description", session_name, session_index, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    

