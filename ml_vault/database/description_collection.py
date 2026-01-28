# change description format

from arango.database import StandardDatabase
from ml_vault.database.log_helper import utils
from ml_vault.database import artifact_collection_helper as helper
from ml_vault.database.operation_management import function_safeguard

@function_safeguard
def add_description_inner(db, timestamp, name, artifact_name, session_name, session_index, description, embedding):
    artifacts = db.collection("artifacts")
    art = artifacts.get({"_key": artifact_name})
    artifact_collection = art["collection"]
    key_ = artifact_name + "_" + name + "_" + "DESCRIPT"
    guard_rev = helper.add_artifact_name(db, key_, "description", timestamp)
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
    guard_rev = utils.guarded_upsert(db, key_, timestamp, guard_rev, "description", key_, {}, doc)
    doc = {
        "_key": str(timestamp),
        "timestamp": timestamp, 
        "_from": f"{artifact_collection}/{artifact_name}",
        "_to": f"description/{key_}"
    }
    guard_rev = utils.guarded_upsert(db, key_, timestamp, guard_rev, "description_edge", str(timestamp), {}, doc)
    doc = {
        "_key": str(timestamp),
        "timestamp": timestamp, 
        "index": session_index,
        "_from": f"session_list/{session_name}",
        "_to": f"description/{key_}"
    }
    utils.guarded_upsert(db, key_, timestamp, guard_rev, "session_parent_edge", str(timestamp), {}, doc)
    

def add_description(db, name, artifact_name, session_name, session_index, description, embedding):
    timestamp, _ = utils.get_new_timestamp(db, ["add_description", name, artifact_name, session_name, session_index])
    add_description_inner(db, timestamp, name, artifact_name, session_name, session_index, description, embedding)
