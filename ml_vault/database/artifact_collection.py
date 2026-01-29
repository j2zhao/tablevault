# centralize creation

from arango.database import StandardDatabase
from ml_vault.database.log_helper import utils
from ml_vault.database import database_vector_indices as vector_helper
from ml_vault.database.log_helper.operation_management import function_safeguard

def delete_artifact_list_inner(db, timestamp, name, artifact_collection, session_name, session_index):
    aql = r"""
    LET rootId = @rootId

    LET childIds = (
      FOR v, e IN 1..1 OUTBOUND rootId parent_edge
        RETURN DISTINCT v._id
    )
    FOR e IN parent_edge
      FILTER e._from == rootId
      REMOVE e IN parent_edge

    FOR e IN session_parent_edge
      FILTER e._from IN childIds OR e._to IN childIds
      REMOVE e IN session_parent_edge

    FOR e IN dependency_edge
      FILTER e._from IN childIds OR e._to IN childIds
      REMOVE e IN dependency_edge

    FOR v IN @@childCol
      FILTER v._id IN childIds
      REMOVE v IN @@childCol

    UPDATE rootId WITH { deleted: 1 } IN @@rootCol

    LET sid = CONCAT(@sessionCol, "/", @sessionKey)
    LET sess = DOCUMENT(sid)
    FILTER sess != null

    UPSERT { _from: rootId, _to: sid }
      INSERT { _key: @edgeKey, _from: rootId, _to: sid, index: @sessionIndex, timestamp: @ts }
      UPDATE { index: @sessionIndex, timestamp: @ts }
    INTO deleted_session_parent_edge

    RETURN { rootId, session_id: sid }
    """
    child_col = artifact_collection.split("_")[0]
    root_id = f"{artifact_collection}/{name}"
    bind_vars = {
        "rootId": root_id,
        "@rootCol": artifact_collection,
        "@childCol": child_col,
        "sessionCol": "session_list",
        "sessionKey": session_name,
        "sessionIndex": session_index,
        "ts": timestamp,
        "edgeKey": str(timestamp),
    }
    return next(db.aql.execute(aql, bind_vars=bind_vars), {})

def delete_artifact_list(db, name, session_name, session_index, timestamp = None):
    artifacts = db.collection("artifacts")
    artifact = artifacts.get(name)
    if artifact["collection"] in ["session_list", "description"]:
        raise ValueError("Cannot delete session or description items.")
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(db, ["delete_artifact_list", name, artifact["collection"], session_name, session_index], name)
    delete_artifact_list_inner(db, timestamp, name, artifact["collection"], session_name, session_index)
    utils.commit_new_timestamp(db, timestamp)

@function_safeguard
def create_artifact_list(db, timestamp, name, session_name, session_index, artifact, collection_type):
    rev_ = utils.add_artifact_name(db, name, collection_type, timestamp)
    artifact["name"] = name
    artifact["session_name"] = session_name
    artifact["session_index"] = session_index
    artifact["timestamp"] = timestamp
    artifact["n_items"] = 0
    artifact["length"] = 0
    artifact["deleted"] = -1
    rev_ = utils.guarded_upsert(db, name, timestamp, rev_, collection_type, name, {}, artifact)
    if session_name != "":
        doc = {
            "_key": str(timestamp),
            "timestamp": timestamp, 
            "index": session_index,
            "_from": f"session_list/{session_name}",
            "_to": f"{collection_type}/{name}"
        }
        rev_ = utils.guarded_upsert(db, name, timestamp, rev_, "session_parent_edge", str(timestamp), {}, doc)
    utils.commit_new_timestamp(db, timestamp)

def create_file_list(db, name, session_name, session_index): 
    timestamp, _ = utils.get_new_timestamp(db, ["create_artifact_list", name, "file_list", session_name, session_index])
    create_artifact_list(db, timestamp, name, session_name, session_index, {}, "file_list")

def create_document_list(db,  name, session_name, session_index):
    timestamp, _ = utils.get_new_timestamp(db, ["create_artifact_list", name, "document_list", session_name, session_index])
    create_artifact_list(db, timestamp, name, session_name, session_index, {}, "document_list")
   
def create_embedding_list(db, name, session_name, session_index, n_dim):
    timestamp, _ = utils.get_new_timestamp(db, ["create_artifact_list", name, "embedding_list", session_name, session_index, n_dim])
    create_artifact_list(db, timestamp, name, session_name, session_index, {"n_dim": n_dim}, "embedding_list")

def create_record_list(db, name, session_name, session_index, column_names):
    timestamp, _ = utils.get_new_timestamp(db, ["create_artifact_list", name, "record_list", session_name, session_index, column_names])
    create_artifact_list(db, timestamp, name, session_name, session_index, {"column_names": column_names}, "record_list")

@function_safeguard
def append_artifact(
    db,
    timestamp,
    name,
    artifact,
    session_name,
    session_index,
    input_artifacts,
    dtype,
    index,
    start_position,
    end_position,
    rev_):
    artifact["index"] = index
    artifact["start_position"] = start_position
    artifact["end_position"] = end_position
    artifact["name"] = name
    artifact["session_name"] = session_name
    artifact["session_index"] = session_index
    artifact["timestamp"] = timestamp
    artifact_key = f"{name}_{index}"
    rev_ = utils.guarded_upsert(db, name, timestamp, rev_, dtype, artifact_key , {}, artifact)
    doc = {
        "timestamp": timestamp, 
        "start_position": start_position,
        "end_position": end_position,
        "_from": f"{dtype}_list/{name}",
        "_to": f"{dtype}/{artifact_key}"
    }
    rev_ = utils.guarded_upsert(db, name, timestamp, rev_, "parent_edge", str(timestamp), {}, doc)
    if session_name != "":
        doc = {
            "timestamp": timestamp, 
            "index": session_index,
            "_from": f"session_list/{session_name}",
            "_to": f"{dtype}/{artifact_key}"
        }
        rev_ = utils.guarded_upsert(db, name, timestamp, rev_, "session_parent_edge", str(timestamp), {}, doc)
    artifacts = db.collection("artifacts")
    if input_artifacts is not None:
        for art_name in input_artifacts:
            art = artifacts.get({"_key": art_name})
            art_collection = art["collection"]
            doc = {
                "timestamp": timestamp, 
                "start_position": start_position,
                "end_position": end_position,
                "_from": f"{art_collection}/{art_name}",
                "_to": f"{dtype}/{artifact_key}"
            }
            rev_ = utils.guarded_upsert(db, name, timestamp, rev_, "dependency_edge", str(timestamp) + '_' + art_name, {}, doc)
    list_collection = db.collection(f"{dtype}_list")
    artifact_list = list_collection.get(name)
    if artifact_list["n_items"] <= index:
        artifact_list["n_items"] = index + 1
    if artifact_list["length"] < end_position:
        artifact_list["length"] = end_position
    rev_ = utils.guarded_upsert(db, name, timestamp, rev_, f"{dtype}_list", name, {}, artifact_list)    
    utils.commit_new_timestamp(db, timestamp)

def append_file(db:StandardDatabase, 
    name,
    location,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = None):
    timestamp, art = utils.get_new_timestamp(db, [], name)
    file_list = db.collection('file_list').get(name)
    artifact = {
        "location": location,
    }
    if index is None:
        index = file_list['n_items']
        start_position = file_list['length']
        end_position = file_list['length'] + 1

    data = ["append_artifact", name, "file", input_artifacts or {}, session_name, session_index, file_list["n_items"], file_list["length"]]
    utils.update_timestamp_info(db, timestamp, data)
    
    append_artifact(
        db,
        timestamp,
        name,
        artifact,
        session_name,
        session_index,
        input_artifacts,
        "file",
        index,
        start_position,
        end_position,
        art["_rev"])
    

def append_document(
    db,
    name, 
    text,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = None):

    timestamp, art = utils.get_new_timestamp(db, [], name)
    document_list = db.collection('document_list').get(name)
    
    artifact = {
        "text": text,
    }
    if index is None:
        index = document_list['n_items']
        start_position = document_list['length']
        end_position = document_list['length'] + len(text)

    data = ["append_artifact", name, "document",  input_artifacts or {}, session_name, session_index, document_list["n_items"], document_list["length"]]
    utils.update_timestamp_info(db, timestamp,  data)

    append_artifact(
        db,
        timestamp,
        name,
        artifact,
        session_name,
        session_index,
        input_artifacts,
        "document",
        index,
        start_position,
        end_position,
        art["_rev"])
    
# update timestamp
def append_embedding(
    db,
    name, 
    embedding,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = None,
    build_idx=True, 
    index_rebuild_count = 10000):
    timestamp, art = utils.get_new_timestamp(db, [], name)
    embedding_list = db.collection('embedding_list').get(name)
    
    if len(embedding) != embedding_list["n_dim"]:
        raise ValueError(f"Embedding needs to be {embedding_list['n_dim']} size.")
    embedding_name = "embedding_" + str(len(embedding))
    artifact = {
        embedding_name: embedding,
    }
    if index is None:
        index = embedding_list['n_items']
        start_position = embedding_list['length']
        end_position = embedding_list['length'] + 1

    data = ["append_artifact", name, "embedding",  input_artifacts or {}, session_name, session_index, embedding_list["n_items"], embedding_list["length"]]
    utils.update_timestamp_info(db, timestamp,  data)

    append_artifact(
        db,
        timestamp,
        name,
        artifact,
        session_name,
        session_index,
        input_artifacts,
        "embedding",
        index,
        start_position,
        end_position,
        art["_rev"])
    if build_idx:
        total_count, index_count = vector_helper.add_one_vector_count(db, embedding_name)
        if total_count - index_count > index_rebuild_count:
            vector_helper.build_vector_idx(db, embedding_name, len(embedding), parallelism=1, n_lists=2, default_n_probe=1, training_iterations=2)
            vector_helper.update_vector_idx(db, embedding_name)
        

def append_record(
    db,
    name, 
    record,
    session_name,
    session_index,
    index = None,
    start_position = None,
    end_position = None,
    input_artifacts = None):

    timestamp, art = utils.get_new_timestamp(db, [], name)
    record_list = db.collection('record_list').get(name)
    
    if set(record_list["column_names"]) != set(record.keys()):
        raise ValueError("Record not in correct format.")

    artifact = {
        "data": record,
        "data_text": str(record),
        "column_names": list(record.keys()),
    }
    if index is None:
        index = record_list['n_items']
        start_position = record_list['length']
        end_position = record_list['length'] + 1

    data = ["append_artifact", name, "record",  input_artifacts or {}, session_name, session_index, record_list["n_items"], record_list["length"]]
    utils.update_timestamp_info(db, timestamp, data)

    append_artifact(
        db,
        timestamp,
        name,
        artifact,
        session_name,
        session_index,
        input_artifacts,
        "record",
        index,
        start_position,
        end_position,
        art["_rev"])