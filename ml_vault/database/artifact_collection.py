# centralize creation

from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper
from ml_vault.database import database_vector_indices as vector_helper

def create_file_list(db, name, session_name, session_index): 
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_file_list", name, session_name])
    creation = helper.add_artifact_name(db, name, "file_list", timestamp)
    if creation:
        collection =  db.collection("file_list")
        doc = {
            "_key": name,
            "name": name,
            "session_name": session_name,
            "session_index": session_index,
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
        raise ValueError("Name Duplicated. Need to be unique over all artifacts.")
    helper.add_session_parent_edge(db, name, "file_list", session_name, session_index, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_document_list(db, name, session_name, session_index):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_document_list", name, session_name])
    creation = helper.add_artifact_name(db, name, "document_list", timestamp) # assume success
    if creation:
        collection =  db.collection("document_list")
        doc = {
            "_key": name,
            "name": name,
            "session_name": session_name,
            "session_index": session_index,
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
        raise ValueError("Name Duplicated. Need to be unique over all artifacts.")
    helper.add_session_parent_edge(db, name, "document_list", session_name, session_index, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)
   
def create_embedding_list(db, name, session_name, session_index, n_dim, view = True):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_embedding_list", name, session_name])
    creation = helper.add_artifact_name(db, name, "embedding_list", timestamp) # assume success
    if creation:
        collection =  db.collection("embedding_list")
        doc = {
            "_key": name,
            "name": name,
            "session_name": session_name,
            "session_index": session_index,
            "timestamp": timestamp,
            "n_dim": n_dim,
            "n_items": 0,
            "length": 0,
            "locked": -1
        }
        collection.insert(doc)
    elif not creation and n_dim is not None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Can only vector dimension at collection creation time. Consider using a new collection.")
    else:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Name Duplicated. Need to be unique over all artifacts.")
    helper.add_session_parent_edge(db, name, "embedding_list", session_name, session_index, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)

def create_record_list(db, name, session_name, session_index, column_names):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_record_list", name, session_name, column_names])
    creation = helper.add_artifact_name(db, name, "record_list", timestamp) # assume success
    if creation:
        collection =  db.collection("record_list")
        doc = {
            "_key": name,
            "name": name,
            "session_name": session_name,
            "session_index": session_index,
            "timestamp": timestamp,
            "n_items": 0,
            "length": 0,
            "column_names": column_names,
            "locked": -1,
        }
        collection.insert(doc)
    else:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Name Duplicated. Need to be unique over all artifacts.")
    
    helper.add_session_parent_edge(db, name, "record_list", session_name, session_index, timestamp)
    timestamp_utils.commit_new_timestamp(db, timestamp)

# add session index later
def _append_artifact(
    db,
    name,
    session_name,
    session_index,
    input_artifacts,
    artifact,
    dtype,
    length,
    index = None,
    start_position = None,
    end_position = None,
    timeout = 60, 
    wait_time = 0.1):
    timestamp = timestamp_utils.get_new_timestamp(db, [f"append_{dtype}", name, session_name, input_artifacts])
    art = db.collection("artifacts")
    doc = art.get(name)
    if doc == None:
        raise ValueError("Artifact doesn't Exist")
    doc = helper.lock_artifact(db, name, f"{dtype}_list", timestamp, timeout, wait_time)
    if doc is None:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not append item.")
    if index is None:
        index = doc["n_items"]
    if start_position is None:
        start_position = doc["length"]
        end_position = doc["length"] + length
    artifact["index"] = index
    artifact["start_position"] = start_position
    artifact["end_position"] = end_position
    artifact["_key"] = f"{name}_{index}"
    artifact["name"] = name
    artifact["session_name"] = session_name
    artifact["session_index"] = session_index
    artifact["timestamp"] = timestamp
    print(artifact)
    collection = db.collection(dtype)
    collection.insert(artifact)
    #raise ValueError()
    # try:
    #     collection = db.collection(dtype)
    #     collection.insert(artifact)
    # except DocumentInsertError:
    #     helper.unlock_artifact(db, name, f"{dtype}_list")
    #     timestamp_utils.commit_new_timestamp(db, timestamp)
    #     raise ValueError("Could not append item. Possible index error if specified.")
    success = helper.add_parent_edge(db, f"{name}_{index}", name,  dtype, start_position, end_position, timestamp)
    if not success:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add base parent edge.")
    success = helper.add_session_parent_edge(db, f"{name}_{index}", dtype, session_name, session_index, timestamp)
    if not success:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.delete_parent_edge(db, f"{name}_{index}", name, start_position, end_position)
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add base parent edge.")
    inputs_success = True
    for parent in input_artifacts:
        success = helper.add_dependency_edge(db, f"{name}_{index}", dtype, parent, "", input_artifacts[parent][0], input_artifacts[parent][1], timestamp)
        if not success:
            inputs_success = False
            break
    if not inputs_success:
        collection.delete(f"{name}_{index}", ignore_missing=True)
        helper.delete_parent_edge(db, f"{name}_{index}", name, start_position, end_position)
        helper.delete_session_parent_edge(db, f"{name}_{index}")
        for parent in input_artifacts:
            helper.delete_dependency_edge(db, f"{name}_{index}", parent, input_artifacts[parent][0], input_artifacts[parent][1])
        helper.unlock_artifact(db, name, f"{dtype}_list")
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Could not add parent edge. Possible name error.")
    helper.unlock_artifact(db, name, f"{dtype}_list", index, end_position)
    timestamp_utils.commit_new_timestamp(db, timestamp)
        
def append_file(db:StandardDatabase, 
    name,
    location,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = {},
    timeout = 60, 
    wait_time = 0.1):
    
    artifact = {
        "location": location,
    }
    _append_artifact(
        db,
        name,
        session_name,
        session_index,
        input_artifacts,
        artifact,
        "file",
        1,
        index,
        start_position,
        end_position,
        timeout, 
        wait_time)
    

def append_document(
    db,
    name, 
    text,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = {},
    timeout = 60, 
    wait_time = 0.1):


    artifact = {
        "text": text,
    }
    _append_artifact(
        db,
        name,
        session_name,
        session_index,
        input_artifacts,
        artifact,
        "document",
        len(text),
        index,
        start_position,
        end_position,
        timeout, 
        wait_time)
    
def append_embedding(
    db,
    name, 
    embedding,
    session_name,
    session_index,
    index = None, 
    start_position = None,
    end_position = None,
    input_artifacts = {},
    build_idx=True, 
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
        session_index,
        input_artifacts,
        artifact,
        "embedding",
        1,
        index,
        start_position,
        end_position,
        timeout, 
        wait_time)
    if build_idx:
        print("hello")
        total_count, index_count = vector_helper.add_one_vector_count(db, embedding_name)
        print((total_count, index_count))
        
        # if total_count - index_count > 10000:
        #     vector_helper.build_vector_idx(db, embedding_name, len(embedding))
        #     vector_helper.update_vector_idx(db, embedding_name)

        if total_count - index_count > 10:
            print("HELLLO")
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
    input_artifacts = {},
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
    if index is None:
        end_position = None
    else:
        end_position = index + 1
    _append_artifact(
        db,
        name,
        session_name,
        session_index,
        input_artifacts,
        artifact,
        "record",
        1,
        index,
        index,
        end_position,
        timeout, 
        wait_time)