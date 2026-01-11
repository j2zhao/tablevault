from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError

def validate_parents_creation(db:StandardDatabase, parents, timestamp) -> bool:
    if len(parents) == 0:
        return True
    artifacts = db.collection("artifacts")
    docs = artifacts.get_many(parents)
    for doc in docs:
        if doc == None:
            return False
        elif doc["timestamp"] > timestamp:
            return False
    return True

def validate_parents(db:StandardDatabase, name, artifact_collection, parents) -> bool:
    if not isinstance(parents, dict):
        return False
    for name, val in parents.items():
        if not isinstance(name, str):
            return False
        if not isinstance(val, dict):
            return False
        if set(val.keys()) != {"start", "end"}:
            return False
        if not isinstance(val["start"], int) or not isinstance(val["end"], int):
            return False
    artifacts = db.collection(artifact_collection)
    doc = artifacts.get({"_key": name})
    for parent in parents:
        if parent not in doc["parents"]:
            return False
    return True

def add_artifact_name(db:StandardDatabase, artifact_name, artifact_type, timestamp):
    artifacts = db.collection("artifacts")
    doc = {
        "_key": artifact_name,
        "artifact_name": artifact_name, 
        "collection": artifact_type,
        "timestamp": timestamp
    }
    try: 
        artifacts.insert(doc)
    except DocumentInsertError as e:
        if artifacts.has(name):
            return False
        else:
            raise
    return True

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


# def add_write_artifact_edge(db:StandardDatabase, artifact_name, artifact_collection, session_name, session_index, timestamp, line_num, create):
#     edge = db.collection("write_artifact")
#     doc = {
#         "line_num": line_num,
#         "code_index": session_index,
#         "timestamp": timestamp, 
#         "_from": f"session_code/{session_name}",
#         "_to": f"{artifact_collection}/{artifact_name}"
#     }
#     edge.insert(doc)

# def add_read_artifact_edge(db:StandardDatabase, artifact_name, artifact_collection, session_name, session_index, timestamp, line_num):
#     edge = db.collection("read_artifact")
#     doc = {
#         "line_num": line_num,
#         "code_index": session_index,
#         "timestamp": timestamp, 
#         "_from": f"session_code/{session_name}",
#         "_to": f"{artifact_collection}/{artifact_name}"
#     }
#     edge.insert(doc)

def add_parent_edge(db:StandardDatabase, artifact_name, artifact_collection, start_position, end_position, parent_name, parent_collection, timestamp, base_artifact):
    edge = db.collection("parent_edge")
    artifacts = db.collection("artifacts")
    if parent_collection == "":
        parent = artifacts.get({"_key": parent_name})
        parent_collection = parent["collection"]
    doc = {
        "timestamp": timestamp, 
        "start_postiion": start_position,
        "end_position": end_position,
        "base_artifact": base_artifact,
        "_from": f"{parent_collection}/{parent_name}",
        "_to": f"{artifact_collection}/{artifact_name}"
    }
    try: 
        edge.insert(doc)
        return True
    except DocumentInsertError as e:
        if edge.has(name):
            return False
        else:
            raise
