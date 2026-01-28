
from ml_vault.database.log_helper import utils
from ml_vault.database import artifact_collection_helper as helper
import time

def add_description_reverse(db, timestamp):
    _, op_info = utils.get_timestamp_info(timestamp)
    name = op_info[1]
    artifact_name = op_info[2]
    key_ = artifact_name + "_" + name + "_" + "DESCRIPT"
    artifacts = db.collection("artifacts")
    artifacts.delete(key_, ignore_missing=True)
    coll = db.collection("description")
    coll.delete(key_, ignore_missing=True)
    edge = db.collection("description_edge")
    edge.delete(str(timestamp), ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    utils.commit_new_timestamp(db, timestamp, "failed")

def create_artifact_reverse(db, timestamp):
    _, op_info = utils.get_timestamp_info(timestamp)
    name = op_info[2]
    collection_type = op_info[2]
    artifacts = db.collection("artifacts")
    artifacts.delete(name, ignore_missing=True)
    coll = db.collection(collection_type)
    coll.delete(name, ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    utils.commit_new_timestamp(db, timestamp, "failed")

def append_artifact_reverse(db, timestamp):
    _, op_info = utils.get_timestamp_info(timestamp)
    name = op_info[1]
    dtype = op_info[2]
    input_artifacts = op_info[3]
    n_dim = op_info[6]
    length =  op_info[7]
    collection = db.collection(dtype)
    collection.delete(name, ignore_missing=True)
    parent = db.collection("parent_edge")
    parent.delete(str(timestamp), ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    edge = db.collection("dependency_edge")
    for art in input_artifacts:
        edge.delete(f'{timestamp}_{art}', ignore_missing=True)
    list_collection = db.collection(f'{dtype}_list')
    art = list_collection.get(name)
    art['n_dim'] = n_dim
    art['length'] = length
    list_collection.update(art, check_rev = True)
    utils.commit_new_timestamp(db, timestamp, "failed")

FUNCTION_REVERSE_MAP = {
    "create_artifact_list": add_description_reverse,
    "append_artifact": append_artifact_reverse,
    "add_description_inner": add_description_reverse,
}

def function_safeguard(fn):
    @functools.wraps(fn)
    def wrapper(db, timestamp, *args, **kwargs):
        function_name = fn.__name__
        try:
            return fn(db, timestamp, *args, **kwargs)
        except:
            fn = constants.FUNCTION_REVERSE_MAP[function_name]
            fn(function_name, timestamp, None)
            timestamp_commit()
            raise
        timestamp_commit()
    return wrapper
    
