from ml_vault.database.log_helper import utils
import functools
import sys


def add_description_reverse(db, timestamp):
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
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
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
    name = op_info[1]
    collection_type = op_info[2]
    artifacts = db.collection("artifacts")
    artifacts.delete(name, ignore_missing=True)
    coll = db.collection(collection_type)
    coll.delete(name, ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    utils.commit_new_timestamp(db, timestamp, "failed")


def append_artifact_reverse(db, timestamp):
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
    name = op_info[1]
    dtype = op_info[2]
    input_artifacts = op_info[3]
    n_items = op_info[6]
    length = op_info[7]
    collection = db.collection(dtype)
    collection.delete(f"{name}_{n_items}", ignore_missing=True)
    parent = db.collection("parent_edge")
    parent.delete(str(timestamp), ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    edge = db.collection("dependency_edge")
    for art in input_artifacts:
        edge.delete(f"{timestamp}_{art}", ignore_missing=True)
    list_collection = db.collection(f"{dtype}_list")
    art = list_collection.get(name)
    art["n_items"] = n_items
    art["length"] = length
    list_collection.update(art)
    utils.commit_new_timestamp(db, timestamp, "reverse_failed")


FUNCTION_REVERSE_MAP = {
    "create_artifact_list": create_artifact_reverse,
    "append_artifact": append_artifact_reverse,
    "add_description_inner": add_description_reverse,
}


def function_safeguard(fn):
    @functools.wraps(fn)
    def wrapper(db, timestamp, *args, **kwargs):
        function_name = fn.__name__
        try:
            return fn(db, timestamp, *args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()

            reverse_fn = FUNCTION_REVERSE_MAP.get(function_name)
            if reverse_fn is not None:
                try:
                    reverse_fn(db, timestamp)
                except Exception:
                    raise

            raise exc_info[1].with_traceback(exc_info[2])

    return wrapper
