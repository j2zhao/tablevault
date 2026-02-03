from typing import Any, Callable, Dict, TypeVar

from arango.database import StandardDatabase

from ml_vault.database.log_helper import utils
import functools
import sys

F = TypeVar("F", bound=Callable[..., Any])


def add_description_reverse(db: StandardDatabase, timestamp: int) -> None:
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
    name = op_info[1]
    item_name = op_info[2]
    key_ = item_name + "_" + name + "_" + "DESCRIPT"
    items = db.collection("items")
    items.delete(key_, ignore_missing=True)
    coll = db.collection("description")
    doc = coll.get(key_)
    if doc is not None:
        if int(doc["timestamp"]) == int(timestamp):
            coll.delete(key_, ignore_missing=True)
    edge = db.collection("description_edge")
    edge.delete(str(timestamp), ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    utils.commit_new_timestamp(db, timestamp, "failed")


def create_item_reverse(db: StandardDatabase, timestamp: int) -> None:
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
    name = op_info[1]
    collection_type = op_info[2]
    items = db.collection("items")
    doc = items.get(name)
    if doc is None or int(doc["timestamp"]) != int(timestamp):
        utils.commit_new_timestamp(db, timestamp, "failed")
        return
    items.delete(name, ignore_missing=True)
    coll = db.collection(collection_type)
    coll.delete(name, ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    utils.commit_new_timestamp(db, timestamp, "failed")


def append_item_reverse(db: StandardDatabase, timestamp: int) -> None:
    _, op_info = utils.get_timestamp_info(db, timestamp)
    if op_info is None:
        return
    name = op_info[1]
    dtype = op_info[2]
    input_items = op_info[3]
    n_items = op_info[6]
    length = op_info[7]

    items = db.collection("items")
    doc = items.get(name)
    if doc is None or int(doc["timestamp"]) != int(timestamp):
        utils.commit_new_timestamp(db, timestamp, "failed")
        return
    collection = db.collection(dtype)
    doc = items.get(f"{name}_{n_items}")
    if doc is None or int(doc["timestamp"]) != int(timestamp):
        utils.commit_new_timestamp(db, timestamp, "failed")
        return
    collection.delete(f"{name}_{n_items}", ignore_missing=True)
    parent = db.collection("parent_edge")
    parent.delete(str(timestamp), ignore_missing=True)
    session_edge = db.collection("session_parent_edge")
    session_edge.delete(str(timestamp), ignore_missing=True)
    edge = db.collection("dependency_edge")
    for itm in input_items:
        edge.delete(f"{timestamp}_{itm}", ignore_missing=True)
    list_collection = db.collection(f"{dtype}_list")
    itm = list_collection.get(name)
    itm["n_items"] = n_items
    itm["length"] = length
    list_collection.update(itm)
    utils.commit_new_timestamp(db, timestamp, "reverse_failed")


FUNCTION_REVERSE_MAP: Dict[str, Callable[[StandardDatabase, int], None]] = {
    "create_item_list": create_item_reverse,
    "append_item": append_item_reverse,
    "add_description_inner": add_description_reverse,
}


def function_safeguard(fn: F) -> F:
    @functools.wraps(fn)
    def wrapper(db: StandardDatabase, timestamp: int, *args: Any, **kwargs: Any) -> Any:
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

    return wrapper  # type: ignore
