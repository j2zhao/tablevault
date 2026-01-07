from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
import time

def get_new_timestamp(db: StandardDatabase, wait_time = 0.1, timeout = None, maximum_gap = 300):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        ts = doc["new_timestamp"]
        current_time = time.time()
        doc["active_timestamps"][ts] = time.time()
        doc["new_timestamp"] = ts + 1
        try:
            metadata.update(doc, check_rev=True)
            return t
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    return None

def commit_new_timestamp(db, timestamp, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        if timestamp in doc["active_timestamps"]:
            del doc["active_timestamps"][timestamp] #
        try:
            metadata.update(doc, check_rev=True)
            return True
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    return False


def get_read_timestamp(db: StandardDatabase):
    metadata = db.collection("metadata")
    doc = metadata.get("global")
    for ts_ in doc["active_timestamps"]:
        if current_time - doc["active_timestamps"][ts_] > 300:
            del doc["active_timestamps"][ts_]
    tss = list(doc["active_timestamps"].keys())
    if len(tss) > 0:
        read_timestamp = min(tss) - 1
    else:
        read_timestamp = doc["new_timestamp"] - 1
    return read_timestamp