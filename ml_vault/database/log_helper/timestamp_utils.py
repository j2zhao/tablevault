# ADD LOGS
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
import time

def get_new_timestamp(db: StandardDatabase, data = "", wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        ts = doc["new_timestamp"]
        doc["active_timestamps"][ts] = [time.time(), data]
        doc["new_timestamp"] = ts + 1
        try:
            metadata.update(doc, check_rev=True,  merge = False)
            return ts
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not acquire lock")

def commit_new_timestamp(db, timestamp, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    doc = metadata.get("global")
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        if str(timestamp) in doc["active_timestamps"]:
            del doc["active_timestamps"][str(timestamp)] #
        try:
            metadata.update(doc, check_rev=True, merge = False)
            return True
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not commit lock")