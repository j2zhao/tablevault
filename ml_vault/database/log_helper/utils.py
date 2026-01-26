# ADD LOGS
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
import time
from typing import Any, Dict, Optional, Union


from typing import Any, Dict, Optional
from arango.exceptions import ArangoError

def guarded_upsert(
    db,
    name: str,
    timestamp: int,              
    guard_rev: str,
    target_col: str,
    target_key: str,
    update_patch: Dict[str, Any] = {},
    insert_doc: Optional[Dict[str, Any]] = {},
    merge_objects: bool = False,
):
    aql = r"""
    LET bump = (
      UPDATE { _key: @name, _rev: @guardRev }
        WITH { version: @now }
      IN artifacts
      OPTIONS { ignoreRevs: false }
      RETURN NEW
    )
    FILTER LENGTH(bump) == 1

    LET t = (
      UPSERT { _key: @targetKey }
        INSERT MERGE(@insertDoc, { _key: @targetKey })
        UPDATE @updatePatch
      IN @@targetCol
      OPTIONS { mergeObjects: @mergeObjects }
      RETURN NEW
    )
    RETURN bump[0]._rev
    """
    bind_vars = {
        "name": name,
        "guardRev": guard_rev,
        "now": time.time(),
        "@targetCol": target_col,
        "targetKey": target_key,
        "updatePatch": update_patch,
        "insertDoc": insert_doc,
        "mergeObjects": merge_objects,
    }

    try:
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        out = next(cursor, None)
        if out is None:
            raise RuntimeError("Guard check failed (missing guard doc or rev mismatch).")
        return out
    except ArangoError:
        raise


def log_tuple(log_file: str, record: tuple):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{record}\n")

def add_artifact_name(db, artifact_name, artifact_type, timestamp):
    artifacts = db.collection("artifacts")
    doc = {
        "_key": artifact_name,
        "name": artifact_name, 
        "collection": artifact_type,
        "timestamp": timestamp,
        "version": time.time(),
    }
    art = artifacts.insert(doc)
    return art["_rev"]


def update_artifact(db:StandardDatabase, name, timestamp, timeout = 60, wait_time = 0.1):
    artifacts = db.collection("artifacts")
    while end - start < timeout:
        artifact = artifacts.get(name)
        if artifact == None:
            raise ValueError("Item list doesn't exist.")
        if artifact["timestamp"] != timestamp:
            raise ValueError("Item currently not locked to this different operation.")
        artifact["version"] = artifact["version"] + 1
        try:
            artifacts.update(artifact, check_rev=True, merge=False)
            return artifact
        except Exception:
            pass
        time.sleep(wait_time)
        end = time.time()
    raise ValueError(f"Could not lock artifact {name}")

def lock_artifact(db:StandardDatabase, name, timestamp, timeout = 60, wait_time = 0.1):
    start = time.time()
    end = time.time()
    artifacts = db.collection("artifacts")
    while end - start < timeout:
        artifact = artifacts.get(name)
        if artifact == None:
            raise ValueError("Item list doesn't exist.")
        info = timestamp_utils.get_timestamp_info(db, artifact["timestamp"])
        if info is None:        
            artifact["timestamp"] = timestamp
            artifact["version"] = time.time()
            try:
                artifacts.update(artifact, check_rev=True, merge=False)
                return artifact
            except Exception:
                pass
        time.sleep(wait_time)
        end = time.time()
    raise ValueError(f"Could not lock artifact {name}")

def get_new_timestamp(db: StandardDatabase, data = [], artifact = None, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    ts = None
    success = False
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        ts = doc["new_timestamp"]
        doc["active_timestamps"][ts] = [time.time(), data]
        doc["new_timestamp"] = ts + 1
        
        try:
            metadata.update(doc, check_rev=True,  merge = False)
            success = True
            break
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    if not success:
        raise ValueError("Could not acquire lock")
    if artifact is not None:
        try:
            art = lock_artifact(db, artifact, ts)
            return ts, art
        except:
            commit_new_timestamp(db, ts)
            raise
    else:
        return ts, None

def update_timestamp_info(db:StandardDatabase, timestamp, data = [], wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        doc["active_timestamps"][timestamp] = [time.time(), data]        
        try:
            metadata.update(doc, check_rev=True,  merge = False)
            return ts
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not update timestamp information.")

def commit_new_timestamp(db, timestamp, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    doc = metadata.get("global")
    log_file = doc["log_file"]
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        data = None
        if str(timestamp) in doc["active_timestamps"]:
            data = doc["active_timestamps"][str(timestamp)]
            del doc["active_timestamps"][str(timestamp)] #
        try:
            metadata.update(doc, check_rev=True, merge = False)
            if data is not None:
                log_tuple(log_file, data)
            return True
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not commit lock")

def get_timestamp_info(db, timestamp):
    metadata = db.collection("metadata")
    doc = metadata.get("global")
    if str(timestamp) in doc["active_timestamps"]:
        return doc["active_timestamps"][str(timestamp)]
    else:
        return None