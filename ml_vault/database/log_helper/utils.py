# ADD LOGS
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
import time
from typing import Any, Dict, Optional
from ml_vault.database.log_helper import log_manager

def guarded_upsert(
    db,
    name: str,
    timestamp: int,              
    guard_rev: str,
    target_col: str,
    target_key: str,
    update_patch: Optional[Dict[str, Any]] = None,
    insert_doc: Optional[Dict[str, Any]] = None,
    merge_objects: bool = False,
):
    update_patch = dict(update_patch) if update_patch is not None else {}
    insert_doc = dict(insert_doc) if insert_doc is not None else {}
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
    end = time.time()
    start = time.time()
    while end - start < timeout:
        artifact = artifacts.get(name)
        if artifact is None:
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
        if artifact is None:
            raise ValueError("Item list doesn't exist.")
        artifact["timestamp"] = timestamp
        artifact["version"] = time.time()
        try:
            artifact = artifacts.update(artifact, check_rev=True, merge=False)
            return artifact
        except Exception:
            pass
        time.sleep(wait_time)
        end = time.time()
    raise ValueError(f"Could not lock artifact {name}")

def get_new_timestamp(db: StandardDatabase, data: Optional[list] = None, artifact = None, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    ts = None
    success = False
    data = [] if data is None else list(data)
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        ts = doc["new_timestamp"]
        key = str(ts)
        doc["active_timestamps"][key] = ["start", time.time(), data]
        doc["new_timestamp"] = ts + 1
        log_file = doc["log_file"]
        try:
            log_manager.log_tuple(log_file, doc["active_timestamps"][key])
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
        except Exception:
            commit_new_timestamp(db, ts)
            raise
    else:
        return ts, None

def update_timestamp_info(db:StandardDatabase, timestamp, data: Optional[list] = None, wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    key = str(timestamp)
    data = [] if data is None else list(data) 
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        doc["active_timestamps"][key] = ["update", time.time(), data]        
        log_file = doc["log_file"]
        try:
            log_manager.log_tuple(log_file, doc["active_timestamps"][key])
            metadata.update(doc, check_rev=True,  merge = False)
            return
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not update timestamp information.")

def commit_new_timestamp(db, timestamp, status = "complete", wait_time = 0.1, timeout = None):
    metadata = db.collection("metadata")
    start = time.time()
    end = time.time()
    doc = metadata.get("global")
    log_file = doc["log_file"]
    key = str(timestamp)
    while timeout is None or end - start < timeout:
        doc = metadata.get("global")
        data = None
        if key in doc["active_timestamps"]:
            data = doc["active_timestamps"][key]
            data[0] = status
            del doc["active_timestamps"][key] #
        try:
            if data is not None:
                log_manager.log_tuple(log_file, data)
            metadata.update(doc, check_rev=True, merge = False)
            return True
        except ArangoError:
            time.sleep(wait_time)
        end = time.time()
    raise ValueError("Could not commit lock")

def get_timestamp_info(db, timestamp = None):
    metadata = db.collection("metadata")
    doc = metadata.get("global")
    if timestamp is not None:
        key = str(timestamp)
        if key in doc["active_timestamps"]:
            return doc["active_timestamps"][key][1:]
        else:
            return None
    else:
        return doc["active_timestamps"]
