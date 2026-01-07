from arango import ArangoClient, ArangoError
from pydantic import BaseModel
import re
from ml_vault.artifacts.artifacts import Artifact
from ml_vault.utils import utils, constants
from pathlib import Path

class Experiment(Artifact):
    code: str
    file_name: str
    artifacts: list[int] #EDGE
    pid: int
    status: str


def update(db, experiment_id, line, parent_id, timestamp, pid=None, status=None):
    coll = db.collection("experiment")
    doc = coll.get({"_key": experiment_id}) or {}
    changes = {"_key": experiment_id}
    if pid is not None:
        prev = doc.get("pid_events", [])
        prev.append({"pid": pid, "ts": timestamp})
        changes["pid_events"] = prev
    if status is not None:
        prev = doc.get("status_events", [])
        prev.append({"status": status, "ts": timestamp})
        changes["status_events"] = prev

    prev = doc.get("timestamps", [])
    prev.append(timestamp)
    changes["timestamps"] = prev

    coll.update(changes)
    edge_data = {
        "_from": f"experiment/{experiment_id}",
        "_to":   f"experiment/{parent_id}",
        "type": "parent_experiment",
        "timestamp": timestamp,
        "action": "update",
        "data_type": "experiment",
        "line": line,
    }
    db.collection("parent_experiment").insert(edge_data)


def write(db, 
          experiment_id, 
          file_name, 
          code,
          pid,
          start_time,
          line,
          parent_id,
          timestamp,
          status, 
          description = ""):
    # Base experiment document using the new schema
    properties = {
        "_key": experiment_id,          # use _key; _id will be "experiment/<_key>"
        "timestamps": [timestamp],
        "file_name": file_name,
        "code": code,
        "start_time": start_time,
        "description": description,
        "active": True,

        "pid_events": [
            {"pid": pid, "ts": timestamp}
        ],
        "pid_events": [
            {"status": status, "ts": timestamp}
        ]
    }

    result = db.collection("experiment").insert(properties)

    # lineage / parent edge
    edge_data = {
        "_from": f"experiment/{experiment_id}",
        "_to":   f"experiment/{parent_id}",
        "type": "parent_experiment",
        "timestamp": timestamp,
        "action": "write",
        "data_type": "experiment",
        "line": line,
    }
    db.collection("parent_experiment").insert(edge_data)

    return result

def get_from_id(id, timestamp):
    pass

def filter_by_property_search(db, ids, props, time_stamp = None):
    # description -> embedding
    # description -> String search
    # start_time -> integer comparision
    # pid int
    # code 
    # file_name
    # status

    query = """
    FOR doc IN experiment_search_view
        SEARCH doc._id IN @ids
           AND doc.@prop LIKE @pattern
        RETURN doc
    """
    cursor = db.aql.execute(query, bind_vars={
        "ids": ids,
        "pattern": f"%{substring}%",
        "prop": prop,
    })
    return list(cursor)


def create(db):
    db.create_collection("experiment")
    db.create_collection("experiment_files")
    db.create_collection("parent_experiment", edge=True)
    db.collection("experiment").add_index(
        type="vector",
        fields=["embedding"],
        metric="cosine",  # or "l2" (Euclidean)
        dimension=1536      # Must match your vector size
    )