from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper
import os
import signal
import time

def create_session(db: StandardDatabase, name, timestamp, last_pid, user_id):
    session = db.collection("session_list")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_key": name,
        "name": name,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "interupt_request": "",
        "interupt_action": "",
        "execution_type": 'notebook',  # e.g. "notebook", "script", "batch"
        "n_items": 0,
        "length": 0,
        "pid": os.getpid(),
        "creator_user_id": user_id,
    }
    try:
        meta = session.insert(doc)  # meta contains _id, _key, _rev
    except DocumentInsertError as e:
        if session.has(name):
            raise ValueError(f"{name} already exists as session")
    timestamp_utils.commit_new_timestamp(timestamp)
    return meta


def session_stop_pause_request(db: StandardDatabase, name, action, session_name, session_pid, timeout = 5, wait_time = 0.1):
    if action != "stop" and action != "pause":
        raise ValueError("Only support stop or pause events.")
    
    session = db.collection("session_list")
    doc = session.get({"_key":name})   # or session.get({"_key": key})
    if doc is None:
        raise ValueError(f"No session with name {name} found.")
    if doc["interupt_request"] != "":
        return False
    doc["interupt_request"] = session_name 
    doc["interupt_action"] = action
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc["last_timestamp"] = timestamp 
    try:
        session.update(doc, check_rev=True)
        timestamp_utils.commit_new_timestamp(timestamp)
    except ArangoError:
        timestamp_utils.commit_new_timestamp(timestamp)
        return False
    return True

def session_resume_request(db: StandardDatabase, name, action, session_name, session_pid, timeout = 5, wait_time = 0.1):
    if action != "resume":
        raise ValueError("Only support resume events.")
    session = db.collection("session_list")
    doc = session.get({"_key":name})   # or session.get({"_key": key})
    if doc is None:
        raise ValueError(f"No session with name {name} found.")
    if doc["interupt_request"] != session_name:
        raise ValueError(f"Session controlled by {doc["interupt_request"]}.")
    if doc["interupt_action"] != "start_pause":
        raise ValueError(f"session status is not paused.")
    
    timestamp = timestamp_utils.get_new_timestamp(db) 
    doc["interupt_request"] = ""
    doc["interupt_action"] = "start_resume"
    doc["last_timestamp"] = timestamp
    session.update(doc)
    timestamp_utils.commit_new_timestamp(timestamp)

    try:
        os.kill(doc["pid"], signal.SIGCONT)
    except Exception as e:
        print(e)
        return False 
    timestamp = timestamp_utils.get_new_timestamp(db)                
    doc["interupt_request"] = ""
    doc["interupt_action"]  = ""
    doc["last_timestamp"] = timestamp
    session.update(doc)
    timestamp_utils.commit_new_timestamp(timestamp)
    return True

def session_checkpoint(db:StandardDatabase, name, timeout = 60):
    session = db.collection("session_list")
    doc = session.get({"_key":name})
    timestamp = timestamp_utils.get_new_timestamp(db) 
    if doc["interupt_request"] != "" and doc["last_timestamp"] < timstamp:
        doc["last_timestamp"] = timestamp
        if doc["interupt_request"] == "pause":
            doc["interupt_action"] = "start_pause"
        else:
            doc["interupt_action"] = "start_kill"
        session.update(doc)
        timestamp_utils.commit_new_timestamp(timestamp)
        if action == "pause":
            sig = signal.SIGSTOP
        else:
            sig = signal.SIGTERM
        os.kill(doc["pid"], sig)   


def session_add_code_start(db:StandardDatabase, name, code):
    session = db.collection("session_list")
    session_code = db.collection("session")
    doc = session.get({"_key":name})
    start_index = doc["n_items"]
    start_position = doc["length"]
    end_position = start_position + len(code)
    timestamp = timestamp_utils.get_new_timestamp(db)
    code_doc = {
        "_key": f"{name}_{start_index}",
        "name": f"{name}_{start_index}", # need to evaluate this
        "index": start_index,
        "timestamp": timestamp,
        "last_timestamp": timestamp,
        "position_start": start_position,
        "position_end":end_position,
        "text": code,
        "status": "start",
        "error": "",
    }
    session_code.insert(code_doc)

    # edge_doc = {
    #     "index": start_index,
    #     "start_position": start_position,
    #     "end_position": end_position,
    #     "_from": f"session_code/{name}_{start_index}",
    #     "_to": f"session/{name}"
    # }
    helper.add_parent_edge(db, f"{name}_{start_index}", "session", start_position, end_position, name, "session_list", timestamp, True)
    doc["last_timestamp"] = timestamp
    doc["length"] = start_position + len(code)
    doc["n_items"] = start_index + 1
    session.update(doc)
    timestamp_utils.commit_new_timestamp(timestamp)
    return start_index


def session_add_code_end(db:StandardDatabase, name, index, status = "complete", error = ""):
    session = db.collection("session_list")
    session_code = db.collection("session")
    doc = session.get({"_key":name})
    code_doc = session.get({"_key":f"{name}_{index}"})
    timestamp = timestamp_utils.get_new_timestamp(db)
    code_doc["last_timestamp"] = timestamp
    code_doc["status"] = status
    code_doc["error"] = error
    session_code.update(code_doc)
    doc["last_timestamp"] = timestamp
    session.update(doc)
    timestamp_utils.commit_new_timestamp(timestamp)



