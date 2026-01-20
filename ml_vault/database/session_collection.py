from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database import timestamp_utils
from ml_vault.database import artifact_collection_helper as helper
import os
import signal

def create_session(db: StandardDatabase, name, user_id, session_name = "", session_index = 0):
    timestamp = timestamp_utils.get_new_timestamp(db, ["create_session", name, user_id])
    creation = helper.add_artifact_name(db, name, "session_list", timestamp)
    if creation:
        session = db.collection("session_list")
        doc = {
            "_key": name,
            "name": name,
            "session_name": session_name,
            "session_index": 0,
            "timestamp": timestamp,
            "interupt_request": "",
            "interupt_action": "",
            "execution_type": 'notebook',  # e.g. "notebook", "script", "batch"
            "n_items": 0,
            "length": 0,
            "pid": os.getpid(),
            "creator_user_id": user_id,
        }
        try:
            meta = session.insert(doc)
            timestamp_utils.commit_new_timestamp(db, timestamp)
            return meta
        except DocumentInsertError:
            if session.has(name):
                timestamp_utils.commit_new_timestamp(db, timestamp)
                raise ValueError(f"{name} already exists as session")
            else:
                raise
        if session_name != "":
            helper.add_session_parent_edge(db, name, "session_list", session_name, session_index, timestamp)
    else:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        raise ValueError("Name Duplicated. Need to be unique over all artifacts.")
    
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
    try:
        session.update(doc, check_rev=True, merge = False)
        timestamp_utils.commit_new_timestamp(db, timestamp)
    except ArangoError:
        timestamp_utils.commit_new_timestamp(db, timestamp)
        return False
    return True

def session_resume_request(db: StandardDatabase, name, session_name, session_pid, timeout = 5, wait_time = 0.1):
    session = db.collection("session_list")
    doc = session.get({"_key":name})   # or session.get({"_key": key})
    if doc is None:
        raise ValueError(f"No session with name {name} found.")
    if doc["interupt_request"] != session_name:
        raise ValueError(f"Session controlled by {doc['interupt_request']}.")
    if doc["interupt_action"] != "start_pause":
        raise ValueError("session status is not paused.")
    
    timestamp = timestamp_utils.get_new_timestamp(db) 
    doc["interupt_request"] = ""
    doc["interupt_action"] = "start_resume"
    session.update(doc,  merge = False)
    timestamp_utils.commit_new_timestamp(db, timestamp)

    try:
        os.kill(doc["pid"], signal.SIGCONT)
    except Exception as e:
        print(e)
        return False 
    timestamp = timestamp_utils.get_new_timestamp(db)                
    doc["interupt_request"] = ""
    doc["interupt_action"]  = ""
    session.update(doc,  merge = False)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    return True

def session_checkpoint(db:StandardDatabase, name, timeout = 60):
    session = db.collection("session_list")
    doc = session.get({"_key":name})
    timestamp = timestamp_utils.get_new_timestamp(db) 
    if doc["interupt_request"] != "":
        if doc["interupt_request"] == "pause":
            doc["interupt_action"] = "start_pause"
        else:
            doc["interupt_action"] = "start_kill"
        session.update(doc,  merge = False)
        timestamp_utils.commit_new_timestamp(db, timestamp)
        if doc["interupt_request"] == "pause":
            sig = signal.SIGSTOP
        else:
            sig = signal.SIGTERM
        os.kill(doc["pid"], sig)   


def session_add_code_start(db:StandardDatabase, name, code, session_name = "", session_index = 0):
    session = db.collection("session_list")
    session_code = db.collection("session")
    doc = session.get({"_key":name})
    start_index = doc["n_items"]
    start_position = doc["length"]
    end_position = start_position + len(code)
    timestamp = timestamp_utils.get_new_timestamp(db, ["session_add_code_start", name, code])
    code_doc = {
        "_key": f"{name}_{start_index}",
        "name": f"{name}_{start_index}", # need to evaluate this
        "index": start_index,
        "session_name": session_name,
        "session_index": session_index,
        "timestamp": timestamp,
        "start_position": start_position,
        "end_position":end_position,
        "text": code,
        "status": "start",
        "error": "",
    }
    session_code.insert(code_doc)
    helper.add_parent_edge(db, f"{name}_{start_index}", name, "session", start_position, end_position, timestamp)
    if session_name != "":
        helper.add_session_parent_edge(db, f"{name}_{start_index}", "session", session_name, session_index, timestamp)
    doc["length"] = start_position + len(code)
    doc["n_items"] = start_index + 1
    session.update(doc,  merge = False)
    timestamp_utils.commit_new_timestamp(db, timestamp)
    return start_index


def session_add_code_end(db:StandardDatabase, name, index, error = ""):
    session = db.collection("session_list")
    session_code = db.collection("session")
    doc = session.get({"_key":name})
    code_doc = session_code.get({"_key":f"{name}_{index}"})
    timestamp = timestamp_utils.get_new_timestamp(db, ["session_add_code_end", name, index, error])
    code_doc["status"] = "complete"
    code_doc["error"] = error
    session_code.update(code_doc,  merge = False)
    session.update(doc, merge = False)
    timestamp_utils.commit_new_timestamp(db, timestamp)



