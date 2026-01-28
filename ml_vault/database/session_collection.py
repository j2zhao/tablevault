# Move to general artifacts
# double check conditions

from arango.database import StandardDatabase
from arango.exceptions import ArangoError, DocumentInsertError
from ml_vault.database.log_helper import utils
from ml_vault.database import artifact_collection_helper as helper
from ml_vault.database import artifact_collection

import os
import signal

def create_session(db, name, user_id, session_name = "", session_index = 0):
    doc = {
        "interupt_request": "",
        "interupt_action": "",
        "execution_type": 'notebook',
        "pid": os.getpid(),
        "creator_user_id": user_id,
    }
    timestamp, _ = utils.get_new_timestamp(db, ["create_artifact", name, "session_list", code, session_name, session_index])
    artifact_collection.create_artifact_list(db, timestamp, version, name, session_name, session_index, artifact, "session_list")

def session_add_code_start(db, name, code, session_name, session_index):
    timestamp, art = utils.get_new_timestamp(db, [], name)
    session_list = db.collection("session_list").get(name)
    data = ["append_artifact", name, "session", {}, session_name, session_index, session_list["n_items"], session_list["length"]]
    utils.update_timestamp_info(db, timestamp,  data)
    code_doc = {
        "text": code,
        "status": "start",
        "error": "",
    }
    
    artifact_collection.append_artifact(db, timestamp, name, code_doc, session_name, session_index, {}, "session", session["n_items"], session["length"], session["length"] + len(code), art["_rev"])
    return session["n_items"]

def session_add_code_end(db, name, index, error = "", timestamp = None):
    if timestamp is None:
        timestamp, artifact = utils.get_new_timestamp(db, ["session_add_code_end", name, index, error], name)
    session_code = db.collection('session')
    code_doc = session_code.get({"_key":f"{name}_{index}"})
    code_doc["status"] = "complete"
    code_doc["error"] = error
    session_code.update(code_doc, check_rev = True, merge = False)
    utils.commit_new_timestamp(db, timestamp)
    
def session_stop_pause_request(db, name, action, session_name):
    if action != "stop" and action != "pause":
        raise ValueError("Only support stop or pause events.")
    timestamp, _ = utils.get_new_timestamp(db, ["session_stop_pause_request", name, action, session_name], name)
       
    session = db.collection("session_list")
    doc = session.get({"_key":name}) 
    if doc["interupt_request"] != "":
        utils.commit_new_timestamp(db, timestamp)
        raise ValueError(f"Pre-existing request found by {doc["interupt_request"]} found.")
    doc["interupt_request"] = session_name 
    doc["interupt_action"] = action
    session.update(doc, check_rev=True, merge = False)
    utils.commit_new_timestamp(db, timestamp)

def session_resume_request(db, name, session_name, timestamp = None):
    if timestamp is None:    
        timestamp, _ = utils.get_new_timestamp(db, ["session_resume_request", name, session_name], name)
    doc = session.get({"_key":name})
    if doc is None:
        utils.commit(db, timestamp)
        raise ValueError(f"No session with name {name} found.")
    if doc["interupt_action"] != "start_pause":
        utils.commit(db, timestamp)
        raise ValueError("session status is not paused.")
    try:
        os.kill(doc["pid"], signal.SIGCONT)
    except Exception as e:
        utils.commit_new_timestamp(db, timestamp)
        raise
    doc["interupt_request"] = ""
    doc["interupt_action"]  = ""
    session.update(doc, check_rev = True, merge = False) # check reve should always be true
    utils.commit_new_timestamp(db, timestamp)
    return True

def session_checkpoint(db, name, timestamp = None):
    session = db.collection("session_list")
    doc = session.get({"_key":name})
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(db, ["session_checkpoint", name], name)
    if doc["interupt_request"] != "":
        if doc["interupt_request"] == "pause":
            doc["interupt_action"] = "start_pause"
        else:
            doc["interupt_action"] = "start_kill"
        
        session.update(doc, check_rev = True, merge = False)        
        utils.commit_new_timestamp(db, timestamp)
        if doc["interupt_request"] == "pause":
            sig = signal.SIGSTOP
        else:
            sig = signal.SIGTERM
        os.kill(doc["pid"], sig)