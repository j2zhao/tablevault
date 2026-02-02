# Move to general artifacts
# double check conditions

from ml_vault.database.log_helper import utils
from ml_vault.database import artifact_collection

import os
import psutil


def create_session(db, name, user_id, execution_type, session_name="", session_index=0):
    doc = {
        "interrupt_request": "",
        "interrupt_action": "",
        "execution_type": execution_type,
        "pid": os.getpid(),
        "creator_user_id": user_id,
    }
    timestamp, _ = utils.get_new_timestamp(
        db, ["create_artifact", name, "session_list", session_name, session_index]
    )
    artifact_collection.create_artifact_list(
        db, timestamp, name, session_name, session_index, doc, "session_list"
    )


def session_add_code_start(db, name, code, session_name, session_index):
    timestamp, art = utils.get_new_timestamp(db, [], name)
    session_list = db.collection("session_list").get(name)
    data = [
        "append_artifact",
        name,
        "session",
        {},
        session_name,
        session_index,
        session_list["n_items"],
        session_list["length"],
        "dtype"
    ]
    utils.update_timestamp_info(db, timestamp, data)
    code_doc = {
        "text": code,
        "status": "start",
        "error": "",
    }
    
    return artifact_collection.append_artifact(
        db,
        timestamp,
        name,
        code_doc,
        session_name,
        session_index,
        None,
        "session",
        session_list["n_items"],
        session_list["length"],
        session_list["length"] + len(code),
        art["_rev"],
    )
    return


def session_add_code_end(db, name, index, error="", timestamp=None):
    if timestamp is None:
        timestamp, artifact = utils.get_new_timestamp(
            db, ["session_add_code_end", name, index, error], name
        )
    session_code = db.collection("session")
    code_doc = session_code.get(f"{name}_{index}")
    code_doc["status"] = "complete"
    code_doc["error"] = error
    session_code.update(code_doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def session_stop_pause_request(db, name, action, session_name):
    timestamp, _ = utils.get_new_timestamp(
        db, ["session_stop_pause_request", name, action, session_name], name
    )
    session = db.collection("session_list")
    doc = session.get({"_key": name})
    if doc is None:
        utils.commit_new_timestamp(db, timestamp)
        raise ValueError(f"Session with {name} doesn't exist.")
    if doc["interrupt_request"] != "":
        utils.commit_new_timestamp(db, timestamp)
        raise ValueError(
            f"Pre-existing request found by {doc['interrupt_request']} found."
        )
    doc["interrupt_request"] = session_name
    doc["interrupt_action"] = action
    session.update(doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def session_resume_request(db, name, session_name, timestamp=None):
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(
            db, ["session_resume_request", name, session_name], name
        )
        session = db.collection("session_list")
        doc = session.get({"_key": name})
        if doc is None:
            utils.commit_new_timestamp(db, timestamp)
            raise ValueError(f"No session with name {name} found.")
        if doc["interrupt_action"] != "pause":
            utils.commit_new_timestamp(db, timestamp)
            raise ValueError("session status is not paused.")
        try:
            p = psutil.Process(doc["pid"])
            p.resume()
        except Exception:
            utils.commit_new_timestamp(db, timestamp)
            raise
        doc["interrupt_request"] = ""
        doc["interrupt_action"] = ""
        session.update(doc, check_rev=True, merge=False)
        utils.commit_new_timestamp(db, timestamp)
    else:
        session = db.collection("session_list")
        doc = session.get({"_key": name})
        if doc is None:
            utils.commit_new_timestamp(db, timestamp, status="failed")
            raise ValueError(f"No session with name {name} found.")

        p = psutil.Process(doc["pid"])
        if p.status() == psutil.STATUS_RUNNING:
            doc["interrupt_request"] = ""
            doc["interrupt_action"] = ""
            session.update(doc, check_rev=True, merge=False)
        utils.commit_new_timestamp(db, timestamp)
    


def session_checkpoint(db, name):
    session = db.collection("session_list")
    doc = session.get({"_key": name})
    if doc["interrupt_request"] != "":
        p = psutil.Process(doc["pid"])
        if doc["interrupt_action"] == "pause":
            p.suspend()
        elif doc["interrupt_action"] == "stop":
            p.terminate()
