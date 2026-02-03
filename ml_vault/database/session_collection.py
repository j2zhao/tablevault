# Move to general items
# double check conditions

from typing import Optional

from arango.database import StandardDatabase

from ml_vault.database.log_helper import utils
from ml_vault.database import item_collection
from ml_vault.utils.errors import NotFoundError, ConflictError, ValidationError

import os
import psutil


def create_session(
    db: StandardDatabase,
    name: str,
    user_id: str,
    execution_type: str,
    session_name: str = "",
    session_index: int = 0,
) -> None:
    doc = {
        "interrupt_request": "",
        "interrupt_action": "",
        "execution_type": execution_type,
        "pid": os.getpid(),
        "creator_user_id": user_id,
    }
    timestamp, _ = utils.get_new_timestamp(
        db, ["create_item", name, "session_list", session_name, session_index]
    )
    item_collection.create_item_list(
        db, timestamp, name, session_name, session_index, doc, "session_list"
    )


def session_add_code_start(
    db: StandardDatabase, name: str, code: str, session_name: str, session_index: int
) -> int:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    session_list = db.collection("session_list").get(name)
    data = [
        "append_item",
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

    return item_collection.append_item(
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
        itm["_rev"],
    )
    return


def session_add_code_end(
    db: StandardDatabase,
    name: str,
    index: int,
    error: str = "",
    timestamp: Optional[int] = None,
) -> None:
    if timestamp is None:
        timestamp, item = utils.get_new_timestamp(
            db, ["session_add_code_end", name, index, error], name
        )
    session_code = db.collection("session")
    code_doc = session_code.get(f"{name}_{index}")
    code_doc["status"] = "complete"
    code_doc["error"] = error
    session_code.update(code_doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def session_stop_pause_request(
    db: StandardDatabase, name: str, action: str, session_name: str
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db, ["session_stop_pause_request", name, action, session_name], name
    )
    session = db.collection("session_list")
    doc = session.get({"_key": name})
    if doc is None:
        utils.commit_new_timestamp(db, timestamp)
        raise NotFoundError(
            f"Session '{name}' does not exist.",
            operation="session_stop_pause_request",
            collection="session_list",
            key=name,
        )
    if doc["interrupt_request"] != "":
        utils.commit_new_timestamp(db, timestamp)
        raise ConflictError(
            f"Existing interrupt request by '{doc['interrupt_request']}' with action "
            f"'{doc['interrupt_action']}' blocks new '{action}' request.",
            operation="session_stop_pause_request",
            collection="session_list",
            key=name,
        )
    doc["interrupt_request"] = session_name
    doc["interrupt_action"] = action
    session.update(doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def session_resume_request(
    db: StandardDatabase, name: str, session_name: str, timestamp: Optional[int] = None
) -> None:
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(
            db, ["session_resume_request", name, session_name], name
        )
        session = db.collection("session_list")
        doc = session.get({"_key": name})
        if doc is None:
            utils.commit_new_timestamp(db, timestamp)
            raise NotFoundError(
                f"No session with name '{name}' found.",
                operation="session_resume_request",
                collection="session_list",
                key=name,
            )
        if doc["interrupt_action"] != "pause":
            utils.commit_new_timestamp(db, timestamp)
            raise ValidationError(
                f"Session '{name}' is not paused; current state is '{doc['interrupt_action'] or 'running'}'.",
                operation="session_resume_request",
                collection="session_list",
                key=name,
            )
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
            raise NotFoundError(
                f"No session with name '{name}' found.",
                operation="session_resume_request",
                collection="session_list",
                key=name,
            )

        p = psutil.Process(doc["pid"])
        if p.status() == psutil.STATUS_RUNNING:
            doc["interrupt_request"] = ""
            doc["interrupt_action"] = ""
            session.update(doc, check_rev=True, merge=False)
        utils.commit_new_timestamp(db, timestamp)
    


def session_checkpoint(db: StandardDatabase, name: str) -> None:
    session = db.collection("session_list")
    doc = session.get({"_key": name})
    if doc["interrupt_request"] != "":
        p = psutil.Process(doc["pid"])
        if doc["interrupt_action"] == "pause":
            p.suspend()
        elif doc["interrupt_action"] == "stop":
            p.terminate()
