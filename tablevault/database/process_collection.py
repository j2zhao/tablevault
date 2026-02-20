# Move to general items
# double check conditions

from typing import Optional

from arango.database import StandardDatabase

from tablevault.database.log_helper import utils
from tablevault.database import item_collection
from tablevault.utils.errors import NotFoundError, ConflictError, ValidationError

import os
import psutil


def create_process(
    db: StandardDatabase,
    name: str,
    user_id: str,
    execution_type: str,
    process_name: str = "",
    process_index: int = 0,
) -> None:
    doc = {
        "interrupt_request": "",
        "interrupt_action": "",
        "execution_type": execution_type,
        "pid": os.getpid(),
        "creator_user_id": user_id,
    }
    timestamp, _ = utils.get_new_timestamp(
        db, ["create_item", name, "process_list", process_name, process_index]
    )
    item_collection.create_item_list(
        db, timestamp, name, process_name, process_index, doc, "process_list"
    )


def process_add_code_start(
    db: StandardDatabase, name: str, code: str, process_name: str, process_index: int
) -> int:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    process_list = db.collection("process_list").get(name)
    data = [
        "append_item",
        name,
        "process",
        {},
        process_name,
        process_index,
        process_list["n_items"],
        process_list["length"],
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
        process_name,
        process_index,
        None,
        "process",
        process_list["n_items"],
        process_list["length"],
        process_list["length"] + len(code),
        itm["_rev"],
    )
    return


def process_add_code_end(
    db: StandardDatabase,
    name: str,
    index: int,
    error: str = "",
    timestamp: Optional[int] = None,
) -> None:
    if timestamp is None:
        timestamp, item = utils.get_new_timestamp(
            db, ["process_add_code_end", name, index, error], name
        )
    process_code = db.collection("process")
    code_doc = process_code.get(f"{name}_{index}")
    code_doc["status"] = "complete"
    code_doc["error"] = error
    process_code.update(code_doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def process_stop_pause_request(
    db: StandardDatabase, name: str, action: str, process_name: str
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db, ["process_stop_pause_request", name, action, process_name], name
    )
    process = db.collection("process_list")
    doc = process.get({"_key": name})
    if doc is None:
        utils.commit_new_timestamp(db, timestamp)
        raise NotFoundError(
            f"Process '{name}' does not exist.",
            operation="process_stop_pause_request",
            collection="process_list",
            key=name,
        )
    if doc["interrupt_request"] != "":
        utils.commit_new_timestamp(db, timestamp)
        raise ConflictError(
            f"Existing interrupt request by '{doc['interrupt_request']}' with action "
            f"'{doc['interrupt_action']}' blocks new '{action}' request.",
            operation="process_stop_pause_request",
            collection="process_list",
            key=name,
        )
    doc["interrupt_request"] = process_name
    doc["interrupt_action"] = action
    process.update(doc, check_rev=True, merge=False)
    utils.commit_new_timestamp(db, timestamp)


def process_resume_request(
    db: StandardDatabase, name: str, process_name: str, timestamp: Optional[int] = None
) -> None:
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(
            db, ["process_resume_request", name, process_name], name
        )
        process = db.collection("process_list")
        doc = process.get({"_key": name})
        if doc is None:
            utils.commit_new_timestamp(db, timestamp)
            raise NotFoundError(
                f"No process with name '{name}' found.",
                operation="process_resume_request",
                collection="process_list",
                key=name,
            )
        if doc["interrupt_action"] != "pause":
            utils.commit_new_timestamp(db, timestamp)
            raise ValidationError(
                f"Process '{name}' is not paused; current state is '{doc['interrupt_action'] or 'running'}'.",
                operation="process_resume_request",
                collection="process_list",
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
        process.update(doc, check_rev=True, merge=False)
        utils.commit_new_timestamp(db, timestamp)
    else:
        process = db.collection("process_list")
        doc = process.get({"_key": name})
        if doc is None:
            utils.commit_new_timestamp(db, timestamp, status="failed")
            raise NotFoundError(
                f"No process with name '{name}' found.",
                operation="process_resume_request",
                collection="process_list",
                key=name,
            )

        p = psutil.Process(doc["pid"])
        if p.status() == psutil.STATUS_RUNNING:
            doc["interrupt_request"] = ""
            doc["interrupt_action"] = ""
            process.update(doc, check_rev=True, merge=False)
        utils.commit_new_timestamp(db, timestamp)


def process_checkpoint(db: StandardDatabase, name: str) -> None:
    process = db.collection("process_list")
    doc = process.get({"_key": name})
    if doc["interrupt_request"] != "":
        p = psutil.Process(doc["pid"])
        if doc["interrupt_action"] == "pause":
            p.suspend()
        elif doc["interrupt_action"] == "stop":
            p.terminate()
