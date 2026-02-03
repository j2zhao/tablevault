# change description format

from typing import List

from arango.database import StandardDatabase

from ml_vault.database.log_helper import utils
from ml_vault.database.log_helper.operation_management import function_safeguard
from ml_vault.utils.errors import NotFoundError


@function_safeguard
def add_description_inner(
    db: StandardDatabase,
    timestamp: int,
    name: str,
    item_name: str,
    session_name: str,
    session_index: int,
    description: str,
    embedding: List[float],
) -> None:
    items = db.collection("items")
    itm = items.get({"_key": item_name})
    if itm is None:
        utils.commit_new_timestamp(db, timestamp)
        raise NotFoundError(
            f"Item '{item_name}' not found while adding description.",
            operation="add_description_inner",
            collection="items",
            key=item_name,
        )
    item_collection = itm["collection"]
    key_ = item_name + "_" + name + "_" + "DESCRIPT"
    guard_rev = utils.add_item_name(db, key_, "description", timestamp)
    doc = {
        "_key": key_,
        "name": key_,
        "item_name": item_name,
        "session_name": session_name,
        "session_index": session_index,
        "collection": item_collection,
        "timestamp": timestamp,
        "text": description,
        "embedding": embedding,
        "deleted": -1,
    }
    guard_rev = utils.guarded_upsert(
        db, key_, timestamp, guard_rev, "description", key_, {}, doc
    )
    doc = {
        "_key": str(timestamp),
        "timestamp": timestamp,
        "_from": f"{item_collection}/{item_name}",
        "_to": f"description/{key_}",
    }
    guard_rev = utils.guarded_upsert(
        db, key_, timestamp, guard_rev, "description_edge", str(timestamp), {}, doc
    )

    doc = {
        "_key": str(timestamp),
        "timestamp": timestamp,
        "index": session_index,
        "_from": f"session_list/{session_name}",
        "_to": f"description/{key_}",
    }
    utils.guarded_upsert(
        db, key_, timestamp, guard_rev, "session_parent_edge", str(timestamp), {}, doc
    )


def add_description(
    db: StandardDatabase,
    name: str,
    item_name: str,
    session_name: str,
    session_index: int,
    description: str,
    embedding: List[float],
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db, ["add_description", name, item_name, session_name, session_index]
    )
    add_description_inner(
        db,
        timestamp,
        name,
        item_name,
        session_name,
        session_index,
        description,
        embedding,
    )
    utils.commit_new_timestamp(db, timestamp)
