# centralize creation

from typing import Any, Dict, List, Optional

from arango.database import StandardDatabase
from tablevault.database.log_helper import utils
from tablevault.database import database_vector_indices as vector_helper
from tablevault.database.log_helper.operation_management import function_safeguard
from tablevault.utils.errors import ValidationError


def delete_item_list_inner(
    db: StandardDatabase,
    timestamp: int,
    name: str,
    item_collection: str,
    session_name: str,
    session_index: int,
) -> None:
    aql = r"""
    LET rootId  = @rootId
    LET rootKey = @rootKey
    LET sid     = CONCAT(@sessionCol, "/", @sessionKey)

    LET childIds = UNIQUE(
    FOR v, e IN 1..1 OUTBOUND rootId parent_edge
        RETURN v._id
    )

    LET rmSessionEdges = (
    FOR e IN session_parent_edge
        FILTER e._to IN childIds
        REMOVE e IN session_parent_edge
        RETURN 1
    )

    LET rmDepEdges = (
    FOR e IN dependency_edge
        FILTER e._from IN childIds
        REMOVE e IN dependency_edge
        RETURN 1
    )

    LET rmChildren = (
    FOR v IN @@childCol
        FILTER v._id IN childIds
        REMOVE v IN @@childCol
        RETURN 1
    )

    LET rmParentEdges = (
    FOR e IN parent_edge
        FILTER e._from == rootId
        REMOVE e IN parent_edge
        RETURN 1
    )

    LET updRoot = (
    UPDATE rootKey WITH { deleted: 1 } IN @@rootCol
    RETURN 1
    )

    LET upsertDeletedSessEdge = (
        INSERT {
        _key: @edgeKey,
        _from: rootId, _to: sid,
        index: @sessionIndex,
        timestamp: @ts
        }
        INTO deleted_session_parent_edge
        RETURN 1
        )

    RETURN {
    rootId,
    session_id: sid,
    childCount: LENGTH(childIds),

    removed: {
        session_parent_edge: LENGTH(rmSessionEdges),
        dependency_edge: LENGTH(rmDepEdges),
        children: LENGTH(rmChildren),
        parent_edge: LENGTH(rmParentEdges)
    },

    updated: { root: LENGTH(updRoot) },
    inserted: { deleted_session_parent_edge: LENGTH(upsertDeletedSessEdge) }
    }

    """
    child_col = item_collection.split("_")[0]
    root_id = f"{item_collection}/{name}"
    bind_vars = {
        "rootId": root_id,
        "rootKey": name,
        "@rootCol": item_collection,
        "@childCol": child_col,
        "sessionCol": "session_list",
        "sessionKey": session_name,
        "sessionIndex": session_index,
        "ts": timestamp,
        "edgeKey": str(timestamp),
    }
    print(bind_vars)
    val = next(db.aql.execute(aql, bind_vars=bind_vars), {})
    print(val)


def delete_item_list(
    db: StandardDatabase,
    name: str,
    session_name: str,
    session_index: int,
    timestamp: Optional[int] = None,
) -> None:
    items = db.collection("items")
    item = items.get(name)
    if item["collection"] in ["session_list", "description"]:
        raise ValidationError(
            f"Cannot delete items in protected collections ('session_list', 'description'); "
            f"item='{name}' belongs to '{item['collection']}'.",
            operation="delete_item_list",
            collection=item["collection"],
            key=name,
        )
    if timestamp is None:
        timestamp, _ = utils.get_new_timestamp(
            db,
            [
                "delete_item_list",
                name,
                item["collection"],
                session_name,
                session_index,
            ],
            name,
        )
    delete_item_list_inner(
        db, timestamp, name, item["collection"], session_name, session_index
    )
    utils.commit_new_timestamp(db, timestamp)


@function_safeguard
def create_item_list(
    db: StandardDatabase,
    timestamp: int,
    name: str,
    session_name: str,
    session_index: int,
    item: Dict[str, Any],
    collection_type: str,
) -> None:
    rev_ = utils.add_item_name(db, name, collection_type, timestamp)
    item["name"] = name
    item["session_name"] = session_name
    item["session_index"] = session_index
    item["timestamp"] = timestamp
    item["n_items"] = 0
    item["length"] = 0
    item["deleted"] = -1
    rev_ = utils.guarded_upsert(
        db, name, timestamp, rev_, collection_type, name, {}, item
    )
    if session_name != "":
        doc = {
            "_key": str(timestamp),
            "timestamp": timestamp,
            "index": session_index,
            "_from": f"session_list/{session_name}",
            "_to": f"{collection_type}/{name}",
        }
        rev_ = utils.guarded_upsert(
            db, name, timestamp, rev_, "session_parent_edge", str(timestamp), {}, doc
        )
    utils.commit_new_timestamp(db, timestamp)


def create_file_list(
    db: StandardDatabase, name: str, session_name: str, session_index: int
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db, ["create_item_list", name, "file_list", session_name, session_index]
    )
    create_item_list(
        db, timestamp, name, session_name, session_index, {}, "file_list"
    )


def create_document_list(
    db: StandardDatabase, name: str, session_name: str, session_index: int
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db, ["create_item_list", name, "document_list", session_name, session_index]
    )
    create_item_list(
        db, timestamp, name, session_name, session_index, {}, "document_list"
    )


def create_embedding_list(
    db: StandardDatabase, name: str, session_name: str, session_index: int, n_dim: int
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db,
        [
            "create_item_list",
            name,
            "embedding_list",
            session_name,
            session_index,
            n_dim,
        ],
    )
    create_item_list(
        db,
        timestamp,
        name,
        session_name,
        session_index,
        {"n_dim": n_dim},
        "embedding_list",
    )


def create_record_list(
    db: StandardDatabase,
    name: str,
    session_name: str,
    session_index: int,
    column_names: List[str],
) -> None:
    timestamp, _ = utils.get_new_timestamp(
        db,
        [
            "create_item_list",
            name,
            "record_list",
            session_name,
            session_index,
            column_names,
        ],
    )
    create_item_list(
        db,
        timestamp,
        name,
        session_name,
        session_index,
        {"column_names": column_names},
        "record_list",
    )


@function_safeguard
def append_item(
    db: StandardDatabase,
    timestamp: int,
    name: str,
    item: Dict[str, Any],
    session_name: str,
    session_index: int,
    input_items: Optional[Dict[str, List[int]]],
    dtype: str,
    index: int,
    start_position: int,
    end_position: int,
    rev_: str,
) -> int:
    item["index"] = index
    item["start_position"] = start_position
    item["end_position"] = end_position
    item["name"] = name
    item["session_name"] = session_name
    item["session_index"] = session_index
    item["timestamp"] = timestamp
    item_key = f"{name}_{index}"
    rev_ = utils.guarded_upsert(
        db, name, timestamp, rev_, dtype, item_key, {}, item
    )
    doc = {
        "timestamp": timestamp,
        "start_position": start_position,
        "end_position": end_position,
        "_from": f"{dtype}_list/{name}",
        "_to": f"{dtype}/{item_key}",
    }
    # if session_name == "test1" and dtype == "file":
    #     import time
    #     print("HELLO TESTING")
    #     time.sleep(60)
    rev_ = utils.guarded_upsert(
        db, name, timestamp, rev_, "parent_edge", str(timestamp), {}, doc
    )
    if session_name != "":
        doc = {
            "timestamp": timestamp,
            "index": session_index,
            "_from": f"session_list/{session_name}",
            "_to": f"{dtype}/{item_key}",
        }
        rev_ = utils.guarded_upsert(
            db, name, timestamp, rev_, "session_parent_edge", str(timestamp), {}, doc
        )
    items = db.collection("items")
    if input_items is not None:
        for itm_name, positions in input_items.items():
            itm = items.get({"_key": itm_name})
            itm_collection = itm["collection"]
            doc = {
                "timestamp": timestamp,
                "start_position": positions[0],
                "end_position": positions[1],
                "_from": f"{itm_collection}/{itm_name}",
                "_to": f"{dtype}/{item_key}",
            }
            rev_ = utils.guarded_upsert(
                db,
                name,
                timestamp,
                rev_,
                "dependency_edge",
                str(timestamp) + "_" + itm_name,
                {},
                doc,
            )
    list_collection = db.collection(f"{dtype}_list")
    item_list = list_collection.get(name)
    if item_list["n_items"] <= index:
        item_list["n_items"] = index + 1
    if item_list["length"] < end_position:
        item_list["length"] = end_position
    rev_ = utils.guarded_upsert(
        db, name, timestamp, rev_, f"{dtype}_list", name, item_list, {}
    )
    item_list = list_collection.get(name)
    utils.commit_new_timestamp(db, timestamp)
    return index


def append_file(
    db: StandardDatabase,
    name: str,
    location: str,
    session_name: str,
    session_index: int,
    index: Optional[int] = None,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
    input_items: Optional[Dict[str, List[int]]] = None,
) -> None:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    file_list = db.collection("file_list").get(name)
    item = {
        "location": location,
    }
    if index is None:
        index = file_list["n_items"]
        start_position = file_list["length"]
        end_position = file_list["length"] + 1

    data = [
        "append_item",
        name,
        "file",
        input_items or {},
        session_name,
        session_index,
        file_list["n_items"],
        file_list["length"],
    ]
    utils.update_timestamp_info(db, timestamp, data)

    append_item(
        db,
        timestamp,
        name,
        item,
        session_name,
        session_index,
        input_items,
        "file",
        index,
        start_position,
        end_position,
        itm["_rev"],
    )


def append_document(
    db: StandardDatabase,
    name: str,
    text: str,
    session_name: str,
    session_index: int,
    index: Optional[int] = None,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
    input_items: Optional[Dict[str, List[int]]] = None,
) -> None:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    document_list = db.collection("document_list").get(name)

    item = {
        "text": text,
    }
    if index is None:
        index = document_list["n_items"]
        start_position = document_list["length"]
        end_position = document_list["length"] + len(text)

    data = [
        "append_item",
        name,
        "document",
        input_items or {},
        session_name,
        session_index,
        document_list["n_items"],
        document_list["length"],
    ]
    utils.update_timestamp_info(db, timestamp, data)

    append_item(
        db,
        timestamp,
        name,
        item,
        session_name,
        session_index,
        input_items,
        "document",
        index,
        start_position,
        end_position,
        itm["_rev"],
    )


# update timestamp
def append_embedding(
    db: StandardDatabase,
    name: str,
    embedding: List[float],
    session_name: str,
    session_index: int,
    index: Optional[int] = None,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
    input_items: Optional[Dict[str, List[int]]] = None,
    build_idx: bool = True,
    index_rebuild_count: int = 10000,
) -> None:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    embedding_list = db.collection("embedding_list").get(name)

    if len(embedding) != embedding_list["n_dim"]:
        raise ValidationError(
            f"Embedding length {len(embedding)} does not match required dimension "
            f"{embedding_list['n_dim']} for list '{name}'.",
            operation="append_embedding",
            collection="embedding_list",
            key=name,
        )
    embedding_name = "embedding_" + str(len(embedding))
    item = {
        embedding_name: embedding,
    }
    if index is None:
        index = embedding_list["n_items"]
        start_position = embedding_list["length"]
        end_position = embedding_list["length"] + 1

    data = [
        "append_item",
        name,
        "embedding",
        input_items or {},
        session_name,
        session_index,
        embedding_list["n_items"],
        embedding_list["length"],
    ]
    utils.update_timestamp_info(db, timestamp, data)

    append_item(
        db,
        timestamp,
        name,
        item,
        session_name,
        session_index,
        input_items,
        "embedding",
        index,
        start_position,
        end_position,
        itm["_rev"],
    )
    if build_idx:
        total_count, index_count = vector_helper.add_one_vector_count(
            db, embedding_name
        )
        if total_count - index_count > index_rebuild_count:
            vector_helper.build_vector_idx(
                db,
                embedding_name,
                len(embedding),
                parallelism=1,
                n_lists=2,
                default_n_probe=1,
                training_iterations=2,
            )
            vector_helper.update_vector_idx(db, embedding_name)


def append_record(
    db: StandardDatabase,
    name: str,
    record: Dict[str, Any],
    session_name: str,
    session_index: int,
    index: Optional[int] = None,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
    input_items: Optional[Dict[str, List[int]]] = None,
) -> None:
    timestamp, itm = utils.get_new_timestamp(db, [], name)
    record_list = db.collection("record_list").get(name)

    if set(record_list["column_names"]) != set(record.keys()):
        expected = set(record_list["column_names"])
        provided = set(record.keys())
        missing = expected - provided
        extra = provided - expected
        details = []
        if missing:
            details.append(f"missing={sorted(missing)}")
        if extra:
            details.append(f"extra={sorted(extra)}")
        detail_msg = "; ".join(details) if details else "column mismatch"
        raise ValidationError(
            f"Record columns do not match record_list '{name}': {detail_msg}.",
            operation="append_record",
            collection="record_list",
            key=name,
        )

    item = {
        "data": record,
        "data_text": str(record),
        "column_names": list(record.keys()),
    }
    if index is None:
        index = record_list["n_items"]
        start_position = record_list["length"]
        end_position = record_list["length"] + 1

    data = [
        "append_item",
        name,
        "record",
        input_items or {},
        session_name,
        session_index,
        record_list["n_items"],
        record_list["length"],
    ]
    utils.update_timestamp_info(db, timestamp, data)

    append_item(
        db,
        timestamp,
        name,
        item,
        session_name,
        session_index,
        input_items,
        "record",
        index,
        start_position,
        end_position,
        itm["_rev"],
    )
