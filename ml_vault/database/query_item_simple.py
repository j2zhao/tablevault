from typing import Optional, Any, List, Dict, Union

from arango.database import StandardDatabase
from ml_vault.utils.errors import ValidationError


def _query_session_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
) -> List[Dict[str, Any]]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)
    LET targetId = CONCAT("session_list/", @name)

    FOR s IN session_list
      FILTER s._id == targetId
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position < qEnd)
          AND (!hasStart OR e.end_position   > qStart)
        SORT v.start_position ASC
        RETURN {
          text: v.text,
          status: v.status,
          error: v.error,
          start_position: v.start_position,
          index: v.index
        }
    """

    bind_vars = {
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_file_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int],
    end_position: Optional[int],
) -> List[str]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)
    LET targetId = CONCAT("file_list/", @name)

    FOR s IN file_list
      FILTER s._id == targetId
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position < qEnd)
          AND (!hasStart OR e.end_position   > qStart)
        SORT v.start_position ASC
        RETURN v.location
    """

    bind_vars = {
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_embedding_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int],
    end_position: Optional[int],
) -> List[Optional[List[float]]]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)
    LET targetId = CONCAT("embedding_list/", @name)

    FOR s IN embedding_list
      FILTER TO_STRING(s._id) == targetId

      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position < qEnd)
          AND (!hasStart OR e.end_position   > qStart)
        LET embKey = FIRST(
          FOR k IN ATTRIBUTES(v, true)   // true => include system attrs too; OK either way
            FILTER STARTS_WITH(k, "embedding_")
            SORT k ASC                  // deterministic choice if there are multiple
            RETURN k
        )
        SORT v.start_position ASC
        RETURN embKey != null ? v[embKey] : null
    """

    bind_vars = {
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_document_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int],
    end_position: Optional[int],
) -> List[str]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)
    LET targetId = CONCAT("document_list/", @name)

    FOR s IN document_list
      FILTER s._id == targetId
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position < qEnd)
          AND (!hasStart OR e.end_position   > qStart)
        SORT v.start_position ASC
        RETURN v.text
    """

    bind_vars = {
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_record_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int],
    end_position: Optional[int],
) -> List[Optional[Dict[str, Any]]]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)
    LET targetId = CONCAT("record_list/", @name)

    FOR s IN record_list
      FILTER s._id == targetId
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position < qEnd)
          AND (!hasStart OR e.end_position   > qStart)
        SORT v.start_position ASC
        RETURN  v.data
    """

    bind_vars = {
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_item_list(db: StandardDatabase, name: str) -> Dict[str, Any]:
    items = db.collection("items")
    itm = items.get(name)
    coll_name = itm["collection"]
    coll = db.collection(coll_name)
    return coll.get(name)


def query_item_index(db: StandardDatabase, name: str, index: int) -> Any:
    print('hello')
    items = db.collection("items")
    itm = items.get(name)
    coll_name = itm["collection"]
    if coll_name == "description":
        raise ValidationError(
            "Use query_item_list instead for descriptions.",
            operation="query_item_index",
            collection=coll_name,
            key=name,
        )
    key_ = f"{name}_{index}"
    coll_name = itm["collection"].split("_")[0]
    item = db.collection(coll_name).get(key_)
    if coll_name == "session":
        return {
          "text": item["text"],
          "status": item["status"],
          "error": item["error"],
          "start_position": item["start_position"],
          "index": item["index"]
        }
    elif coll_name == "file":
        return item["location"]
    elif coll_name == "embedding":
        return item["embedding_"]
    elif coll_name == "document":
        return item["text"]
    elif coll_name == "record":
        return item["data"]

def query_item(
    db: StandardDatabase,
    name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
) -> Optional[List[Any]]:
    items = db.collection("items")
    itm = items.get(name)
    coll_name = itm["collection"]
    if coll_name == "description":
        raise ValidationError(
            "Use query_item_list instead for descriptions.",
            operation="query_item",
            collection=coll_name,
            key=name,
        )
    elif coll_name == "session_list":
        return _query_session_item(db, name, start_position, end_position)
    elif coll_name == "file_list":
        return _query_file_item(db, name, start_position, end_position)
    elif coll_name == "embedding_list":
        return _query_embedding_item(db, name, start_position, end_position)
    elif coll_name == "document_list":
        return _query_document_item(db, name, start_position, end_position)
    elif coll_name == "record_list":
        return _query_record_item(db, name, start_position, end_position)


def query_item_input(
    db: StandardDatabase, name: str, start_position: Optional[int], end_position: Optional[int]
) -> List[Any]:
    AQL_QUERY_ITEM_DEPENDENCY = r"""
    LET itm = DOCUMENT("items", @name)
    LET startId = CONCAT(itm.collection, "/", @name)

    FOR child, parentE IN 1..1 OUTBOUND startId parent_edge
    FILTER (@start_position == null OR parentE.start_position > @start_position)
        AND (@end_position   == null OR parentE.end_position   < @end_position)

    FOR dep, depE IN 1..1 INBOUND child dependency_edge
        RETURN [
        parentE.start_position,
        parentE.end_position,
        PARSE_IDENTIFIER(dep._id).collection,
        dep._id,
        depE.start_position,
        depE.end_position
        ]
"""
    bind_vars = {
        "name": name,
        "start_position": start_position,
        "end_position": end_position,
    }
    cursor = db.aql.execute(
        AQL_QUERY_ITEM_DEPENDENCY,
        bind_vars=bind_vars,
    )
    return list(cursor)


def query_item_output(
    db: StandardDatabase, name: str, start_position: Optional[int], end_position: Optional[int]
) -> List[Any]:
    AQL_QUERY_ITEM_CHILDREN = r"""
    LET itm = DOCUMENT("items", @name)
    LET startId = CONCAT(itm.collection, "/", @name)
    FOR dep, depE IN 1..1 OUTBOUND startId dependency_edge
    FILTER (@start_position == null OR depE.start_position > @start_position)
        AND (@end_position   == null OR depE.end_position   < @end_position)

    RETURN [
        depE.start_position,
        depE.end_position,
        PARSE_IDENTIFIER(dep._id).collection,
        dep.name,
        dep.start_position,
        dep.end_position
    ]
    """
    bind_vars = {
        "name": name,
        "start_position": start_position,  # None -> AQL null
        "end_position": end_position,  # None -> AQL null
    }

    cursor = db.aql.execute(
        AQL_QUERY_ITEM_CHILDREN,
        bind_vars=bind_vars,
    )
    return list(cursor)


def query_item_description(db: StandardDatabase, name: str) -> List[str]:
    AQL_QUERY_ITEM_DESCRIPTION = r"""
    LET itm = DOCUMENT("items", @name)
    LET startId = CONCAT(itm.collection, "/", @name)
    FOR d IN 1..1 OUTBOUND startId description_edge
    RETURN d.text
    """
    cursor = db.aql.execute(
        AQL_QUERY_ITEM_DESCRIPTION,
        bind_vars={"name": name},
    )
    return list(cursor)


def query_item_creation_session(db: StandardDatabase, name: str) -> List[Dict[str, Any]]:
    aql = r"""
    LET itm = DOCUMENT(CONCAT("items/", @name))
    LET startId = CONCAT(itm.collection, "/", @name)
    FOR s, sPE IN 1..1 INBOUND startId session_parent_edge
      COLLECT sid = s._id, idx = sPE.index
      RETURN { session_id: sid, index: idx }
    """

    cursor = db.aql.execute(
        aql,
        bind_vars={
            "name": name,
        },
    )
    return list(cursor)


def query_item_session(
    db: StandardDatabase, name: str, start_position: Optional[int], end_position: Optional[int]
) -> List[Dict[str, Any]]:
    aql = r"""
    LET itm = DOCUMENT(CONCAT("items/", @name))
    LET startId = CONCAT(itm.collection, "/", @name)

    FOR child, pE IN 1..1 OUTBOUND startId parent_edge
      FILTER (@start_position == null OR pE.start_position > @start_position)
        AND (@end_position   == null OR pE.end_position   < @end_position)

      FOR s, sPE IN 1..1 INBOUND child session_parent_edge
        COLLECT sid = s._id, idx = sPE.index
        RETURN { session_id: sid, index: idx }
    """

    cursor = db.aql.execute(
        aql,
        bind_vars={
            "name": name,
            "start_position": start_position,
            "end_position": end_position,
        },
    )
    return list(cursor)


def query_session_item(db: StandardDatabase, session_name: str) -> List[Dict[str, Any]]:
    aql = r"""
    LET sid = @sid

    LET candidates = (
      FOR v IN 1..1 OUTBOUND sid session_parent_edge
        FILTER v.name != null
        RETURN v
    )

    LET aggRows = (
      FOR v IN candidates
        FILTER v.start_position != null AND v.end_position != null
        COLLECT name = v.name
        AGGREGATE
          minS = MIN(v.start_position),
          maxE = MAX(v.end_position)
        RETURN { name, start_position: minS, end_position: maxE }
    )

    LET nullRows = (
      FOR v IN candidates
        FILTER v.start_position == null OR v.end_position == null
        COLLECT name = v.name
        RETURN { name, start_position: null, end_position: null }
    )

    FOR row IN UNION_DISTINCT(aggRows, nullRows)
      SORT row.name ASC, row.start_position ASC
      RETURN row
    """

    bind_vars = {"sid": f"session_list/{session_name}"}
    return list(db.aql.execute(aql, bind_vars=bind_vars))
