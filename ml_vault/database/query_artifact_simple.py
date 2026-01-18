


def _query_session_artifact(
    db,
    name: str,
    start_position: Optional[int] = None,
    end_position: Optional[int] = None,
) -> List[Dict[str, Any]]:
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)

    FOR s IN session_list
      FILTER CONTAINS(s._id, @name)
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position <= qEnd)
          AND (!hasStart OR e.end_position   >= qStart)
        SORT v.start_position ASC
        RETURN {
          text: v.text,
          status: v.status,
          error: v.error,
          start_position: v.start_position
        }
    """

    bind_vars = {
        "@session_list": session_list_collection,
        "@edge_col": edge_collection,
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_file_artifact(db, name, start_position, end_position):
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)

    FOR s IN file_list
      FILTER CONTAINS(s._id, @name)
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position <= qEnd)
          AND (!hasStart OR e.end_position   >= qStart)
        SORT v.start_position ASC
        RETURN v.location
    """

    bind_vars = {
        "@session_list": session_list_collection,
        "@edge_col": edge_collection,
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def _query_embedding_artifact(db, name, start_position, end_position):
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)

    FOR s IN embedding_list
      FILTER CONTAINS(TO_STRING(s._id), @name)

      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position <= qEnd)
          AND (!hasStart OR e.end_position   >= qStart)
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


def _query_document_artifact(name, start_position, end_position):
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)

    FOR s IN document_list
      FILTER CONTAINS(s._id, @name)
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position <= qEnd)
          AND (!hasStart OR e.end_position   >= qStart)
        SORT v.start_position ASC
        RETURN v.text,
    """

    bind_vars = {
        "@session_list": session_list_collection,
        "@edge_col": edge_collection,
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))

def _query_record_artifact(name, start_position, end_position):
    aql = r"""
    LET qStart = @qStart
    LET qEnd   = @qEnd

    LET hasStart = (qStart != null)
    LET hasEnd   = (qEnd != null)

    FOR s IN record_list
      FILTER CONTAINS(s._id, @name)
      FOR v, e IN 1..1 OUTBOUND s parent_edge
        FILTER (!hasEnd   OR e.start_position <= qEnd)
          AND (!hasStart OR e.end_position   >= qStart)
        SORT v.start_position ASC
        RETURN  v.data
    """

    bind_vars = {
        "@session_list": session_list_collection,
        "@edge_col": edge_collection,
        "name": name,
        "qStart": start_position,
        "qEnd": end_position,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_artifact_list(db, name):
    artifacts =  db.collection("artifacts")
    art = artifacts.get(name)
    coll_name = art["collection"]
    coll = db.collection(coll_name)
    return coll.get(name) 

def query_artifact(db, name, start_position = None, end_position=None):
    artifacts =  db.collection("artifacts")
    art = artifacts.get(name)
    coll_name = art["collection"]
    if coll_name == "session_list":
        return _query_session_artifact(db, name, start_position, end_position) 
    elif coll_name == "file_list":
        return _query_file_artifact(db, name, start_position, end_position)
    elif coll_name == "embedding_list":
        return _query_embedding_artifact(db, name, start_position, end_position)
    elif  coll_name == "document_list":
        return _query_document_artifact(db, name, start_position, end_position)
    elif coll_name == "record_list":
        return _query_record_artifact(db, name, start_position, end_position)

