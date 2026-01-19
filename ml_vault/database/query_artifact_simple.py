


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


def _query_document_artifact(db, name, start_position, end_position):
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

def _query_record_artifact(db, name, start_position, end_position):
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



def query_artifact_input(db, name: str, collection:str, start_position:Optional[int], end_position:Optional[int]):
    AQL_QUERY_ARTIFACT_DEPENDENCY = r"""
    LET art = DOCUMENT("artifacts", @name)
    LET startId = CONCAT(art.collection, "/", @name)

    FOR child, parentE IN 1..1 OUTBOUND startId parent_edge
    FILTER (@start_position == null OR parentE.start_position >= @start_position)
        AND (@end_position   == null OR parentE.end_position   <= @end_position)

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
        "collection": collection,
        "start_position": start_position,  
        "end_position": end_position,
    }
    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_DEPENDENCY,
        bind_vars=bind_vars,
        batch_size=batch_size,
        ttl=ttl,
        silent=silent,
    )
    return list(cursor)

def query_artifact_output(db, name: str, start_position:Optional[int], end_position:Optional[int]):
    AQL_QUERY_ARTIFACT_CHILDREN = r"""
    LET art = DOCUMENT("artifacts", @name)
    LET startId = CONCAT(art.collection, "/", @name)

    FOR dep, depE IN 1..1 OUTBOUND startId dependency_edge
    FILTER (@start_position == null OR depE.start_position >= @start_position)
        AND (@end_position   == null OR depE.end_position   <= @end_position)

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
        "end_position": end_position,      # None -> AQL null
    }

    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_CHILDREN,
        bind_vars=bind_vars,
        batch_size=batch_size,
    )
    return list(cursor)


def query_artifact_description(db: Any, name: str) -> List[str]:
    AQL_QUERY_ARTIFACT_DESCRIPTION = r"""
    LET art = DOCUMENT("artifacts", @name)
    LET startId = CONCAT(art.collection, "/", @name)

    FOR d IN 1..1 OUTBOUND startId description_edge
    RETURN d.text
    """
    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_DESCRIPTION,
        bind_vars={"name": name_key},
    )
    return list(cursor)

def query_artifact_session(db, name: str, start_position:Optional[int], end_position:Optional[int]):
    AQL_QUERY_ARTIFACT_SESSION = r"""
    LET art = DOCUMENT("artifacts", @name)
    LET startId = CONCAT(art.collection, "/", @name)

    // 1) pick child nodes via parent_edge, optionally filtered by the edge interval
    FOR child, pE IN 1..1 OUTBOUND startId parent_edge
    FILTER (@start_position == null OR pE.start_position >= @start_position)
        AND (@end_position   == null OR pE.end_position   <= @end_position)

    // 2) from each child, follow session_parent_edge and return session node ids
    FOR s IN 1..1 INBOUND child session_parent_edge
        RETURN s._id
    """
    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_SESSION,
        bind_vars={
            "name": name,
            "start_position": start_position,  # None -> AQL null (no filter)
            "end_position": end_position,      # None -> AQL null (no filter)
        },
    )
    return list(cursor)

def query_session_artifact(db, session_name: str):
    aql = r"""
    LET sid = @sid
    FOR v IN 1..1 OUTBOUND sid session_parent_edge
      FILTER v.name != null
      FILTER v.start_position != null AND v.end_position != null
      COLLECT name = v.name AGGREGATE
        minS = MIN(v.start_position),
        maxE = MAX(v.end_position)
      SORT name ASC
      RETURN { name, start_position: minS, end_position: maxE }
    """
    bind_vars = {"sid": session_name}
    return list(db.aql.execute(aql, bind_vars=bind_vars))