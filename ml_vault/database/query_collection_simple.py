from typing import Any, Dict, List, Optional

def query_embedding_no_idx(
    db,
    embedding,
    desciption_embedding: Optional[Any] = None,
    desciption_text: Optional[str] = None,
    text_code: Optional[str] = None,
    k1: int = 500,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # expects embedding _id strings like "embedding/123"
):
    filtered = filtered or []

    use_desc_vec = desciption_embedding is not None
    use_desc_txt = bool(desciption_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(text_code)

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    // --- Embedding candidates (vector top-k) ---
    LET embCandidates = (
      FOR e IN embedding
        LET score = COSINE_SIMILARITY(e.@@embedding_field, @e1)
        SORT score DESC
        LIMIT @k1
        RETURN {_id: e._id, _key: e._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---

    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.artifact_collection == "embedding_list"
        LET score = COSINE_SIMILARITY(d.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN d._id
    ) : []

    LET descQTokens = TOKENS(@desc_t1, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR d IN description_view
        SEARCH ANALYZER(d.text IN descQTokens, @text_analyzer)
        LET dTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN dTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN d._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        SEARCH ANALYZER(s.code IN qTokens, @text_analyzer)
        LET sTokens = TOKENS(s.code, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from embeddings to connected descriptions/sessions and enforce filters ---
    FOR e IN embCandidates
      LET embDoc = DOCUMENT(e._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND embDoc parent_edge
          FOR d IN 1..1 INBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
        ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND embDoc write_artifact
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """
    embedding_field = "embedding_" + str(len(embedding))
    
    bind_vars: Dict[str, Any] = {
        "e1": list(embedding),
        "k1": k1,
        "useDescVec": use_desc_vec,
        "e2": list(desciption_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": desciption_text if use_desc_txt else "",
        "useDesc": use_desc,
        "useText": use_text,
        "t1": text_code if use_text else "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
        "@embedding_field": embedding_field
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars)
    return list(cursor)


def query_embedding(
    db,
    embedding,
    desciption_embedding: Optional[Any] = None,
    desciption_text: Optional[str] = None,
    text_code: Optional[str] = None,
    k1: int = 500,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # expects embedding _id strings like "embedding/123"
):
    filtered = filtered or []

    use_desc_vec = desciption_embedding is not None
    use_desc_txt = bool(desciption_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(text_code)

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    // --- Embedding candidates (vector top-k) ---
    LET embCandidates = (
      FOR e IN embedding
        SEARCH APPROX_NEAR_COSINE(e.@@embedding_field, @e1, @k1)
        FILTER (LENGTH(@filtered) == 0) OR (e._id IN @filtered)
        RETURN {_id: e._id, _key: e._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---

    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.artifact_collection == "embedding_list"
        LET score = COSINE_SIMILARITY(d.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN d._id
    ) : []

    LET descQTokens = TOKENS(@desc_t1, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR d IN description_view
        SEARCH ANALYZER(d.text IN descQTokens, @text_analyzer)
        LET dTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN dTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN d._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        SEARCH ANALYZER(s.code IN qTokens, @text_analyzer)
        LET sTokens = TOKENS(s.code, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from embeddings to connected descriptions/sessions and enforce filters ---
    FOR e IN embCandidates
      LET embDoc = DOCUMENT(e._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND embDoc parent_edge
          FOR d IN 1..1 INBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
        ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND embDoc write_artifact
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """
    embedding_field = "embedding_" + str(len(embedding))
    
    bind_vars: Dict[str, Any] = {
        "e1": list(embedding),
        "k1": k1,
        "useDescVec": use_desc_vec,
        "e2": list(desciption_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": desciption_text if use_desc_txt else "",
        "useDesc": use_desc,
        "useText": use_text,
        "t1": text_code if use_text else "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
        "@embedding_field": embedding_field
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars)
    return list(cursor)

def query_record(
    db,
    record_text,
    desciption_embedding: Optional[Any] = None,
    desciption_text: Optional[str] = None,
    text_code: Optional[str] = None,
    k1: int = 500,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # expects embedding _id strings like "embedding/123"
):
    filtered = filtered or []

    use_desc_vec = desciption_embedding is not None
    use_desc_txt = bool(desciption_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(text_code)

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    // --- Record candidates ---
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET recordCandidates = (useText && LENGTH(qTokens) > 0) ? (
      FOR r IN record_view
        SEARCH ANALYZER(r.data_text IN qTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET recordTokens = TOKENS(r.data_text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN recordTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN { _id: e._id, _key: e._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---

    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.artifact_collection == "embedding_list"
        LET score = COSINE_SIMILARITY(d.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN d._id
    ) : []

    LET descQTokens = TOKENS(@desc_t2, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR d IN description_view
        // Stage 1: indexed retrieval (OR) at least one token
        SEARCH ANALYZER(d.text IN descQTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET dTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN dTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN d._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET sessQTokens = TOKENS(@t2, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        // Stage 1: indexed retrieval (OR) – at least one token
        SEARCH ANALYZER(s.code IN qTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET sTokens = TOKENS(s.code, @text_analyzer)
        FILTER LENGTH(
          FOR t IN sessQTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(sessQTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from embeddings to connected descriptions/sessions and enforce filters ---
    FOR e IN recordCandidates
      LET recDoc = DOCUMENT(e._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND recDoc parent_edge
          FOR d IN 1..1 INBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND recDoc write_artifact
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """    
    bind_vars: Dict[str, Any] = {
        "t1": record_text,
        "k1": k1,
        "useDescVec": use_desc_vec,
        "e2": list(desciption_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t2": desciption_text if use_desc_txt else "",
        "useDesc": use_desc,
        "useText": use_text,
        "t2": text_code if use_text else "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars)
    return list(cursor)

def query_document(db,
    document_text,
    desciption_embedding: Optional[Any] = None,
    desciption_text: Optional[str] = None,
    text_code: Optional[str] = None,
    k1: int = 500,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # expects embedding _id strings like "embedding/123"
):
    filtered = filtered or []

    use_desc_vec = desciption_embedding is not None
    use_desc_txt = bool(desciption_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(text_code)

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    // --- Record candidates ---
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET documentCandidates = (useText && LENGTH(qTokens) > 0) ? (
      FOR r IN document_view
        SEARCH ANALYZER(r.text IN qTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET docTokens = TOKENS(r.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN docTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN { _id: e._id, _key: e._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---

    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.artifact_collection == "embedding_list"
        LET score = COSINE_SIMILARITY(d.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN d._id
    ) : []

    LET descQTokens = TOKENS(@desc_t2, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR d IN description_view
        // Stage 1: indexed retrieval (OR) at least one token
        SEARCH ANALYZER(d.text IN descQTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET dTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN dTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN d._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET sessQTokens = TOKENS(@t2, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        // Stage 1: indexed retrieval (OR) – at least one token
        SEARCH ANALYZER(s.code IN qTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET sTokens = TOKENS(s.code, @text_analyzer)
        FILTER LENGTH(
          FOR t IN sessQTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(sessQTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from embeddings to connected descriptions/sessions and enforce filters ---
    FOR e IN documentCandidates
      LET txtDoc = DOCUMENT(e._id)

      LET matchedDescriptions = useDesc ? (
        FOR d IN 1..1 OUTBOUND txtDoc description_edge
          FILTER d._id IN descCandidateIds
          RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND txtDoc write_artifact
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """    
    bind_vars: Dict[str, Any] = {
        "t1": document_text,
        "k1": k1,
        "useDescVec": use_desc_vec,
        "e2": list(desciption_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t2": desciption_text if use_desc_txt else "",
        "useDesc": use_desc,
        "useText": use_text,
        "t2": text_code if use_text else "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars)
    return list(cursor)

def query_file(
    db,
    embedding,  # kept for API compatibility; no longer used for file candidates
    desciption_embedding: Optional[Any] = None,
    desciption_text: Optional[str] = None,
    text_code: Optional[str] = None,
    k1: int = 500,      # kept for API compatibility; no longer used
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # kept for API compatibility; no longer used
):
    filtered = filtered or []

    use_desc_vec = desciption_embedding is not None
    use_desc_txt = bool(desciption_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(text_code)

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    // --- File candidates (NO filtering; scan all) ---
    LET fileCandidates = (
      FOR f IN file
        RETURN { _id: f._id, _key: f._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---

    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.artifact_collection == "embedding_list"
        LET score = COSINE_SIMILARITY(d.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN d._id
    ) : []

    LET descQTokens = TOKENS(@desc_t1, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR d IN description_view
        // Stage 1: indexed retrieval (OR) – at least one token
        SEARCH ANALYZER(d.text IN descQTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET dTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN dTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN d._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        // Stage 1: indexed retrieval (OR) – at least one token
        SEARCH ANALYZER(s.code IN qTokens, @text_analyzer)

        // Stage 2: enforce AND over tokens (post-filter)
        LET sTokens = TOKENS(s.code, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from file docs to connected descriptions/sessions and enforce filters ---
    FOR f IN fileCandidates
      LET fileDoc = DOCUMENT(f._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND fileDoc parent_edge
          FOR d IN 1..1 INBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND fileDoc write_artifact
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [f._key, matchedDescriptions, matchedSessions]
    """

    bind_vars: Dict[str, Any] = {
        "useDescVec": use_desc_vec,
        "e2": list(desciption_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": desciption_text if use_desc_txt else "",
        "useDesc": use_desc,
        "useText": use_text,
        "t1": text_code if use_text else "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars)
    return list(cursor)


def query_artifact_input(name: str, collection:str, start_position:Optional[int], end_position:Optional[int]):
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

def query_artifact_output(name_key: str, start_position:Optional[int], end_position:Optional[int]):
    AQL_QUERY_ARTIFACT_CHILDREN = r"""
    LET art = DOCUMENT("artifacts", @name_key)
    LET startId = CONCAT(art.collection, "/", @name_key)

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
        "name_key": name_key,
        "start_position": start_position,  # None -> AQL null
        "end_position": end_position,      # None -> AQL null
    }

    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_CHILDREN,
        bind_vars=bind_vars,
        batch_size=batch_size,
    )
    return list(cursor)


def query_artifact_description(db: Any, name_key: str) -> List[str]:
    AQL_QUERY_ARTIFACT_DESCRIPTION = r"""
    LET art = DOCUMENT("artifacts", @name_key)
    LET startId = CONCAT(art.collection, "/", @name_key)

    FOR d IN 1..1 OUTBOUND startId description_edge
    RETURN d.text
    """
    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_DESCRIPTION,
        bind_vars={"name_key": name_key},
    )
    return list(cursor)

def query_artifact_session(name_key: str, start_position:Optional[int], end_position:Optional[int]):
    AQL_QUERY_ARTIFACT_SESSION = r"""
    LET art = DOCUMENT("artifacts", @name_key)
    LET startId = CONCAT(art.collection, "/", @name_key)

    // 1) pick child nodes via parent_edge, optionally filtered by the edge interval
    FOR child, pE IN 1..1 OUTBOUND startId parent_edge
    FILTER (@start_position == null OR pE.start_position >= @start_position)
        AND (@end_position   == null OR pE.end_position   <= @end_position)

    // 2) from each child, follow session_parent_edge and return session node ids
    FOR s IN 1..1 OUTBOUND child session_parent_edge
        RETURN s._id
    """
    cursor = db.aql.execute(
        AQL_QUERY_ARTIFACT_SESSION,
        bind_vars={
            "name_key": name_key,
            "start_position": start_position,  # None -> AQL null (no filter)
            "end_position": end_position,      # None -> AQL null (no filter)
        },
    )
    return list(cursor)
