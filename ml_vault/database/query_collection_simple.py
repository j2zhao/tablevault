from typing import Any, Dict, List, Optional


def query_session(
    db,
    code_text: Optional[str] = None,
    description_embedding: Optional[Any] = None,
    description_text: Optional[str] = None,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # list of session.name strings
):
    filtered = filtered or []

    use_text = bool(code_text)
    use_desc_vec = description_embedding is not None
    use_desc_txt = bool(description_text)
    use_desc = use_desc_vec or use_desc_txt

    aql = r"""
    LET useText    = @useText
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc

    LET filteredNames = @filtered
    LET hasFilter = LENGTH(filteredNames) > 0

    LET qTokens = TOKENS(@t1, @text_analyzer)

    // --- Session candidates ---
    LET sessionCandidates = (useText && LENGTH(qTokens) > 0) ? (
      FOR s IN session_view
        SEARCH ANALYZER(s.text IN qTokens, @text_analyzer)

        FILTER !hasFilter OR s.name IN filteredNames

        // enforce AND over tokens (post-filter)
        LET sTokens = TOKENS(s.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN { _id: s._id, _key: s._key }
    ) : (
      FOR s IN session
        FILTER !hasFilter OR s.name IN filteredNames
        RETURN { _id: s._id, _key: s._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---
    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.collection == "session_list"
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

    // --- Final: traverse from session -> connected descriptions and enforce AND ---
    FOR s IN sessionCandidates
      LET sesDoc = DOCUMENT(s._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND sesDoc parent_edge
          FOR d IN 1..1 OUTBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      RETURN [s._key, matchedDescriptions]
    """

    bind_vars: Dict[str, Any] = {
        "useText": use_text,
        "t1": code_text or "",
        "useDescVec": use_desc_vec,
        "e2": list(description_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": description_text or "",
        "useDesc": use_desc,
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_embedding(
    db,
    embedding: Optional[Any] = None,
    description_embedding: Optional[Any] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    k1: int = 500,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # list of embedding.name strings
    use_approx: bool = True,  # NEW: toggle approx vs exact
):
    filtered = filtered or []

    use_emb_vec = embedding is not None

    use_desc_vec = description_embedding is not None
    use_desc_txt = bool(description_text)
    use_desc = use_desc_vec or use_desc_txt

    use_text = bool(code_text)

    aql = r"""
    LET useEmbVec  = @useEmbVec
    LET useApprox  = @useApprox
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    LET filteredNames = @filtered
    LET hasFilter = LENGTH(filteredNames) > 0

    // --- Embedding candidates ---
    LET embCandidates = useEmbVec ? (
      FOR e IN embedding
        // safety checks
        FILTER HAS(e, @embedding_field)
        LET vec = e[@embedding_field]
        FILTER IS_ARRAY(vec) && LENGTH(vec) == LENGTH(@e1)

        // toggle: approx (index-backed) vs exact (scan)
        LET score = useApprox
          ? APPROX_NEAR_COSINE(vec, @e1)
          : COSINE_SIMILARITY(vec, @e1)

        FILTER !hasFilter OR e.name IN filteredNames
        SORT score DESC
        LIMIT @k1
        RETURN { _id: e._id, _key: e._key, score: score }
    ) : (
      FOR e IN embedding
        FILTER !hasFilter OR e.name IN filteredNames
        RETURN { _id: e._id, _key: e._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---
    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.collection == "embedding_list"
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
        SEARCH ANALYZER(s.text IN qTokens, @text_analyzer)

        LET sTokens = TOKENS(s.text, @text_analyzer)
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
          FOR d IN 1..1 OUTBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND embDoc session_parent_edge
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """

    embedding_field = None
    e1_list: List[float] = []
    if use_emb_vec:
        e1_list = list(embedding)
        embedding_field = f"embedding_{len(e1_list)}"  # e.g., embedding_16

    bind_vars: Dict[str, Any] = {
        "useEmbVec": use_emb_vec,
        "useApprox": bool(use_approx),
        "e1": e1_list,
        "k1": k1,
        "useDescVec": use_desc_vec,
        "e2": list(description_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": description_text or "",
        "useDesc": use_desc,
        "useText": use_text,
        "t1": code_text or "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
        "embedding_field": embedding_field or "embedding_0",
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_record(
    db,
    record_text: Optional[str] = None,
    description_embedding: Optional[Any] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # list of record.name strings
):
    filtered = filtered or []

    use_record_txt = bool(record_text)  # NEW

    use_desc_vec = description_embedding is not None
    use_desc_txt = bool(description_text)
    use_desc = use_desc_vec or use_desc_txt
    use_text = bool(code_text)

    aql = r"""
    LET useRecordTxt = @useRecordTxt
    LET useDescVec   = @useDescVec
    LET useDescTxt   = @useDescTxt
    LET useDesc      = @useDesc
    LET useText      = @useText

    LET filteredNames = @filtered
    LET hasFilter = LENGTH(filteredNames) > 0

    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET recordCandidates = useRecordTxt ? (
      FOR r IN record_view
        SEARCH ANALYZER(r.data_text IN qTokens, @text_analyzer)

        FILTER !hasFilter OR r.name IN filteredNames

        // enforce AND over tokens (post-filter)
        LET recordTokens = TOKENS(r.data_text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN recordTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN { _id: r._id, _key: r._key }
    ) : (
      FOR r IN record
        FILTER !hasFilter OR r.name IN filteredNames
        RETURN { _id: r._id, _key: r._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---
    LET descVecCandidateIds = useDescVec ? (
      FOR d IN description
        FILTER d.collection == "record_list"
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
    LET sessQTokens = TOKENS(@t2, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(sessQTokens) > 0) ? (
      FOR s IN session_view
        SEARCH ANALYZER(s.text IN sessQTokens, @text_analyzer)

        LET sTokens = TOKENS(s.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN sessQTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(sessQTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from record -> connected descriptions/sessions and enforce "AND" across modalities ---
    FOR r IN recordCandidates
      LET recDoc = DOCUMENT(r._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND recDoc parent_edge
          FOR d IN 1..1 OUTBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND recDoc session_parent_edge
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [r._key, matchedDescriptions, matchedSessions]
    """

    bind_vars: Dict[str, Any] = {
        "useRecordTxt": use_record_txt,  # NEW
        "t1": record_text or "",  # safe
        "useDescVec": use_desc_vec,
        "e2": list(description_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": description_text or "",
        "useDesc": use_desc,
        "useText": use_text,
        "t2": code_text or "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_document(
    db,
    document_text: Optional[str] = None,
    description_embedding: Optional[Any] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # list of document.name strings
):
    filtered = filtered or []

    use_doc_txt = bool(document_text)

    use_desc_vec = description_embedding is not None
    use_desc_txt = bool(description_text)
    use_desc = use_desc_vec or use_desc_txt
    use_text = bool(code_text)

    aql = r"""
    LET useDocTxt  = @useDocTxt
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText

    LET filteredNames = @filtered
    LET hasFilter = LENGTH(filteredNames) > 0

    // --- Document candidates ---
    // If document_text provided: token-AND text hits
    // Else: scan all documents (optionally restricted by filteredNames)
    LET qTokens = TOKENS(@t1, @text_analyzer)

    LET documentCandidates = useDocTxt ? (
      FOR d IN document_view
        SEARCH ANALYZER(d.text IN qTokens, @text_analyzer)

        FILTER !hasFilter OR d.name IN filteredNames

        // enforce AND over tokens (post-filter)
        LET docTokens = TOKENS(d.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN qTokens
            FILTER t IN docTokens
            RETURN 1
        ) == LENGTH(qTokens)

        LIMIT @k_text
        RETURN { _id: d._id, _key: d._key }
    ) : (
      // simplest: scan base collection
      FOR d IN document
        FILTER !hasFilter OR d.name IN filteredNames
        RETURN { _id: d._id, _key: d._key }
    )

    // --- Description candidates: OR (union) of vector hits and token-AND text hits ---
    LET descVecCandidateIds = useDescVec ? (
      FOR x IN description
        FILTER x.collection == "document_list"
        LET score = COSINE_SIMILARITY(x.embedding, @e2)
        SORT score DESC
        LIMIT @k2
        RETURN x._id
    ) : []

    LET descQTokens = TOKENS(@desc_t1, @text_analyzer)

    LET descTxtCandidateIds = (useDescTxt && LENGTH(descQTokens) > 0) ? (
      FOR x IN description_view
        SEARCH ANALYZER(x.text IN descQTokens, @text_analyzer)

        LET xTokens = TOKENS(x.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN descQTokens
            FILTER t IN xTokens
            RETURN 1
        ) == LENGTH(descQTokens)

        LIMIT @k2
        RETURN x._id
    ) : []

    LET descCandidateIds = UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))

    // --- Session candidates: token-AND text hits (optional) ---
    LET sessQTokens = TOKENS(@t2, @text_analyzer)

    LET sessCandidateIds = (useText && LENGTH(sessQTokens) > 0) ? (
      FOR s IN session_view
        SEARCH ANALYZER(s.text IN sessQTokens, @text_analyzer)

        LET sTokens = TOKENS(s.text, @text_analyzer)
        FILTER LENGTH(
          FOR t IN sessQTokens
            FILTER t IN sTokens
            RETURN 1
        ) == LENGTH(sessQTokens)

        LIMIT @k_text
        RETURN s._id
    ) : []

    // --- Final: traverse from documents to connected descriptions/sessions and enforce "AND" across modalities ---
    FOR d IN documentCandidates
      LET txtDoc = DOCUMENT(d._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND txtDoc parent_edge
          FOR x IN 1..1 OUTBOUND sl description_edge
            FILTER x._id IN descCandidateIds
            RETURN DISTINCT x._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND txtDoc session_parent_edge
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [d._key, matchedDescriptions, matchedSessions]
    """

    bind_vars: Dict[str, Any] = {
        "useDocTxt": use_doc_txt,
        "t1": document_text or "",
        "useDescVec": use_desc_vec,
        "e2": list(description_embedding) if use_desc_vec else [],
        "k2": k2,
        "useDescTxt": use_desc_txt,
        "desc_t1": description_text or "",
        "useDesc": use_desc,
        "useText": use_text,
        "t2": code_text or "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_file(
    db,
    description_embedding: Optional[Any] = None,
    description_text: Optional[str] = None,
    code_text: Optional[str] = None,
    k1: int = 500,  # kept for API compatibility; not used
    k2: int = 500,
    k_text: int = 500,
    text_analyzer: str = "text_en",
    filtered: Optional[List[str]] = None,  # list of file.name strings
):
    filtered = filtered or []

    use_desc_vec = description_embedding is not None
    use_desc_txt = bool(description_text)
    use_desc = use_desc_vec or use_desc_txt
    use_text = bool(code_text)

    has_any_constraint = use_desc or use_text  # NEW: any modality filter at all?

    aql = r"""
    LET useDescVec = @useDescVec
    LET useDescTxt = @useDescTxt
    LET useDesc    = @useDesc
    LET useText    = @useText
    LET hasAny     = @hasAny

    LET filteredNames = @filtered
    LET hasFilter = LENGTH(filteredNames) > 0

    // --- File candidates (optionally restricted by filtered names) ---
    LET fileCandidates = (
      FOR f IN file
        FILTER !hasFilter OR f.name IN filteredNames
        RETURN { _id: f._id, _key: f._key }
    )

    // If no desc/text constraints at all, skip building candidate sets and just return files.
    // (This avoids wasted work + avoids scanning views.)
    LET descCandidateIds = hasAny ? (
      LET descVecCandidateIds = useDescVec ? (
        FOR d IN description
          FILTER d.collection == "file_list"
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

      RETURN UNIQUE(APPEND(descVecCandidateIds, descTxtCandidateIds))
    ) : []

    LET sessCandidateIds = (hasAny && useText) ? (
      LET qTokens = TOKENS(@t1, @text_analyzer)

      RETURN (LENGTH(qTokens) > 0) ? (
        FOR s IN session_view
          SEARCH ANALYZER(s.text IN qTokens, @text_analyzer)

          LET sTokens = TOKENS(s.text, @text_analyzer)
          FILTER LENGTH(
            FOR t IN qTokens
              FILTER t IN sTokens
              RETURN 1
          ) == LENGTH(qTokens)

          LIMIT @k_text
          RETURN s._id
      ) : []
    ) : []

    // --- Final: traverse from file docs to connected descriptions/sessions and enforce filters ---
    FOR f IN fileCandidates
      LET fileDoc = DOCUMENT(f._id)

      LET matchedDescriptions = useDesc ? (
        FOR sl IN 1..1 INBOUND fileDoc parent_edge
          FOR d IN 1..1 OUTBOUND sl description_edge
            FILTER d._id IN descCandidateIds
            RETURN DISTINCT d._key
      ) : []
      FILTER (!useDesc) OR (LENGTH(matchedDescriptions) > 0)

      LET matchedSessions = useText ? (
        FOR sl IN 1..1 INBOUND fileDoc session_parent_edge
          FOR s IN 1..1 OUTBOUND sl parent_edge
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      ) : []
      FILTER (!useText) OR (LENGTH(matchedSessions) > 0)

      RETURN [f._key, matchedDescriptions, matchedSessions]
    """

    bind_vars: Dict[str, Any] = {
        "useDescVec": use_desc_vec,
        "useDescTxt": use_desc_txt,
        "useDesc": use_desc,
        "useText": use_text,
        "hasAny": has_any_constraint,  # NEW
        "e2": list(description_embedding) if use_desc_vec else [],
        "k2": k2,
        "desc_t1": description_text or "",
        "t1": code_text or "",
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "filtered": filtered,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))
