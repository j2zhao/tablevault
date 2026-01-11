base_aql = """
////////////////////////////////////////////////////////////////////////////////
// FULL QUERY: Option 1 + Option 5
// - Option 1: Flatten groups + (optional) per-collection text/vector candidates (branching)
// - Option 5: Build relByColl once per object to avoid scanning all rel for each group
//
// Placeholders you must replace:
// - childA_text_view, childB_text_view (ArangoSearch Views)
// - childA, childB (Collections that store embeddings field "embedding")
// - Vector similarity function name may differ by ArangoDB version/index.
//
// Bind params expected (example):
// @specByCollection = {
//   "childA": [ { filters:[...], text:{...}, vector:{...} }, ... ],
//   "childB": [ { filters:[...], text:{...} }, ... ],
//   ...
// }
// Optional: @witnessLimit
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// 0) Flatten groups into one array with a "collection" tag
////////////////////////////////////////////////////////////////////////////////
LET flatGroups = FLATTEN(
  FOR c IN ATTRIBUTES(@specByCollection)
    FOR g IN @specByCollection[c]
      RETURN MERGE(g, { collection: c })
)

////////////////////////////////////////////////////////////////////////////////
// 1) Precompute candidate IDs once per group (queries same for all objects)
//    candidateIdsByGroup[i] is:
//      - null => no retrieval restriction (no text/vector provided)
//      - []   => retrieval requested but got no hits (group will fail later)
//      - [ids] => restrict to these ids
////////////////////////////////////////////////////////////////////////////////
LET candidateIdsByGroup = (
  FOR g IN flatGroups

    // TEXT candidates per collection view (branch)
    LET textIds =
      (HAS(g, "text") && HAS(g.text, "query") && g.text.query != null && g.text.query != "")
        ? (
            g.collection == "childA" ? (
              FOR d IN childA_text_view
                SEARCH ANALYZER(
                  d[g.text.field] LIKE CONCAT("%", g.text.query, "%"),
                  "text_en"
                )
                SORT BM25(d) DESC
                LIMIT (HAS(g.text, "topK") ? g.text.topK : 200)
                RETURN d._id
            ) :
            g.collection == "childB" ? (
              FOR d IN childB_text_view
                SEARCH ANALYZER(
                  d[g.text.field] LIKE CONCAT("%", g.text.query, "%"),
                  "text_en"
                )
                SORT BM25(d) DESC
                LIMIT (HAS(g.text, "topK") ? g.text.topK : 200)
                RETURN d._id
            ) :
            []
          )
        : []

    // VECTOR candidates per collection/index (branch)
    LET vectorIds =
      (HAS(g, "vector") && IS_ARRAY(g.vector.query))
        ? (
            g.collection == "childA" ? (
              FOR d IN childA
                LET sim = APPROX_NEAR_COSINE(d.embedding, g.vector.query)
                FILTER !HAS(g.vector, "minSim") OR sim >= g.vector.minSim
                SORT sim DESC
                LIMIT (HAS(g.vector, "topK") ? g.vector.topK : 200)
                RETURN d._id
            ) :
            g.collection == "childB" ? (
              FOR d IN childB
                LET sim = APPROX_NEAR_COSINE(d.embedding, g.vector.query)
                FILTER !HAS(g.vector, "minSim") OR sim >= g.vector.minSim
                SORT sim DESC
                LIMIT (HAS(g.vector, "topK") ? g.vector.topK : 200)
                RETURN d._id
            ) :
            []
          )
        : []

    LET hasText = LENGTH(textIds) > 0
    LET hasVec  = LENGTH(vectorIds) > 0

    RETURN
      (hasText && hasVec) ? INTERSECTION(textIds, vectorIds) :
      (hasText)           ? textIds :
      (hasVec)            ? vectorIds :
      (HAS(g, "text") || HAS(g, "vector")) ? [] : null
)

////////////////////////////////////////////////////////////////////////////////
// 2) Scan objects
////////////////////////////////////////////////////////////////////////////////
FOR o IN objects

  // Pull 1-hop children once and tag with collection
  LET rel = (
    FOR v, e IN 1..1 OUTBOUND o object_to_child
      FILTER e.start_position != null AND e.end_position != null
      FILTER e.start_position < e.end_position
      LET coll = PARSE_IDENTIFIER(v)._collection
      RETURN { v, e, coll }
  )

  // Option 5: group rel by collection so each group only scans relevant children
  LET relByColl = MERGE(
    FOR r IN rel
      COLLECT c = r.coll INTO g = r
      // g is array of "r" values
      RETURN { [c]: g }
  )

  // For each group i, compute ms from only its collection bucket
  LET matchesByGroup = (
    FOR i IN 0..LENGTH(flatGroups)-1
      LET g = flatGroups[i]
      LET candidateIds = candidateIdsByGroup[i]
      LET filters = (HAS(g, "filters") && IS_ARRAY(g.filters)) ? g.filters : []

      LET bucket = HAS(relByColl, g.collection) ? relByColl[g.collection] : []

      LET ms = (
        FOR r IN bucket
          // optional retrieval restriction
          FILTER candidateIds == null OR r.v._id IN candidateIds

          // structured filters (AND)
          FILTER ALL(f IN filters SATISFIES
            (
              f.op == "eq"    ? r.v[f.field] == f.value :
              f.op == "in"    ? (IS_ARRAY(f.values) AND r.v[f.field] IN f.values) :
              f.op == "range" ? (r.v[f.field] >= f.min AND r.v[f.field] <= f.max) :
              false
            )
          END)
          RETURN r
      )

      RETURN ms
  )

  // Every group must have >= 1 match
  FILTER ALL(ms IN matchesByGroup SATISFIES LENGTH(ms) > 0 END)

  // Candidate overlap points: all start_positions across all groups
  LET candidates = UNIQUE(
    FLATTEN(
      FOR ms IN matchesByGroup
        RETURN ms[*].e.start_position
    )
  )

  // Find smallest x such that every group has some interval containing x
  LET x = FIRST(
    FOR c IN candidates
      FILTER ALL(ms IN matchesByGroup SATISFIES
        LENGTH(
          FOR r IN ms
            FILTER r.e.start_position <= c AND c < r.e.end_position
            LIMIT 1
            RETURN 1
        ) > 0
      END)
      SORT c ASC
      LIMIT 1
      RETURN c
  )

  FILTER x != null

  // Common overlap interval [x, overlap_end)
  LET overlap_end = MIN(
    FOR ms IN matchesByGroup
      RETURN MIN(
        FOR r IN ms
          FILTER r.e.start_position <= x AND x < r.e.end_position
          RETURN r.e.end_position
      )
  )

  // Multiple witnesses per group: all matches covering x (optionally cap)
  LET witnessesByGroup = (
    FOR ms IN matchesByGroup
      RETURN (
        FOR r IN ms
          FILTER r.e.start_position <= x AND x < r.e.end_position
          SORT r.e.start_position ASC, r.e.end_position ASC
          // LIMIT @witnessLimit
          RETURN r
      )
  )

  RETURN {
    o,
    overlap: { start: x, end: overlap_end },
    groups: flatGroups,
    witnessesByGroup
  }

"""

def file_query_generator():
    """
    FOR t IN @types
      LET spec = HAS(@specs, t) ? @specs[t] : { }
      LET items = HAS(m, t) ? m[t] : [ ]
      LET filtered = (
        FOR r IN items
          FILTER !HAS(spec, "eq") OR r.v[spec.eq.field] == spec.eq.value
          FILTER !HAS(spec, "in") OR r.v[spec.in.field] IN spec.in.values
          FILTER !HAS(spec, "range") OR (
            r.v[spec.range.field] >= spec.range.min AND
            r.v[spec.range.field] <= spec.range.max
          )
          // add more optional clauses as needed
          RETURN r
      )
      RETURN { [t]: filtered }
    )
    """

def embedding_query_generator():
    pass

def document_query_generator():
    pass

def record_query_generator():
    pass

def description_query_generator():
    pass

def session_query_generator():
    pass