from ml_vault.description import embedding

def query_embedding(embedding, desciption = None, code_text = None, filtered = []):
    aql = r"""
    // --- Global candidate sets (compute once) ---

    LET embCandidates = (
      FOR e IN @@embedding_view
        SEARCH APPROX_NEAR_COSINE(e.embedding_16, @e1, @k1)
        RETURN { _id: e._id, _key: e._key }
    )

    LET descCandidateIds = (
      FOR d IN @@description_view
        SEARCH APPROX_NEAR_COSINE(d.embedding, @e2, @k2)
        RETURN d._id
    )

    // Text candidates. You can swap PHRASE() for TOKENS()/NGRAM_MATCH() depending on your analyzer setup.
    LET sessCandidateIds = (
      FOR s IN @@session_view
        SEARCH ANALYZER(PHRASE(s.text, @t1), @text_analyzer)
        LIMIT @k_text
        RETURN s._id
    )

    FOR e IN embCandidates
      // Descriptions connected to this embedding that are in the vector-matched set
      LET matchedDescriptions = (
        FOR d IN 1..1 OUTBOUND DOCUMENT("embedding/" + e._key) @@edge_emb_desc
          FILTER d._id IN descCandidateIds
          RETURN DISTINCT d._key
      )
      FILTER LENGTH(matchedDescriptions) > 0

      // Sessions reachable via embedding -> session_list -> session that are in the text-matched set
      LET matchedSessions = (
        FOR sl IN 1..1 OUTBOUND DOCUMENT("embedding/" + e._key) @@edge_emb_slist
          FOR s IN 1..1 OUTBOUND sl @@edge_slist_sess
            FILTER s._id IN sessCandidateIds
            RETURN DISTINCT s._key
      )
      FILTER LENGTH(matchedSessions) > 0

      RETURN [e._key, matchedDescriptions, matchedSessions]
    """

    bind_vars: Dict[str, Any] = {
        "e1": list(e1),
        "e2": list(e2),
        "t1": t1,
        "k1": k1,
        "k2": k2,
        "k_text": k_text,
        "text_analyzer": text_analyzer,
        "@embedding_view": embedding_view,
        "@description_view": description_view,
        "@session_view": session_view,
        "@edge_emb_desc": edge_embedding_description,
        "@edge_emb_slist": edge_embedding_session_list,
        "@edge_slist_sess": edge_session_list_session,
    }

    cursor = db.aql.execute(aql, bind_vars=bind_vars, batch_size=1000)
    return list(cursor)

def query_record(record_dict, record_text = None, desciption = None, code_text = None, filtered = []):
    pass

def query_document(desciption = None, code_text = None):
    pass

def query_file(description, code_text):
    pass




def query_artifact_parents(name_keys: list[str], dtype = None, get_data = False):
    pass

def query_artifact_children(name_keys: list[str], dtype = None, get_data = False):
    # input -> [[name, ]]
    pass

def query_artifact_description(name_keys: list[str]):
    pass

def query_artifact_session(name_keys: list[str]):
    pass