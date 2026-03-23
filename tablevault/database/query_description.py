from typing import Any, Dict, List, Optional


def query_description_token(
    db,
    description_text: str,
    k: int = 500,
    text_analyzer: str = "text_en",
) -> List[Any]:
    """Search descriptions by token AND match on text field, across all data types.

    Returns list of [description_name, description_text, list_name, list_type].
    """
    aql = r"""
    LET qTokens = TOKENS(@description_text, @text_analyzer)

    FOR d IN description_view
      SEARCH ANALYZER(d.text IN qTokens, @text_analyzer)

      LET dTokens = TOKENS(d.text, @text_analyzer)
      FILTER LENGTH(
        FOR t IN qTokens
          FILTER t IN dTokens
          RETURN 1
      ) == LENGTH(qTokens)

      LIMIT @k
      RETURN [d.name, d.text, d.item_name, d.collection]
    """

    bind_vars: Dict[str, Any] = {
        "description_text": description_text,
        "text_analyzer": text_analyzer,
        "k": k,
    }

    return list(db.aql.execute(aql, bind_vars=bind_vars))


def query_description_embedding(
    db,
    embedding: Any,
    k: int = 500,
    use_approx: bool = False,
) -> List[Any]:
    """Search descriptions by cosine similarity of embedding, across all data types.

    Returns list of [description_name, description_text, list_name, list_type].
    """
    aql_template = r"""
    FOR d IN description
      LET score = __SCORE_FN__(d.embedding, @embedding)
      SORT score DESC
      LIMIT @k
      RETURN [d.name, d.text, d.item_name, d.collection]
    """

    bind_vars: Dict[str, Any] = {
        "embedding": list(embedding),
        "k": k,
    }

    def _run_query(score_fn: str) -> List[Any]:
        aql = aql_template.replace("__SCORE_FN__", score_fn)
        return list(db.aql.execute(aql, bind_vars=bind_vars))

    if not use_approx:
        return _run_query("COSINE_SIMILARITY")

    try:
        return _run_query("APPROX_NEAR_COSINE")
    except Exception as exc:
        msg = str(exc).lower()
        if "approx_near_cosine" in msg and ("unknown function" in msg or "vector index" in msg):
            return _run_query("COSINE_SIMILARITY")
        raise
