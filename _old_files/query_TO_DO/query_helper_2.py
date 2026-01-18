def query_child_file(parent_name,
    parent_collection,
    description,
    description_embedding,
    name_filter, 
    timestamp, 
    query_num, 
    dependent):
    f"""
        LET description_{query_num} = (
        LET d1 = (
            FOR d IN description_view
            SEARCH ANALYZER(
                d.description LIKE CONCAT("%", {description}, "%"),
                "text_en"
            )
            SORT BM25(d) DESC
            LIMIT 30
            RETURN d._id
        )
        LET d2 = (
            FOR d IN description
            LET sim = APPROX_NEAR_COSINE(d.embedding, {description_embedding})
            SORT sim DESC
            LIMIT 30
            RETURN d._id
        )
        LET d3 = INTERSECTION(d1, d2)
        RETURN (
            FOR docId IN d3
            LET doc = DOCUMENT(docId)
            FOR v, e IN 1..1 INBOUND doc description_edge
                FILTER PARSE_IDENTIFIER(v)._collection == {parent_collection}
                RETURN {{
                text: doc.description,
                embedding: doc.embedding,
                start_position: e.start_position,
                end_position: e.end_position,
                parent: v
                }}
            )
        )
        """
        """
        LET candidates_{query_num} = (
        FOR obj IN file_view
            SEARCH obj.name == {name_filter}
            AND obj.timestamp <= {timestamp}
            AND parent_name in obj.parents

            FOR v, e IN 1..1 INBOUND obj {parent_collection}_edge
                FILTER v.name == {parent_name}
                FILTER e.type = {dtype}
            
                FOR v2, e2 IN 1..1 INBOUND obj description_edge
                    FILTER v2._id in {parent_name}
                        RETURN {{child: obj, edge: e }}
        )
        """

def query():
    """
    LET groups = [
        candidates_0,
        candidates_1,
        candidates_2
        // ...
    ]
    LET sets = REDUCE(
        acc = [ { items: [], start: -1e18, end: 1e18 } ],
        ms IN cleanGroups :
            (
            LET next = FLATTEN(
                FOR partial IN acc
                FOR r IN ms
                    LET s = MAX([ partial.start, r.edge.start_position ])
                    LET e = MIN([ partial.end,   r.edge.end_position   ])
                    FILTER s < e
                    RETURN {
                    items: PUSH(partial.items, r),  // append this group's chosen candidate
                    start: s,
                    end: e
                    }
            )
            RETURN next
            )
        )
    """