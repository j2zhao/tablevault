from arango.database import StandardDatabase

def ensure_vector_index(db, embedding_dimension):
    view = db.view("embedding_vector_view")
    props = view.properties()
    links = props.get('links', {})
    
    field_name = f"embedding_{embedding_dimension}"
    
    current_fields = links.get("embedding_vector", {}).get("fields", {})
    if field_name in current_fields and "indices" in current_fields[field_name]:
        return field_name

    links["embedding_vector"]["fields"][field_name] = {
        "indices": [{
            "type": "vector",
            "dimensions": embedding_dimension,
            "distance_metric": "cosine",
            "provider": "hnsw"
        }]
    }
    
    view.replace_properties({"links": links})
    return field_name

def create_session_views(db: StandardDatabase, d_embedding_size: int):
    db.create_arangosearch_view(
        name="session_code_view",
        properties={
            "links": {
                "session_code": {
                    "includeAllFields": False,
                    "storeValues": "id", 
                    "fields": {
                        "name": {
                            "analyzers": ["identity"], 
                            "storeValues": "none"
                        },
                        "index": {
                            "analyzers": ["identity"]
                        },
                        "text": {
                            "analyzers": ["text_en"], 
                            "storeValues": "positions"
                        },
                        "status": {
                            "analyzers": ["identity"], 
                            "storeValues": "none"
                        },
                        "error": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "timestamp": {
                            "analyzers": ["identity"]
                        },
                        "last_timestamp": {
                            "analyzers": ["identity"]
                        },
                        "position_start": {
                            "analyzers": ["identity"]
                        },
                        "position_end": {
                            "analyzers": ["identity"]
                        },
                    }
                }
            },
            "primarySort": [
                {"field": "name", "direction": "asc"},
                {"field": "index", "direction": "asc"}
            ],
            "storedValues": [
                {"fields": ["timestamp", "position_start", "position_end", "status"]}
            ]
        }
    )

    db.create_arangosearch_view(
        name="record_view",
        properties={
            "links": {
                "record": {
                    "includeAllFields": False,
                    "storeValues": "id", 
                    "fields": {
                        "session_name": {
                            "analyzers": ["identity"], 
                            "storeValues": "none"
                        },
                        "name": {
                            "analyzers": ["identity"], 
                            "storeValues": "none"
                        },
                        "timestamp": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },

                        "value_str": {
                            "analyzers": ["text_en"],  
                            "storeValues": "positions"
                        },
                        "value_num": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "value_bool": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        }
                    }
                }
            },

            "primarySort": [
                {"field": "session_name", "direction": "asc"},
                {"field": "name", "direction": "asc"},
                {"field": "index", "direction": "asc"}
            ],

            "storedValues": [
                {
                    "fields": [
                        "timestamp",
                    ]
                }
            ]
        }
    )

    db.create_arangosearch_view(
        name = "embedding_vector_view",
        properties = {
            "links": {
                "embedding_vector": {
                    "includeAllFields": False,
                    "fields": {
                        "name": {"analyzers": ["identity"]},
                        "index": {"analyzers": ["identity"]},
                        "timestamp": {"analyzers": ["identity"]},
                        "position_start": {"analyzers": ["identity"]},
                        "position_end": {"analyzers": ["identity"]}
                    }
                }
            },
            "primarySort": [
                {"field": "name", "direction": "asc"},
                {"field": "index", "direction": "asc"}
            ],
            "storedValues": [
                {"fields": ["timestamp", "position_start", "position_end"]}
            ]
        }
    )

    db.create_arangosearch_view(
        name="document_chunk_view",
        properties={
            "links": {
                "document_chunk": {
                    "includeAllFields": False,
                    "storeValues": "id", 
                    "fields": {
                        "name": {
                            "analyzers": ["identity"],
                            "storeValues": "none",
                        },
                        "timestamp": {
                            "analyzers": ["identity"],
                            "storeValues": "none",
                        },
                        "index": {
                            "analyzers": ["identity"],
                            "storeValues": "none",
                        },
                        "start_position": {
                            "analyzers": ["identity"],
                            "storeValues": "none",
                        },
                        "end_position": {
                            "analyzers": ["identity"],
                            "storeValues": "none",
                        },
                        "text": {
                            "analyzers": ["text_en"],
                            "storeValues": "positions",
                        },
                    },
                }
            },

            "primarySort": [
                {"field": "name", "direction": "asc"},
                {"field": "index", "direction": "asc"},
            ],

            "storedValues": [
                {
                    "fields": [
                        "timestamp",
                        "start_position",
                        "end_position",
                    ]
                }
            ],
        },
    )

    db.create_arangosearch_view(
        name="record_item_view",
        properties={
            "links": {
                "record_item": {
                    "includeAllFields": False,
                    "storeValues": "id",
                    "fields": {
                        "name": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "index": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "timestamp": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "data_text": {
                            "analyzers": ["text_en"], 
                            "storeValues": "none" 
                        },
                        "data": {
                            "includeAllFields": True, 
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "column_names": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        }
                    }
                }
            },
            "primarySort": [
                {"field": "name", "direction": "asc"},
                {"field": "index", "direction": "asc"} 
            ],
            "storedValues": [
                {"fields": [ "timestamp"]}
            ]
        }
    )

    db.create_arangosearch_view(
        name="description_view",
        properties={
            "links": {
                "description": {
                    "includeAllFields": False,
                    "storeValues": "id",
                    "fields": {
                        "text": {
                            "analyzers": ["text_en"],
                            "storeValues": "positions"
                        },
                        "session_name": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "item_name": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "collection": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "type": {
                            "analyzers": ["identity"],
                            "storeValues": "none"
                        },
                        "timestamp": { "analyzers": ["identity"] },
                        "embedding": {
                            "indices": [
                                {
                                    "type": "vector",
                                    "dimensions": d_embedding_size, 
                                    "distance_metric": "cosine",  
                                    "provider": "hnsw"
                                }
                            ]
                        }
                    }
                }
            },
            "primarySort": [
                {"field": "item_name", "direction": "asc"},
                {"field": "session_name", "direction": "asc"},
                {"field": "index", "direction": "asc"}
            ],
            "storedValues": [
                {"fields": ["collection", "timestamp"]}
            ],
        }
    )

    # db.create_arangosearch_view(
    #     name="description_embedding_view",
    #     properties={
    #         "links": {
    #             "description_embedding": {
    #                 "includeAllFields": False,
    #                 "storeValues": "id",
    #                 "fields": {
    #                     "name": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none"
    #                     },
    #                     "index": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "timestamp": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "start_position": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "end_position": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "collection": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "data_id": {
    #                         "analyzers": ["identity"],
    #                         "storeValues": "none",
    #                     },
    #                     "embedding": {
    #                         "indices": [
    #                             {
    #                                 "type": "vector",
    #                                 "dimensions": d_embedding_size, 
    #                                 "distance_metric": "cosine",  
    #                                 "provider": "hnsw"
    #                             }
    #                         ]
    #                     }
    #                 }
    #             }
    #         },
    #         "primarySort": [
    #             {"field": "index", "direction": "asc"}
    #         ],
    #         "storedValues": [
    #             {"fields": ["name", "timestamp", "last_timestamp"]}
    #         ],
    #     }
    # )

