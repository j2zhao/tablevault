from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError

ALL_ARTIFACT_COLLECTIONS = [
    "session", "user", "file_list", "file", "embeddings", "embedding_vector", "document", "document_chunk",
    "record_list", "record_item", "description", "description_embedding",
]

DESCRIPTION_COLLECTIONS = [
    "session", "user", "file", "array", "document", "record_list", "description"
]

VIEW_COLLECTIONS = [
    "session_code", "embedding_vector", "document_chunk", "record_item", "description_embedding"
]

def create_collection_safe(db: StandardDatabase, name: str, schema: dict = None, edge: bool = False):
    """Helper to create collection only if it doesn't exist."""
    if not db.has_collection(name):
        db.create_collection(name=name, schema=schema, edge=edge)

def get_arango_db(database_id:str, 
    arango_url: str, 
    arango_username:str, 
    arango_password:str, 
    arango_root_username = "root",
    arango_root_password = "123abc",
    new_arango_db = True):
    client = ArangoClient(hosts="http://localhost:8529")
    sys_db = client.db("_system", username=arango_root_username, password=arango_root_password)
    if new_arango_db and sys_db.has_database(database_id):
        raise ValueError("Collection Name Exists Already as Database in Arango")
    elif not sys_db.has_database(database_id):
        sys_db.create_database(
            name=db_name,
            users=[{ 
                "username": app_user,
                "password": app_pass,
                "active": True
            }]
        )
       
    db = client.db(database_id, username=arango_username, password=arango_password)
    return db

    
def create_ml_vault_db(db: StandardDatabase, file_location:str, description_embedding_size: int, openai_key: str):
    if db.has_graph("lineage_graph"):
        graph = db.graph("lineage_graph")
    else:
        graph = db.create_graph("lineage_graph")

    create_collection_safe(db, "metadata")
    doc = {
        "_key": "global",
        "file_location": file_location,
        "description_embedding_size": description_embedding_size,
        "openai_key": openai_key,
        "active_timestamps": {},
        "new_timestamp": 1,
        "vector_indices": [],
    }
    col.insert(doc)

    create_collection_safe(db, "session", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                # "can_interupt": {"type": "boolean"}, 
                "interupt_request": {"type": "string"},
                "interupt_action": {"type": "string"},
                "execution_type": {"type": "string"}, 
                "length": {"type": "number"},
                "n_items": {"type": "number"},
                "pid": {"type": "number"},
                "creator_user_id": {"type": "str"},
                # "status": {"type": "string"}
            },
            "required": ["name", "timestamp", 
                "last_timestamp", "interupt_action", 
                "interupt_request", "execution_type",
                "n_items", "length", 
                "pid", "creator_user_id"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    db.collection("session").add_hash_index(fields=["name"], unique=True)

    create_collection_safe(db, "session_code", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                "position_start": {"type": "number"},
                "position_end": {"type": "number"},
                "text": {"type": "string"},
                "status": {"type": "string"},
                "error": {"type": "string"},
            },
            "required": ["index", "timestamp", "last_timestamp", "position_start", "position_end", "text", "status", "error"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    db.collection("session_code").add_hash_index(fields=["name", "index"], unique=True)

    create_collection_safe(db, "user", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
            },
            "required": ["name", "timestamp"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    db.collection("user").add_hash_index(fields=["name"], unique=True)

    # create_collection_safe(db, "record", {
    #     "rule": {
    #         "properties": {
    #             "name": {"type": "string"},
    #             "session_name": {"type": "string"},
    #             "timestamp": {"type": "number"},
    #             "value": {"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}, {"type": "null"}]},
    #             "value_num": {"type": ["number", "null"]},
    #             "value_str": {"type": ["string", "null"]},
    #             "value_bool": {"type": ["boolean", "null"]},
    #         },
    #         "required": ["name", "timestamp", "value"],
    #         "additionalProperties": False
    #     },
    #     "level": "strict"
    # })

    # db.collection("record").add_hash_index(fields=["session_name", "name"], unique=True)

    create_collection_safe(db, "file_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "locked": {"type": "bool"}
            },
            "required": ["name", "timestamp", "last_timestamp", "n_items", "length", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })

    db.collection("file_list").add_hash_index(fields=["name"], unique=True)


    create_collection_safe(db, "file", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "position_start": {"type": "number"},
                "position_end": {"type": "number"},
                "location": {"type": "string"},
            },
            "required": ["name", "index", "timestamp",  "position_start", "position_end", "location"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("file").add_hash_index(fields=["name", "index"], unique=True)

    create_collection_safe(db, "embeddings", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "n_dim": {"type": "number"},
                "locked": {"type": "bool"}

            },
            "required": ["name", "timestamp", "last_timestamp", "n_items", "length", "n_dim", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("embeddings").add_hash_index(fields=["name"], unique=True)


    create_collection_safe(db, "embedding_vector", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "position_start": {"type": "number"},
                "position_end": {"type": "number"},
                #"embedding": {"type": "array", "items": {"type": "number"}},
            },
            "patternProperties": {
                "^embedding_\d+$": {
                    "type": "array", 
                    "items": {"type": "number"}
                }
            },
            "required": ["name", "index", "timestamp", "position_start", "position_end"],
            "additionalProperties": True
        },
        "level": "strict"
    })
    db.collection("embedding_vector").add_hash_index(fields=["name", "index"], unique=True)


    create_collection_safe(db, "document", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "locked": {"type": "bool"},
            },
            "required": ["name", "timestamp", "last_timestamp", "n_items", "length", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("document").add_hash_index(fields=["name"], unique=True)


    create_collection_safe(db, "document_chunk", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "text": {"type": "string"},
            },
            "required": ["name", "index", "timestamp","start_position", "end_position", "text"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("document_chunk").add_hash_index(fields=["name", "index"], unique=True)


    create_collection_safe(db, "record_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "last_timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "column_names": {"type": "array", "items": {"type": "string"}},
                "locked": {"type": "bool"}
            },
            "required": ["name", "timestamp", "last_timestamp", "n_items", "length", "column_names", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("record_list").add_hash_index(fields=["name"], unique=True)

    create_collection_safe(db, "record_item", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "data": {"type": "object"},
                "data_text": {"type": "string"},
                "column_names": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name", "index", "timestamp", "data", "data_text", "column_names"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    db.collection("record_item").add_hash_index(fields=["name", "index"], unique=True)


    create_collection_safe(db, "description", {
        "rule": {
            "properties": {
                "session_name": {"type": "string"},
                "item_name": {"type": "string"},
                "timestamp": {"type": "number"},
                "collection": {"type": "string"},
                "text": {"type": "string"},
                "type":  {"type": "string"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "embedding": {"type": "array", "items": {"type": "number"}},
            },
            "required": ["session_name", "item_name", "timestamp", "collection", "text", "type", "start_position", "end_position", "embedding"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    #db.collection("description").add_hash_index(fields=["name", "index"], unique=True)

    # create_collection_safe(db, "description_embedding", {
    #     "rule": {
    #         "properties": {
    #             "name": {"type": "string"},
    #             "index": {"type": "number"},
    #             "timestamp": {"type": "number"},
    #             "start_position": {"type": "number"},
    #             "end_position": {"type": "number"},
    #             "embedding": {"type": "array", "items": {"type": "number"}},
    #             "collection": {"type": "string"},
    #             "data_id": {"type": "number"},
    #         },
    #         "required": ["name", "index", "timestamp", "start_position", "end_position", "embedding", "collection", "data_id"],
    #         "additionalProperties": False
    #     },
    #     "level": "strict"
    # })

    create_collection_safe(db, "user_session", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
            },
            "required": ["timestamp"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    
    create_collection_safe(db, "write_artifact", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
                "line_num": {"type": "number"}
            },
            "required": ["timestamp", "line_num"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    
    create_collection_safe(db, "read_artifact", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
                "line_num": {"type": "number"}
            },
            "required": ["timestamp", "line_num"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "session_edge", edge=True, schema={
        "rule": {
            "properties": {"index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},},
            "required": ["index", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })


    create_collection_safe(db, "file_edge", edge=True, schema={
        "rule": {
            "properties": {"index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},},
            "required": ["index", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "embedding_edge", edge=True, schema={
        "rule": {
            "properties": {"index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},},
            "required": ["index", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    

    create_collection_safe(db, "document_edge", edge=True, schema={
        "rule": {
            "properties": {"index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},},
            "required": ["index", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    # create_collection_safe(db, "record_edge", edge=True, schema={
    #     "rule": {
    #         "properties": {"index": {"type": "number"},
    #             "start_position": {"type": "number"},
    #             "end_position": {"type": "number"},},
    #         "required": ["index", "start_position", "end_position"],
    #         "additionalProperties": False
    #     },
    #     "level": "strict",
    # })

    create_collection_safe(db, "description_edge", edge=True, schema={
        "rule": {
            "properties": {"index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},},
            "required": ["index", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "dependent_edge", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
            },
            "required": ["timestamp"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    
    def add_edge_def(edge_col, from_cols, to_cols):
        if graph.has_edge_definition(edge_col):
            pass
        else:
            graph.create_edge_definition(
                edge_collection=edge_col,
                from_vertex_collections=from_cols,
                to_vertex_collections=to_cols
            )

    add_edge_def("write_artifact", ["session"], DESCRIPTION_COLLECTIONS)
    add_edge_def("read_artifact", DESCRIPTION_COLLECTIONS, ["session"])
    add_edge_def("session_edge", ["session"], ["session_code"])
    add_edge_def("file_edge", ["file_list"], ["file"])
    add_edge_def("embedding_edge", ["embeddings"], ["embedding_vector"])
    add_edge_def("document_edge", ["document"], ["document_chunk"])
    # add_edge_def("record_edge", ["record_list"], ["record_item"])
    add_edge_def("description_edge", ["description"],  ["description_embedding"])
    add_edge_def("dependent_edge", DESCRIPTION_COLLECTIONS,  DESCRIPTION_COLLECTIONS)
