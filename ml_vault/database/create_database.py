from arango import ArangoClient
from arango.database import StandardDatabase
from ml_vault.database.database_views import create_ml_vault_query_views
ALL_ARTIFACT_COLLECTIONS = [
    "session", "file_list", "file", "embedding_list", "embedding", "document_list", "document",
    "record_list", "record", "description"
]

DESCRIPTION_COLLECTIONS = [
    "session_list", "file_list", "document_list", "record_list"
]

VIEW_COLLECTIONS = [
    "session", "embedding", "document", "record", "description"
]

def create_collection_safe(db: StandardDatabase, name: str, schema: dict = None, edge: bool = False):
    """Helper to create collection only if it doesn't exist."""
    if not db.has_collection(name):
        db.create_collection(name=name, schema=schema, edge=edge)

def get_arango_db(database_name:str, 
    arango_url: str, 
    arango_username:str, 
    arango_password:str, 
    arango_root_username:str,
    arango_root_password:str,
    new_arango_db = True):
    client = ArangoClient(hosts=arango_url)
    sys_db = client.db("_system", username=arango_root_username, password=arango_root_password)
    if new_arango_db and sys_db.has_database(database_name):
        sys_db.delete_database(database_name)
    if new_arango_db:
        sys_db.create_database(
            name=database_name,
            users=[{ 
                "username": arango_username,
                "password": arango_password,
                "active": True
            }]
        )
       
    db = client.db(database_name, username=arango_username, password=arango_password)
    return db

    
def create_ml_vault_db(db: StandardDatabase, description_embedding_size: int):
    if db.has_graph("lineage_graph"):
        graph = db.graph("lineage_graph")
    else:
        graph = db.create_graph("lineage_graph")

    create_collection_safe(db, "metadata")
    doc = {
        "_key": "global",
        "description_embedding_size": description_embedding_size,
        "active_timestamps": {},
        "new_timestamp": 1,
        "vector_indices": {},
    }
    col = db.collection("metadata")
    col.insert(doc)

    create_collection_safe(db, "artifacts", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "timestamp": {"type": "number"},
                "collection": {"type": "string"}, 
            },
            "required": ["name", "timestamp", "collection"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "session_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "interupt_request": {"type": "string"},
                "interupt_action": {"type": "string"},
                "execution_type": {"type": "string"}, 
                "length": {"type": "number"},
                "n_items": {"type": "number"},
                "pid": {"type": "number"},
                "creator_user_id": {"type": "string"},
            },
            "required": ["name", 
                "session_name",
                "session_index",
                "timestamp", 
                "interupt_request", 
                "interupt_action",
                "execution_type",
                "length", 
                "n_items", 
                "pid", 
                "creator_user_id"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "session", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "text": {"type": "string"},
                "status": {"type": "string"},
                "error": {"type": "string"},
            },
            "required": ["name", "session_name", "session_index", "index", "timestamp", "start_position", "end_position", "text", "status", "error"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "file_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "locked": {"type": "number"}
            },
            "required": ["name", "session_name", "session_index", "timestamp", "n_items", "length", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })

    create_collection_safe(db, "file", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "location": {"type": "string"},
            },
            "required": ["name", 
                        "index", 
                        "session_name",
                        "session_index",
                        "timestamp",  
                        "start_position", 
                        "end_position", 
                        "location",
                        ],
            "additionalProperties": False
        },
        "level": "strict"
    })

    create_collection_safe(db, "embedding_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "n_dim": {"type": "number"},
                "locked": {"type": "number"}

            },
            "required": ["name", "session_name", "session_index", "timestamp", "n_items", "length", "n_dim", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })


    create_collection_safe(db, "embedding", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
            },
            "patternProperties": {
                "^embedding_\d+$": {
                    "type": "array", 
                    "items": {"type": "number"}
                }
            },
            "required": ["name", "index", "session_name", "session_index", "timestamp", "start_position", "end_position"],
            "additionalProperties": True
        },
        "level": "strict"
    })
    

    create_collection_safe(db, "document_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "locked": {"type": "number"},
            },
            "required": ["name", "session_name", "session_index", "timestamp", "n_items", "length", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })
    create_collection_safe(db, "document", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "text": {"type": "string"},
            },
            "required": ["name", "index", "session_name", "session_index", "timestamp","start_position", "end_position", "text"],
            "additionalProperties": False
        },
        "level": "strict"
    })


    create_collection_safe(db, "record_list", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "timestamp": {"type": "number"},
                "n_items": {"type": "number"},
                "length": {"type": "number"},
                "column_names": {"type": "array", "items": {"type": "string"}},
                "locked": {"type": "number"}
            },
            "required": ["name", "session_name", "session_index", "timestamp", "n_items", "length", "column_names", "locked"],
            "additionalProperties": False
        },
        "level": "strict"
    })

    create_collection_safe(db, "record", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "index": {"type": "number"},
                "timestamp": {"type": "number"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
                "data": {"type": ["object", "null"]},
                "data_text": {"type": ["string", "null"]},
                "column_names": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name", "index", "timestamp", "session_name", "session_index", "start_position", "end_position", "data", "data_text", "column_names"],
            "additionalProperties": False
        },
        "level": "strict"
    })


    create_collection_safe(db, "description", {
        "rule": {
            "properties": {
                "name": {"type": "string"},
                "artifact_name": {"type": "string"},
                "session_name": {"type": "string"},
                "session_index": {"type": "number"},
                "collection": {"type": "string"},
                "timestamp": {"type": "number"},
                "text": {"type": "string"},
                "embedding": {"type": "array", "items": {"type": "number"}},
            },
            "required": ["artifact_name", "name", "session_name", "session_index", "collection", "timestamp", "text", "embedding"],
            "additionalProperties": False
        },
        "level": "strict"
    })

    create_collection_safe(db, "parent_edge", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
            },
            "required": ["timestamp", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "dependency_edge", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
                "start_position": {"type": "number"},
                "end_position": {"type": "number"},
            },
            "required": ["timestamp", "start_position", "end_position"],
            "additionalProperties": False
        },
        "level": "strict",
    })

    create_collection_safe(db, "description_edge", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
            },
            "required": ["timestamp"],
            "additionalProperties": False
        },
        "level": "strict",
    })
    
    create_collection_safe(db, "session_parent_edge", edge=True, schema={
        "rule": {
            "properties": {
                "timestamp": {"type": "number"},
                "index": {"type": "number"},
            },
            "required": ["timestamp", "index"],
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

    add_edge_def("dependency_edge", DESCRIPTION_COLLECTIONS,  VIEW_COLLECTIONS) # input_list -> artifact (checked)
    add_edge_def("session_parent_edge",  ["session_list"], VIEW_COLLECTIONS) # session_list -> artifact (checked)
    add_edge_def("description_edge",  ["desciption"], DESCRIPTION_COLLECTIONS) # artifact_list -> description (checked)
    add_edge_def("parent_edge",  DESCRIPTION_COLLECTIONS, VIEW_COLLECTIONS) # artifact_list -> artifact (checked)
    create_ml_vault_query_views(db, description_embedding_size)