from arango.database import StandardDatabase
from typing import Dict, List, Any


def _create_view_safe(db: StandardDatabase, name: str, properties: Dict[str, Any]) -> None:
    """Create or update an ArangoSearch view."""
    if db.has_view(name):
        view = db.view(name)
        view.replace_properties(properties)
    else:
        db.create_arangosearch_view(name=name, properties=properties)


def ensure_vector_index(db: StandardDatabase, embedding_dimension: int) -> str:
    """
    Ensure a vector index exists for the given embedding dimension.
    Returns the field name.
    """
    view_name = "embedding_view"
    
    # Check if view exists
    if not db.has_view(view_name):
        raise ValueError(f"View '{view_name}' does not exist. Create it first with create_session_views().")
    
    view = db.view(view_name)
    props = view.properties()
    links = props.get('links', {})
    
    field_name = f"embedding_{embedding_dimension}"
    
    # Check if this dimension already has a vector index
    current_fields = links.get("embedding", {}).get("fields", {})
    if field_name in current_fields:
        field_indices = current_fields[field_name].get("indices", [])
        if any(idx.get("type") == "vector" for idx in field_indices):
            return field_name
    
    # Add the new vector index
    if "embedding" not in links:
        links["embedding"] = {"fields": {}}
    if "fields" not in links["embedding"]:
        links["embedding"]["fields"] = {}
    
    links["embedding"]["fields"][field_name] = {
        "indices": [{
            "type": "vector",
            "dimensions": embedding_dimension,
            "distance_metric": "cosine",
            "provider": "hnsw"
        }]
    }
    
    view.replace_properties({"links": links})
    return field_name


def create_session_views(db: StandardDatabase, d_embedding_size: int) -> None:
    """Create all ArangoSearch views for the session tracking system."""
    
    # Session code view - for code execution tracking
    _create_view_safe(db, "session_view", {
        "links": {
            "session": {
                "includeAllFields": False,
                "fields": {
                    "name": {"analyzers": ["identity"]},
                    "index": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "last_timestamp": {"analyzers": ["identity"]},
                    "position_start": {"analyzers": ["identity"]},
                    "position_end": {"analyzers": ["identity"]},
                    "text": {"analyzers": ["text_en"]},
                    "status": {"analyzers": ["identity"]},
                    "error": {"analyzers": ["identity"]},    
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
    })

    # File view - for file tracking
    _create_view_safe(db, "file_view", {
        "links": {
            "file": {
                "includeAllFields": False,
                "fields": {
                    "name": {"analyzers": ["identity"]},
                    "index": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "line_num": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "position_start": {"analyzers": ["identity"]},
                    "position_end": {"analyzers": ["identity"]},
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
    })

    # Embedding view - for vector embeddings with dynamic dimensions
    _create_view_safe(db, "embedding_view", {
        "links": {
            "embedding": {
                "includeAllFields": False,
                "fields": {
                    "name": {"analyzers": ["identity"]},
                    "index": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "line_num": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "position_start": {"analyzers": ["identity"]},
                    "position_end": {"analyzers": ["identity"]},
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
    })

    # Document view - for document text search
    _create_view_safe(db, "document_view", {
        "links": {
            "document": {
                "includeAllFields": False,
                "fields": {
                    "name": {"analyzers": ["identity"]},
                    "index": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "line_num": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "start_position": {"analyzers": ["identity"]},
                    "end_position": {"analyzers": ["identity"]},
                    "text": {"analyzers": ["text_en"]},
                }
            }
        },
        "primarySort": [
            {"field": "name", "direction": "asc"},
            {"field": "index", "direction": "asc"}
        ],
        "storedValues": [
            {"fields": ["timestamp", "start_position", "end_position"]}
        ]
    })

    # Record view - for structured data records
    _create_view_safe(db, "record_view", {
        "links": {
            "record": {
                "includeAllFields": False,
                "fields": {
                    "name": {"analyzers": ["identity"]},
                    "index": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "line_num": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "start_position": {"analyzers": ["identity"]},
                    "end_position": {"analyzers": ["identity"]},
                    "data_text": {"analyzers": ["text_en"]},
                    "data": {
                        "includeAllFields": True,
                        "analyzers": ["identity"]
                    },
                    "column_names": {
                        "includeAllFields": True,
                        "analyzers": ["identity"]}
                }
            }
        },
        "primarySort": [
            {"field": "name", "direction": "asc"},
            {"field": "index", "direction": "asc"}
        ],
        "storedValues": [
            {"fields": ["timestamp", "start_position", "end_position"]}
        ]
    })

    _create_view_safe(db, "description_view", {
        "links": {
            "description": {
                "includeAllFields": False,
                "fields": {
                    "artifact_name": {"analyzers": ["identity"]},
                    "artifact_collection": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "text": {"analyzers": ["text_en"]},
                    "embedding": {
                        "indices": [{
                            "type": "vector",
                            "dimensions": d_embedding_size,
                            "distance_metric": "cosine",
                            "provider": "hnsw"
                        }]
                    }
                }
            }
        },
        "primarySort": [
            {"field": "artifact_name", "direction": "asc"},
            {"field": "timestamp", "direction": "asc"}
        ],
        "storedValues": [
            {"fields": ["artifact_name", "artifact_collection", "timestamp"]}
        ]
    })
