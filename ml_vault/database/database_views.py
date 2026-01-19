from typing import Any, Dict, Optional
from arango.database import StandardDatabase

def _create_or_replace_view(db: StandardDatabase, name: str, properties: Dict[str, Any]) -> None:
    if db.has_view(name):
        v = db.view(name)
        v.replace_properties(properties)
    else:
        db.create_arangosearch_view(name=name, properties=properties)


def create_description_view(
    db: StandardDatabase,
    view_name: str = "description_view",
    collection_name: str = "description",
    text_analyzer: str = "text_en",
    description_embedding_size: int = 1536,
    vector_field: str = "embedding",
) -> None:
    props = {
        "links": {
            collection_name: {
                "includeAllFields": False,
                "storeValues": "none",
                "trackListPositions": False,
                "fields": {
                    "artifact_name": {"analyzers": ["identity"]},
                    "collection": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "text": {"analyzers": [text_analyzer]},
                },
            }
        },
        # Optional but often helpful for deterministic paging/sorting on attributes
        "primarySort": [
            {"field": "collection", "direction": "asc"},
            {"field": "artifact_name", "direction": "asc"},
            {"field": "timestamp", "direction": "asc"},
        ],
        "primarySortCompression": "lz4",
    }
    _create_or_replace_view(db, view_name, props)

def create_session_view(
    db: StandardDatabase,
    view_name: str = "session_view",
    collection_name: str = "session",
    text_analyzer: str = "text_en",
    text_field: str = "text",
) -> None:
    props = {
        "links": {
            collection_name: {
                "includeAllFields": False,
                "storeValues": "none",
                "trackListPositions": False,
                "fields": {
                    # text search fields
                    text_field: {"analyzers": [text_analyzer]},
                    "code": {"analyzers": [text_analyzer]},  # compatibility with your AQL

                    # optional identity fields for filtering
                    "name": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                    "status": {"analyzers": ["identity"]},
                    "error": {"analyzers": [text_analyzer]},
                },
            }
        }
    }
    _create_or_replace_view(db, view_name, props)

def create_document_view(
    db: StandardDatabase,
    view_name: str = "document_view",
    collection_name: str = "document",
    text_analyzer: str = "text_en",
    text_field: str = "text",
) -> None:
    props = {
        "links": {
            collection_name: {
                "includeAllFields": False,
                "storeValues": "none",
                "trackListPositions": False,
                "fields": {
                    text_field: {"analyzers": [text_analyzer]},
                    "name": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                },
            }
        }
    }
    _create_or_replace_view(db, view_name, props)

def create_record_view(
    db: StandardDatabase,
    view_name: str = "record_view",
    collection_name: str = "record",
    text_analyzer: str = "text_en",
    record_text_field: str = "data_text",
) -> None:
    props = {
        "links": {
            collection_name: {
                "includeAllFields": False,
                "storeValues": "none",
                "trackListPositions": False,
                "fields": {
                    record_text_field: {"analyzers": [text_analyzer]},
                    "name": {"analyzers": ["identity"]},
                    "session_name": {"analyzers": ["identity"]},
                    "timestamp": {"analyzers": ["identity"]},
                },
            }
        }
    }
    _create_or_replace_view(db, view_name, props)

def create_ml_vault_query_views(
    db: StandardDatabase,
    description_embedding_size: int,
    text_analyzer: str = "text_en",
) -> None:
    create_description_view(
        db,
        view_name="description_view",
        collection_name="description",
        text_analyzer=text_analyzer,
        description_embedding_size=description_embedding_size,
        vector_field="embedding",
    )
    create_session_view(
        db,
        view_name="session_view",
        collection_name="session",
        text_analyzer=text_analyzer,
        text_field="text",
    )
    create_document_view(
        db,
        view_name="document_view",
        collection_name="document",
        text_analyzer=text_analyzer,
        text_field="text",
    )
    create_record_view(
        db,
        view_name="record_view",
        collection_name="record",
        text_analyzer=text_analyzer,
        record_text_field="data_text",
    )