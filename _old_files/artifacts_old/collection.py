"""

Include semantic description of item

"""
from ml_vault.artifacts.artifacts import Artifact, query_object_id
from typing import Optional
from ml_vault.utils import constants

SEARCH_MODES = []

class Collection(Artifact):
    filtered_items: Optional[list[int]]
    filter_query: list[str]
    upstream: FilteredCollection | None
    downstream: FilteredCollection
    n_items: int

    def __init__(self, filter_query):
        self.filter_query = filter_query
    
    def apply(self, filter, time_stamp):
        items = None
        if isinstance(self.upstream, FilteredCollection) and self.upstream.filtered_items:
            items = self.upstream.filtered_items
        if isinstance(self.downstream, FilteredCollection) and self.downstream.filtered_items:
            if items and len(items) > self.downstream.filtered_items:
                items = self.downstream.filtered_items
        if not items and isinstance(self.upstream, None): 
            items = None
        elif not items:
            raise ValueError()

        # deal with this case
        
        for dtype in constants.DATA_TYPES:
            # find all items of that data type
            # apply filter
            pass

def filter(collection, filter_query, mode) -> list[int]:
    # we filter by n objects, collection name
    if mode not in SEARCH_MODES:
        return []
    # filter by collection

def update(db, collection_name, artifact_id, artifact_type, line, parent_id, timestamp):
    doc = db.collection('collection').get(collection_name)

    changes = {
        '_key': collection_name,
        'n_items': doc.get('n_items') + 1,
    }
    db.collection('collection').update(changes)

    edge_data = {
        '_from': f'collection/{collection_name}', 
        '_to':   f'experiment/{parent_id}', 
        'type': 'parent_experiment',
        'timestamp': timestamp, 
        'action': 'update',
        'data_type': 'collection',
        'line': line,
    }
    db.collection('parent_experiment').insert(edge_data)
    
    edge_data = {
        '_from': f'collection/{collection_name}', 
        '_to':   f'{artifact_type}/{artifact_id}', 
        'type': 'collection_item',
        'timestamp': timestamp, 
    }
    db.collection('collection_item').insert(edge_data)

def write(db, collection_name, line, parent_id, timestamp, description= None):
    properties = {
        "_id": collection_name,
        "timestamp": timestamp,
        "n_items": 0,
        "description": description,
        "active": True
    }
    result = db.collection("collection").insert(user_dict)

    edge_data = {
        '_from': f'collection/{collection_name}', 
        '_to':   f'experiment/{experiment_id}', 
        'type': 'parent_experiment',
        'timestamp': timestamp, 
        'action': 'write',
        'data_type': 'collection',
        'line': line,
    }
    db.collection('parent_experiment').insert(edge_data)

def delete(db, collection_name, line, parent_id, timestamp):
    aql = f"""
    FOR e IN collection_item
        FILTER e._from == @target_id
        REMOVE e IN collection_item
    """
    # Execute
    db.aql.execute(aql, bind_vars={'target_id': collection_name})
    doc = db.collection('collection').get(collection_name)
    changes = {
        '_key': collection_name,
        'active': False
    }
    db.collection('collection').update(changes)

    edge_data = {
        '_from': f'collection/{collection_name}', 
        '_to':   f'experiment/{parent_id}', 
        'type': 'parent_experiment',
        'timestamp': timestamp, 
        'action': 'delete',
        'data_type': 'collection',
        'line': line,
    }
    db.collection('parent_experiment').insert(edge_data)

def get_from_id(id):
    pass

# Collection scheduling
def create_table(db):
    db.create_collection("collection")
    db.create_collection("collection_item", edge=True)
    db.collection("collection").add_index(
        type="vector",
        fields=["embedding"],
        metric="cosine",  # or "l2" (Euclidean)
        dimension=1536      # Must match your vector size
    )