from arango import ArangoClient, ArangoError
from pathlib import Path


def write(db,
            artifact_id, 
            artifact_name,
            artifact_file,
            line,
            parent_id,
            timestamp,
            store_dir,
            pickle = True,
            description = ""):
    if not isinstance(store_dir, Path):
        store_dir = Path(store_dir)
    if not isinstance(artifact_file, Path):
        artifact_file = Path(artifact_file)
    file_name = store_dir / str(artifact_id) + artifact_file.suffixes
   
    properties = {
        "_id": artifact_id,
        "timestamp": timestamp,
        "name": artifact_id,
        "description": description,
        "active": True,
    }
    result = db.collection("file").insert(properties)

    edge_data = {
        '_from': f'file/{artifact_id}', 
        '_to':   f'experiment/{experiment_id}', 
        'type': 'parent_experiment',
        'timestamp': timestamp, 
        'action': 'write',
        'data_type': 'file',
        'line': line,
    }
    db.collection('parent_experiment').insert(edge_data)
    

def create_collection(db):
    db.create_collection("file")
    