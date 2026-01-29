# Add description view

from arango.database import StandardDatabase
import time

def _get_index_by_name(db: StandardDatabase, collection: str, index_name: str):
    col = db.collection(collection)
    for idx in col.indexes():
        if idx.get("name") == index_name:
            return idx
    return None

def add_one_vector_count(db, embedding_name, tries = 5, wait_time=0.1):
    coll = db.collection("metadata")
    for i in range(tries):
        meta = coll.get("global")
        if embedding_name in meta["vector_indices"]:
            meta["vector_indices"][embedding_name]["total_count"] += 1
        else:
            meta["vector_indices"][embedding_name] = {}
            meta["vector_indices"][embedding_name]["idx_count"] = 0
            meta["vector_indices"][embedding_name]["total_count"] = 1
        try:
            coll.update(meta, check_rev=True,  merge = False)
            return meta["vector_indices"][embedding_name]["total_count"], meta["vector_indices"][embedding_name]["idx_count"]
        except Exception:
            pass
        time.sleep(wait_time)
    raise ValueError("Vector Update Failed")


def update_vector_idx(db, embedding_name, tries = 5,  wait_time=0.1):
    coll = db.collection("metadata")
    for i in range(tries):
        meta = coll.get("global")
        if embedding_name in meta["vector_indices"]:
            meta["vector_indices"][embedding_name]["idx_count"] = meta["vector_indices"][embedding_name]["total_count"]
        else:
            meta["vector_indices"][embedding_name] = {}
            meta["vector_indices"][embedding_name]["idx_count"] = 0
            meta["vector_indices"][embedding_name]["total_count"] = 1
        try:
            coll.update(meta, check_rev=True,  merge = False)
            return meta["vector_indices"][embedding_name]["total_count"]
        except Exception:
            pass
        time.sleep(wait_time)
    raise ValueError("Vector Update Failed")

def build_vector_idx(db, embedding_name, dim, parallelism = 2, n_lists = 50, default_n_probe= 2, training_iterations = 25):
    col = db.collection("embedding")
    idx_name = embedding_name + "_idx"
    idx = _get_index_by_name(db, "embedding", idx_name)
    if idx is not None:
        db.delete_index(idx["id"])

    col.add_index({
        "type": "vector",
        "name": idx_name,
        "fields": [embedding_name],
        "inBackground": True,
        "parallelism": int(parallelism),
        "params": {
            "metric": "cosine",
            "dimension": dim,
            "nLists": n_lists,
            "defaultNProbe": default_n_probe,
            "trainingIterations": training_iterations,
        },
    })

# ADD DESCIRPTION INDICES