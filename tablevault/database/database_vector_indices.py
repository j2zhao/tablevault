# Add description view

from typing import Any, Dict, Optional, Tuple

from arango.database import StandardDatabase
import time
from tablevault.utils.errors import LockTimeoutError


def _get_index_by_name(
    db: StandardDatabase, collection: str, index_name: str
) -> Optional[Dict[str, Any]]:
    col = db.collection(collection)
    for idx in col.indexes():
        if idx.get("name") == index_name:
            return idx
    return None


def add_one_vector_count(
    db: StandardDatabase, embedding_name: str, tries: int = 5, wait_time: float = 0.1
) -> Tuple[int, int]:
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
            coll.update(meta, check_rev=True, merge=False)
            return meta["vector_indices"][embedding_name]["total_count"], meta[
                "vector_indices"
            ][embedding_name]["idx_count"]
        except Exception:
            pass
        time.sleep(wait_time)
    raise LockTimeoutError(
        f"Failed to update vector counters for '{embedding_name}' after {tries} attempts.",
        operation="add_one_vector_count",
        collection="metadata",
        key=embedding_name,
    )


def update_vector_idx(
    db: StandardDatabase, embedding_name: str, tries: int = 5, wait_time: float = 0.1
) -> int:
    coll = db.collection("metadata")
    for i in range(tries):
        meta = coll.get("global")
        if embedding_name in meta["vector_indices"]:
            meta["vector_indices"][embedding_name]["idx_count"] = meta[
                "vector_indices"
            ][embedding_name]["total_count"]
        else:
            meta["vector_indices"][embedding_name] = {}
            meta["vector_indices"][embedding_name]["idx_count"] = 0
            meta["vector_indices"][embedding_name]["total_count"] = 1
        try:
            coll.update(meta, check_rev=True, merge=False)
            return meta["vector_indices"][embedding_name]["total_count"]
        except Exception:
            pass
        time.sleep(wait_time)
    raise LockTimeoutError(
        f"Failed to update vector index counts for '{embedding_name}' after {tries} attempts.",
        operation="update_vector_idx",
        collection="metadata",
        key=embedding_name,
    )


def build_vector_idx(
    db: StandardDatabase,
    embedding_name: str,
    dim: int,
    parallelism: int = 2,
    n_lists: int = 50,
    default_n_probe: int = 2,
    training_iterations: int = 25,
) -> None:
    col = db.collection("embedding")
    idx_name = embedding_name + "_idx"
    idx = _get_index_by_name(db, "embedding", idx_name)
    if idx is not None:
        db.delete_index(idx["id"])

    col.add_index(
        {
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
        }
    )


# ADD DESCIRPTION INDICES
