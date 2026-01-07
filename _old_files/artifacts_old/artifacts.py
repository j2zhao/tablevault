
from pydantic import BaseModel

class Artifact(BaseModel):
    name: str
    timestamp: int
    code_parents: [int]
    collections: [int]
    description: str
    embedding: [float]

def query_object_id():
    pass

def create_semantic_description():
    pass

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS object_registry (
        global_id INTEGER PRIMARY KEY,
        table_name TEXT NOT NULL,
    ) STRICT;
    """)
    conn.commit()