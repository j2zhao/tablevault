from ml_vault.artifacts.artifacts import Artifact


class Array(Artifact):
    dimensions: list[int]

    def filter_dimension(self, timestamp):
        pass

    def get_arr_snapshot(self, timestamp):
        pass
    
    def get_dim_snapshot(self, timestamp):
        pass

def filter():
    pass

def get_item():
    pass

def update():
    pass

def write():
    pass

def create_table(conn):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,   
            active INTEGER DEFAULT 1,         
            code_id INTEGER
        ) STRICT;
        """)
        conn.commit()