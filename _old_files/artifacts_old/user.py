


properties = ["name"]

def write_user(user_id, uid, timestamp, code_id, code_line, description = None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (
            id, 
            timestamp,
            code_id, 
            code_line,
            user_id, 
            description, 
        ) VALUES (?, ?, ?, ?, ?)
    """, (uid, timestamp, code_id, user_id, description))
    conn.commit()
    

def transformation(uid, conn):
    cursor = conn.cursor()
    query = "SELECT * FROM users WHERE id = ?"
    cursor.execute(query, (uid,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    else:
        return row

def create_table(db):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER,
        timestamp_create INTEGER,
        timestamp_delete INTEGER,
        code_id_create INTEGER,
        code_id_delete INTEGER,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        embedding float[1536],
        PRIMARY KEY (id, code_id_create),   
    ) STRICT;
    """)
    conn.commit()