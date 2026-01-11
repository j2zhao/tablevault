
def create_user(db: StandardDatabase, user_id):
    user = db.collection("user")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_key": user_id,
        "name": name,
        "timestamp": timestamp,
    }
    try:
        meta = user.insert(doc)
    except DocumentInsertError as e:
        if user.has(name):
            raise ValueError(f"{name} already exists as user")
    timestamp_utils.commit_new_timestamp(timestamp)


def create_user_session(db, user_id, session_name):
    user_edge = db.collection("user_session")
    timestamp = timestamp_utils.get_new_timestamp(db)
    doc = {
        "_from": f"user/{user_id}",
        "_to": f"session/{session_name}",
        "timestamp": timestamp,
    }
    meta = user_edge.insert(doc)
    timestamp_utils.commit_new_timestamp(timestamp)