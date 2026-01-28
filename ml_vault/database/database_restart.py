from ml_vault.database.log_helper import operation_management
from ml_vault.database import session_collection, description_collection, artifact_collection
from ml_vault.database import utils

def function_restart(db, interval, name, selected_timestamps = None):
    # only consider values where interval is greater than n
    # for each function condition -> write out the right function interval
    meta = db.collection("metadata").get("global")
    timestamps = meta["active_timestamps"]
    timestamp = utils.get_new_timestamp(db, ["db_restart", name])
    current_time = time.time()
    for ts in timestamps:
        if selected_timestamps is not None and ts not in selected_timestamps:
            continue
        if ts == timestamp:
            continue
        _, last_update, op_info = timestamps[ts]
        if current_time - last_update > interval:
            if op_info[0] == "create_artifact_list":
                operation_management.create_artifact_reverse(db, ts)
            elif op_info[0] == "append_artifact":
                operation_management.append_artifact_reverse(db, ts)
            elif op_info[0] == "add_description_inner":
                operation_management.add_description_reverse(db, ts)
            elif op_info[0] == "delete_artifact_list":
                name = op_info[1]
                artifact_collection = op_info[2]
                session_name = op_info[3]
                session_index = op_info[4]
                artifact_collection.delete_artifact_list_inner(db, ts, name, artifact_collection, session_name, session_index)
            elif op_info[0] == "session_add_code_end":
                name = op_info[1]
                index = op_info[2]
                error = op_info[3]
                session_collection.session_add_code_end(db, name, index, error, ts)
            elif op_info[0] == "session_stop_pause_request":
                utils.commit_new_timestamp(db, ts, "restart")
            elif op_info[0] == "session_checkpoint":
                name = op_info[1]
                session_collection.session_checkpoint(db, name, ts)
            elif op_info[0] == "db_restart":
                pass
            elif op_info[0] == "db_restart":
                pass
        utils.commit_new_timestamp(db, timestamp)