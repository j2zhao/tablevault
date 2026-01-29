from ml_vault.database.log_helper import operation_management, utils
from ml_vault.database import session_collection, artifact_collection
import time


def function_restart(db, interval, session_name, selected_timestamps=None):
    # only consider values where interval is greater than n
    # for each function condition -> write out the right function interval
    timestamps = utils.get_timestamp_info(db)
    timestamp, _ = utils.get_new_timestamp(db, ["db_restart", session_name])
    current_time = time.time()
    if selected_timestamps is not None:
        selected_timestamps = [str(k) for k in selected_timestamps]
    for k in timestamps:
        try:
            if selected_timestamps is not None and k not in selected_timestamps:
                continue
            _, last_update, op_info = timestamps[k]
            ts = int(k)
            if current_time - last_update > interval:
                if op_info[0] == "create_artifact_list":
                    operation_management.create_artifact_reverse(db, ts)
                elif op_info[0] == "append_artifact":
                    operation_management.append_artifact_reverse(db, ts)
                elif op_info[0] == "add_description_inner":
                    operation_management.add_description_reverse(db, ts)
                elif op_info[0] == "delete_artifact_list":
                    op_name = op_info[1]
                    op_coll_name = op_info[2]
                    op_session_name = op_info[3]
                    op_session_index = op_info[4]
                    artifact_collection.delete_artifact_list_inner(
                        db, ts, op_name, op_coll_name, op_session_name, op_session_index
                    )
                    utils.commit_new_timestamp(db, ts, "restart")
                elif op_info[0] == "session_add_code_end":
                    op_name = op_info[1]
                    op_index = op_info[2]
                    op_error = op_info[3]
                    session_collection.session_add_code_end(
                        db, op_name, op_index, op_error, ts
                    )
                elif op_info[0] == "session_resume_request":
                    op_name = op_info[1]
                    op_session_name = op_info[2]
                    session_collection.session_resume_request(
                        db, op_name, op_session_name, timestamp=ts
                    )
                elif op_info[0] == "session_stop_pause_request":
                    utils.commit_new_timestamp(db, ts, "restart")
                elif op_info[0] == "db_restart":
                    pass
        except Exception as e:
            import warnings

            warnings.warn(f"Cleanup failed for timestamp {k}: {e}")
            continue
    utils.commit_new_timestamp(db, timestamp)
