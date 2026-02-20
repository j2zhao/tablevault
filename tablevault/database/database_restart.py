from typing import List, Optional

from arango.database import StandardDatabase

from tablevault.database.log_helper import operation_management, utils
from tablevault.database import process_collection, item_collection
import time


def function_restart(
    db: StandardDatabase,
    interval: int,
    process_name: str,
    selected_timestamps: Optional[List[int]] = None,
) -> None:
    # only consider values where interval is greater than n
    # for each function condition -> write out the right function interval
    timestamps = utils.get_timestamp_info(db)
    timestamp, _ = utils.get_new_timestamp(db, ["db_restart", process_name])
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
                if op_info[0] == "create_item_list":
                    operation_management.create_item_reverse(db, ts)
                elif op_info[0] == "append_item":
                    operation_management.append_item_reverse(db, ts)
                elif op_info[0] == "add_description_inner":
                    operation_management.add_description_reverse(db, ts)
                elif op_info[0] == "delete_item_list":
                    op_name = op_info[1]
                    op_coll_name = op_info[2]
                    op_process_name = op_info[3]
                    op_process_index = op_info[4]
                    item_collection.delete_item_list_inner(
                        db, ts, op_name, op_coll_name, op_process_name, op_process_index
                    )
                    utils.commit_new_timestamp(db, ts, "restart")
                elif op_info[0] == "process_add_code_end":
                    op_name = op_info[1]
                    op_index = op_info[2]
                    op_error = op_info[3]
                    process_collection.process_add_code_end(
                        db, op_name, op_index, op_error, ts
                    )
                elif op_info[0] == "process_resume_request":
                    op_name = op_info[1]
                    op_process_name = op_info[2]
                    process_collection.process_resume_request(
                        db, op_name, op_process_name, timestamp=ts
                    )
                elif op_info[0] == "process_stop_pause_request":
                    utils.commit_new_timestamp(db, ts, "restart")
                elif op_info[0] == "db_restart":
                    pass
        except Exception as e:
            import warnings

            warnings.warn(f"Cleanup failed for timestamp {k}: {e}")
            continue
    utils.commit_new_timestamp(db, timestamp)
