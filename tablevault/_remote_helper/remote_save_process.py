import os
import time

from tablevault._remote_helper import _remote_save_helper
from tablevault._defintions import tv_errors
from tablevault._helper.database_lock import DatabaseLock
from tablevault._helper.utils import gen_tv_id
from pathlib import Path


def initialize_local_from_remote(local_db_dir, remote_db_dir, log_file, compress=True):
    if compress:
        _remote_save_helper.compress_and_copy(remote_db_dir, local_db_dir, log_file)
    else:
        _remote_save_helper.rsync_to_drive(remote_db_dir, local_db_dir, log_file)


def setup_initial_backup(local_db_dir, remote_db_dir, log_file, compress):
    logger = _remote_save_helper.configure_file_logger(Path(log_file))
    logger.info("===== STARTING BACKUP PROCESS =====")
    if compress:
        _remote_save_helper.compress_and_copy(local_db_dir, remote_db_dir, log_file)
    else:
        _remote_save_helper.rsync_to_drive(local_db_dir, remote_db_dir, log_file)
    logger.info("===== FINISHED INITIAL BACKUP =====")


def run_backup_process(
    local_db_dir, remote_db_dir, log_file, interval_seconds, parent_pid
):
    logger = _remote_save_helper.configure_file_logger(Path(log_file))
    logger.info("===== STARTING RECURRING BACKUP =====")
    while True:
        try:
            os.kill(parent_pid, 0)
        except ProcessLookupError:
            logger.info("Parent not found - stopping sync.")
            return
        time.sleep(interval_seconds)
        logger.info("Sync Check Heartbeat...")
        process_id = gen_tv_id()
        db_locks = DatabaseLock(process_id, local_db_dir)
        try:
            _remote_save_helper.rsync_to_drive(local_db_dir, remote_db_dir, log_file)
        except tv_errors.TVLockError:
            logger.info("Couldn't get remote lock - passing this save.")
        finally:
            db_locks.release_all_locks()
        logger.info(f"Waiting for {interval_seconds} seconds until next sync...")
