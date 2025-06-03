from tablevault._helper.database_lock import DatabaseLock
import shutil
import os


def create_mock_db():
    if os.path.exists("jinjin"):
        shutil.rmtree("jinjin")
    os.makedirs("jinjin/locks")
    db_lock = DatabaseLock("test", "jinjin")
    db_lock.make_lock_path("table1")
    db_lock.make_lock_path("table2")
    db_lock.make_lock_path("table1", "instance1")


def test_lock():
    db_lock = DatabaseLock("test", "jinjin")
    # test acquire one read db lock
    db_lock.acquire_shared_lock("table1")
    lid = db_lock.acquire_shared_lock("table1", "instance1")
    db_lock.release_lock(lid)
    # db_lock2 = DatabaseLock('test2', 'jinjin')
    # db_lock2.acquire_exclusive_lock('table1')
    # db_lock.release_all_locks()
    # db_lock.delete_lock_path('table1')
    # db_lock.release_all_locks()
    # db_lock.release_lock(lid)
    # db_lock.acquire_shared_lock()


# db_lock = DatabaseLock('test', 'jinjin')
# db_lock.acquire_shared_lock(timeout=1)
# test acquire one read table lock

# test aquire one read instance lock


if __name__ == "__main__":
    create_mock_db()
    test_lock()
