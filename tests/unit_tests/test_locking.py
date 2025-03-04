from tablevault._helper.database_lock import DatabaseLock, make_lock_path, delete_lock_path
import shutil
import os

#TODO fix:
def create_mock_db():
    if os.path.exists('jinjin'):
        shutil.rmtree('jinjin')
    os.makedirs('jinjin')
    make_lock_path('jinjin', 'table1')
    make_lock_path('jinjin', 'table2')
    make_lock_path('jinjin', 'table1', 'instance1')

def acquire_lock():
    db_lock = DatabaseLock('test', 'jinjin')
    # test acquire one read db lock
    db_lock.acquire_shared_lock('table3')
    db_lock.acquire_shared_lock()
    
    # test acquire one read table lock 

    # test aquire one read instance lock


if __name__ == '__main__':
    create_mock_db()
    acquire_lock()
