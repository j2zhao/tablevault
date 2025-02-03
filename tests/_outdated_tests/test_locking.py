"""
Currently doesn't work with 

"""
import subprocess
import shutil
import os
from tablevault._database_lock import DatabaseLock
from tests._outdated_tests.test_basic import create_db


def multi_instance_reads():
    instance_id, table_name, db_dir = create_db()
    locks = []
    locks.append(DatabaseLock(db_dir))
    locks.append(DatabaseLock(db_dir, table_name))
    locks.append(DatabaseLock(db_dir, table_name, instance_id))
    locks_ = []
    locks_.append(DatabaseLock(db_dir))
    locks_.append(DatabaseLock(db_dir, table_name))
    locks_.append(DatabaseLock(db_dir, table_name, instance_id))
    lock_names = ('db_dir', 'table_name', 'instance_id')
    # should all be true
    for i in range(3):
        for j in range(3):
            print('GETTING LOCKS')
            print((lock_names[i], lock_names[j]))
            locks[i].acquire_shared_lock()
            locks_[j].acquire_shared_lock()
            locks[i].release_shared_lock()
            locks_[j].release_shared_lock()
    

def multi_instance_writes():
    # every combination of writes shouldn't work
    instance_id, table_name, db_dir = create_db()
    locks = []
    locks.append(DatabaseLock(db_dir))
    locks.append(DatabaseLock(db_dir, table_name))
    locks.append(DatabaseLock(db_dir, table_name, instance_id))
    locks_ = []
    locks_.append(DatabaseLock(db_dir))
    locks_.append(DatabaseLock(db_dir, table_name))
    locks_.append(DatabaseLock(db_dir, table_name, instance_id))
    lock_names = ('db_dir', 'table_name', 'instance_id')

    for i in range(3):
        for j in range(3):
            print('GETTING LOCKS')
            print((lock_names[i], lock_names[j]))
            try:
                locks[i].acquire_exclusive_lock()
                print('ACQUIRED FIRST LOCK')
                locks_[j].acquire_exclusive_lock(timeout = 2)
                raise ValueError()
            except TimeoutError:
                locks[i].release_exclusive_lock()

def multi_instance_read_write():
    # every combination of reads and writes shouldn't work
    instance_id, table_name, db_dir = create_db()
    locks = []
    locks.append(DatabaseLock(db_dir))
    locks.append(DatabaseLock(db_dir, table_name))
    locks.append(DatabaseLock(db_dir, table_name, instance_id))
    locks_ = []
    locks_.append(DatabaseLock(db_dir))
    locks_.append(DatabaseLock(db_dir, table_name))
    locks_.append(DatabaseLock(db_dir, table_name, instance_id))
    lock_names = ('db_dir', 'table_name', 'instance_id')

    for i in range(3):
        for j in range(i, 3):
            print('GETTING LOCKS')
            print((lock_names[i], lock_names[j]))
            try:
                locks[i].acquire_shared_lock()
                locks_[j].acquire_exclusive_lock(timeout = 2)
                raise ValueError()
            except TimeoutError:
                locks[i].release_shared_lock()

def multi_instance_write_read():
    # every combination of writes and reads shouldn't work
    instance_id, table_name, db_dir = create_db()
    locks = []
    locks.append(DatabaseLock(db_dir))
    locks.append(DatabaseLock(db_dir, table_name))
    locks.append(DatabaseLock(db_dir, table_name, instance_id))
    locks_ = []
    locks_.append(DatabaseLock(db_dir))
    locks_.append(DatabaseLock(db_dir, table_name))
    locks_.append(DatabaseLock(db_dir, table_name, instance_id))
    lock_names = ('db_dir', 'table_name', 'instance_id')

    for i in range(3):
        for j in range(3):
            try:
                print('GETTING LOCKS')
                print((lock_names[i], lock_names[j]))
                locks[i].acquire_exclusive_lock()
                locks_[j].acquire_shared_lock(timeout = 2)
                raise ValueError()
            except TimeoutError:
                locks[i].release_exclusive_lock()

def test_restart():
    instance_id, table_name, db_dir = create_db()
    locks = []
    locks.append(DatabaseLock(db_dir))
    locks.append(DatabaseLock(db_dir, table_name))
    locks.append(DatabaseLock(db_dir, table_name, instance_id))
    lock_names = ('db_dir', 'table_name', 'instance_id')

    restart_lock = DatabaseLock(db_dir, 'RESTART')
    
    for i in range(3):
        try:
            print('GETTING LOCKS')
            print((lock_names[i]))
            restart_lock.acquire_exclusive_lock()
            print('acquired restart lock')
            locks[i].acquire_shared_lock(timeout = 2)
            raise ValueError()
        except TimeoutError:
            restart_lock.release_exclusive_lock()


if __name__ == "__main__":
    multi_instance_write_read()
    multi_instance_read_write()
    multi_instance_writes()
    multi_instance_reads()
    multi_instance_writes()
    test_restart()
    