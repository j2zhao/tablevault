import os
import stat
from tablevault.defintions import constants
from typing import Optional
from tablevault.defintions import tv_errors

def set_not_writable(path:str, set_childen:bool = True, set_children_files=True, skip_children=[], set_self=True):
    """
    If `path` is a file, set *just* that file to read+execute (no write).
    If `path` is a directory, set it (and everything underneath) to read+execute.
    """
    # First, chmod the path itself
    mode = (
        stat.S_IREAD   | stat.S_IRGRP  | stat.S_IROTH |  # read for owner, group, others
        stat.S_IXUSR   | stat.S_IXGRP  | stat.S_IXOTH    # execute for owner, group, others
    )
    if not os.path.exists(path):
        return
    if set_self:
        os.chmod(path, mode)
    
    # Only recurse if it's a directory
    if os.path.isdir(path) and (set_childen or set_children_files):
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path) and set_childen:
                if entry not in skip_children:
                    set_writable(full_path, set_childen, set_children_files, skip_children)
                else:
                    os.chmod(full_path, mode)
            else:
                if entry not in skip_children:
                    os.chmod(full_path, mode)

def set_writable(path:str, set_childen:bool = True, set_children_files=True, skip_children=[], set_self=True):
    """
    If `path` is a file or directory, set it to read+write+execute for
    owner, group, and others. If it's a directory, recurse into children.
    """
    # Always grant r, w, x to owner/group/others
    mode = (
        stat.S_IREAD   | stat.S_IWRITE   | stat.S_IXUSR   |
        stat.S_IRGRP   | stat.S_IWGRP    | stat.S_IXGRP   |
        stat.S_IROTH   | stat.S_IWOTH    | stat.S_IXOTH
    )
    if not os.path.exists(path):
        return
    if set_self:
        os.chmod(path, mode)
    
    # Only recurse if it's a directory
    if os.path.isdir(path) and (set_childen or set_children_files):
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path) and set_childen:
                if entry not in skip_children:
                    set_writable(full_path, set_childen, set_children_files, skip_children)
                else:
                    os.chmod(full_path, mode)
            else:
                if entry not in skip_children:
                    os.chmod(full_path, mode)

def _check_ex_lock(path:str) -> Optional[bool]:
    if not os.path.exists(path):
        return None
    for file in os.listdir(path):
        if file.endswith(".exlock"):
            return True
    return False

def set_tv_lock_instance(instance_id:str, table_name:str, db_dir:str):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    table_lock_path = os.path.join(lock_dir, table_name)
    table_full_path = os.path.join(db_dir, table_name)
    if instance_id == constants.ARTIFACT_FOLDER:
        skip_children = []
    else:
        skip_children = [constants.BUILDER_FOLDER, constants.META_DESCRIPTION_FILE]
    instance_lock_path = os.path.join(table_lock_path, instance_id)
    instance_full_path = os.path.join(table_full_path, instance_id)
    check_ex = _check_ex_lock(instance_lock_path)
    if check_ex is None:
        return
    elif not check_ex:
        set_not_writable(instance_full_path, set_childen = True, set_children_files=True, skip_children=skip_children)
    else:
        set_writable(instance_full_path, set_childen = True, set_children_files=True, skip_children=skip_children)

def set_tv_lock_table(table_name, db_dir):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    table_lock_path = os.path.join(lock_dir, table_name)
    table_full_path = os.path.join(db_dir, table_name)
    check_ex = _check_ex_lock(table_lock_path)
    if table_name == constants.CODE_FOLDER:
        if check_ex is None:
            raise tv_errors.TVFileError("Lock files not found") 
        if not check_ex:
            set_not_writable(table_full_path, set_childen = False)
        else:
            set_writable(table_full_path, set_childen = False)
    else:
        # we don't lock tables for now
        set_tv_lock_instance(constants.ARTIFACT_FOLDER, table_name, db_dir)
        for instance_id in os.listdir(table_lock_path):
            if not instance_id.startswith(".") and instance_id not in constants.ILLEGAL_TABLE_NAMES:
                instance_lock_path = os.path.join(table_lock_path, instance_id)
                if os.path.isdir(instance_lock_path):
                    set_tv_lock_instance(instance_id, table_name, db_dir)

def set_tv_lock_db(db_dir:str):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    set_tv_lock_table(constants.CODE_FOLDER, db_dir)
    for table_name in os.listdir(lock_dir):
        if not table_name.startswith(".") and table_name not in constants.ILLEGAL_TABLE_NAMES:
            table_lock_path = os.path.join(lock_dir, table_name)
            if os.path.isdir(table_lock_path):
                set_tv_lock_table(table_name, db_dir)

def set_tv_lock(instance_id:str, table_name:str, db_dir:str):
    if instance_id != "":
        set_tv_lock_instance(instance_id, table_name, db_dir)
    elif table_name != "":
        set_tv_lock_table(table_name, db_dir)
    else:
        set_tv_lock_db(db_dir)