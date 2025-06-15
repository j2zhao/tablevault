import os
import stat
from tablevault._defintions import constants
from typing import Optional
from tablevault._defintions import tv_errors
# from tablevault._helper.external_drive_check import is_on_separate_mount


def _can_program_modify_permissions(filepath: str) -> bool:
    if os.name == "nt":
        return False
    # if is_on_separate_mount(filepath):
    #     return False
    current_euid = os.geteuid()
    if current_euid == 0:
        return True
    file_stat = os.stat(filepath)
    file_owner_uid = file_stat.st_uid
    return current_euid == file_owner_uid


def _set_not_writable(
    path: str,
    set_children: bool = True,
    set_children_files=True,
    skip_children=[],
    set_self=True,
):
    """If `path` is a file, set *just* that file to read+execute (no
    write).

    If `path` is a directory, set it (and everything underneath) to read+execute.
    """
    # First, chmod the path itself
    mode = (
        stat.S_IREAD
        | stat.S_IRGRP
        | stat.S_IROTH  # read for owner, group, others
        | stat.S_IXUSR
        | stat.S_IXGRP
        | stat.S_IXOTH  # execute for owner, group, others
    )
    if not os.path.exists(path):
        return
    if set_self:
        os.chmod(path, mode)

    # Only recurse if it's a directory
    if os.path.isdir(path) and (set_children or set_children_files):
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path) and set_children:
                if entry not in skip_children:
                    _set_not_writable(
                        full_path, set_children, set_children_files, skip_children
                    )
                else:
                    os.chmod(full_path, mode)
            elif not os.path.isdir(full_path):
                if entry not in skip_children and set_children_files:
                    os.chmod(full_path, mode)


def _set_writable(
    path: str,
    set_children: bool = True,
    set_children_files=True,
    skip_children=[],
    set_self=True,
):
    """If `path` is a file or directory, set it to read+write+execute
    for owner, group, and others.

    If it's a directory, recurse into children.
    """
    # Always grant r, w, x to owner/group/others
    mode = (
        stat.S_IREAD
        | stat.S_IWRITE
        | stat.S_IXUSR
        | stat.S_IRGRP
        | stat.S_IWGRP
        | stat.S_IXGRP
        | stat.S_IROTH
        | stat.S_IWOTH
        | stat.S_IXOTH
    )
    if not os.path.exists(path):
        return
    if set_self:
        os.chmod(path, mode)

    # Only recurse if it's a directory
    if os.path.isdir(path) and (set_children or set_children_files):
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path) and set_children:
                if entry not in skip_children:
                    _set_writable(
                        full_path, set_children, set_children_files, skip_children
                    )
                else:
                    os.chmod(full_path, mode)
            elif not os.path.isdir(full_path):
                if entry not in skip_children and set_children_files:
                    os.chmod(full_path, mode)


def _check_ex_lock(path: str) -> Optional[bool]:
    if not os.path.exists(path):
        return None
    for file in os.listdir(path):
        if file.endswith(".exlock"):
            return True
    return False


def _set_tv_lock_instance(instance_id: str, table_name: str, db_dir: str):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    table_lock_path = os.path.join(lock_dir, table_name)
    table_full_path = os.path.join(db_dir, table_name)
    if instance_id == constants.ARTIFACT_FOLDER:
        skip_children = []
        set_children = False
    else:
        skip_children = [
            constants.BUILDER_FOLDER,
            constants.META_DESCRIPTION_FILE,
            constants.ARTIFACT_FOLDER,
        ]
        set_children = True
    instance_lock_path = os.path.join(table_lock_path, instance_id)
    instance_full_path = os.path.join(table_full_path, instance_id)
    check_ex = _check_ex_lock(instance_lock_path)
    if check_ex is None:
        return
    elif not check_ex:
        _set_not_writable(
            instance_full_path, set_children=set_children, skip_children=skip_children
        )
    else:
        _set_writable(
            instance_full_path, set_children=set_children, skip_children=skip_children
        )


def _set_tv_lock_table(table_name, db_dir):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    table_lock_path = os.path.join(lock_dir, table_name)
    table_full_path = os.path.join(db_dir, table_name)
    check_ex = _check_ex_lock(table_lock_path)
    if table_name == constants.CODE_FOLDER:
        if check_ex is None:
            raise tv_errors.TVFileError("Lock files not found")
        if not check_ex:
            _set_not_writable(table_full_path, set_children_files=False)
        else:
            _set_writable(table_full_path, set_children_files=False)
    else:
        # we don't lock tables for now
        _set_tv_lock_instance(constants.ARTIFACT_FOLDER, table_name, db_dir)
        for instance_id in os.listdir(table_lock_path):
            if (
                not instance_id.startswith(".")
                and instance_id not in constants.ILLEGAL_TABLE_NAMES
            ):
                instance_lock_path = os.path.join(table_lock_path, instance_id)
                if os.path.isdir(instance_lock_path):
                    _set_tv_lock_instance(instance_id, table_name, db_dir)


def _set_tv_lock_db(db_dir: str):
    lock_dir = os.path.join(db_dir, constants.LOCK_FOLDER)
    _set_tv_lock_table(constants.CODE_FOLDER, db_dir)
    for table_name in os.listdir(lock_dir):
        if (
            not table_name.startswith(".")
            and table_name not in constants.ILLEGAL_TABLE_NAMES
        ):
            table_lock_path = os.path.join(lock_dir, table_name)
            if os.path.isdir(table_lock_path):
                _set_tv_lock_table(table_name, db_dir)


def set_tv_lock(instance_id: str, table_name: str, db_dir: str):
    if not _can_program_modify_permissions(db_dir):
        return
    if instance_id != "":
        _set_tv_lock_instance(instance_id, table_name, db_dir)
    elif table_name != "":
        _set_tv_lock_table(table_name, db_dir)
    else:
        _set_tv_lock_db(db_dir)


def set_writable(db_dir: str):
    if not _can_program_modify_permissions(db_dir):
        return
    _set_writable(db_dir)
