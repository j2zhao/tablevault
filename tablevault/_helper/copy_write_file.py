import os
import shutil
import tempfile
import contextlib
import errno
import stat
import time
from typing import Callable, Tuple

import yaml
import pandas as pd
from filelock import FileLock
from tablevault._defintions import constants


# ----------------------------------------------------------------------
# Copy‑on‑write helper class
# ----------------------------------------------------------------------
class CopyOnWriteFile:
    """
    Hardlink aware I/O helpers for a single repository directory.
    """

    _WRITE_FLAGS = set("wax+")
    _DELETE_RETRIES: int = 5
    _DELETE_SLEEP: float = 0.2  # seconds between retries

    # ─────────────── construction ───────────────
    def __init__(
        self,
        db_dir: str,
        check_hardlink: bool = True,
        has_hardlink: bool = None,
        lock_timeout: float = constants.TIMEOUT,
    ):
        self.db_dir = os.fspath(db_dir)
        lock_path = os.path.join(
            self.db_dir, constants.METADATA_FOLDER, constants.META_FILE_LOCK_FILE
        )
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self._lock = FileLock(lock_path, timeout=lock_timeout)
        if has_hardlink is not None:
            self._hardlink_supported = has_hardlink
        elif check_hardlink:
            self._hardlink_supported = self._detect_hardlink_support(self.db_dir)
        else:
            check_hardlink = False

    # ───── platform / capability helpers ──────
    @staticmethod
    def _detect_hardlink_support(test_dir: str | None = None) -> bool:
        """
        Return True iff the filesystem that backs *test_dir* supports hard links.
        Running the probe in the target directory avoids false-positives on
        FUSE mounts such as Google Drive.
        """
        # Default to a safe temp dir if caller gave nothing
        test_dir = os.fspath(test_dir or tempfile.gettempdir())

        src = dst = None
        try:
            fd, src = tempfile.mkstemp(dir=test_dir)
            os.close(fd)
            dst = f"{src}.lnk"
            os.link(src, dst)  # will raise OSError on Drive/FUSE
            return True
        except (OSError, NotImplementedError):
            return False
        finally:
            with contextlib.suppress(FileNotFoundError):
                if dst:
                    os.unlink(dst)
                if src:
                    os.unlink(src)

    @staticmethod
    def _is_write_mode(mode: str) -> bool:
        return bool(set(mode) & CopyOnWriteFile._WRITE_FLAGS)

    # ─────────── hard-link handling core ───────────
    def _break_link_if_needed(self, path: str) -> None:
        """Ensure *path* has link count 1 by copying to a temp file first."""
        if not self._hardlink_supported:
            return

        try:
            if os.stat(path).st_nlink <= 1:
                return
        except (AttributeError, OSError):
            return

        directory = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile(
            dir=directory,
            prefix=os.path.basename(path) + ".",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_name = tmp.name

        try:
            shutil.copy2(path, tmp_name)
            os.replace(tmp_name, path)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    def _split_links_under(self, root: str) -> None:
        if os.path.isfile(root):
            self._break_link_if_needed(root)
            return
        for parent, _dirs, files in os.walk(root):
            for name in files:
                self._break_link_if_needed(os.path.join(parent, name))

    # ─────────────── read / write helpers ───────────────
    def open(self, path: str, mode: str = "r", **kwargs):
        if self._is_write_mode(mode):
            with self._lock:
                self._break_link_if_needed(path)
        return open(path, mode, **kwargs)

    # JSON
    def read_json(self, path: str, **kwargs):
        with self.open(path, "r", encoding="utf-8") as f:
            import json

            return json.load(f, **kwargs)

    def write_json(self, path: str, obj, **kwargs):
        with self.open(path, "w", encoding="utf-8") as f:
            import json

            return json.dump(obj, f, **kwargs)

    # YAML
    def read_yaml(self, path: str, **kwargs):
        with self.open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f, **kwargs)

    def write_yaml(self, path: str, obj, **kwargs):
        with self.open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, **kwargs)

    # CSV
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        with self.open(path, "r", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return pd.read_csv(f, **kwargs)

    def write_csv(self, path: str, df: pd.DataFrame, **kwargs):
        with self.open(path, "w", encoding=kwargs.pop("encoding", "utf-8")) as f:
            df.to_csv(f, index=False, **kwargs)

    # ─────────────── safe destructive ops ───────────────
    @staticmethod
    def _handle_remove_readonly(
        func: Callable, path: str, exc_info: Tuple[type, BaseException, object]
    ):
        exc_type, exc_value, _ = exc_info
        if exc_type is PermissionError:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        else:
            raise exc_value

    def remove(self, path: str, *, missing_ok: bool = True):
        with self._lock:
            try:
                self._break_link_if_needed(path)
            except FileNotFoundError:
                if missing_ok:
                    return
                raise
            for attempt in range(self._DELETE_RETRIES):
                try:
                    os.remove(path)
                    return
                except FileNotFoundError:
                    if missing_ok:
                        return
                    raise
                except PermissionError:
                    if attempt + 1 == self._DELETE_RETRIES:
                        raise
                    time.sleep(self._DELETE_SLEEP)

    def rmtree(self, path: str, *, missing_ok: bool = True):
        with self._lock:
            if missing_ok and not os.path.exists(path):
                return
            self._split_links_under(path)
            for attempt in range(self._DELETE_RETRIES):
                try:
                    shutil.rmtree(path, onerror=self._handle_remove_readonly)
                    return
                except FileNotFoundError:
                    if missing_ok:
                        return
                    raise
                except OSError as err:
                    if (
                        err.errno not in (errno.EBUSY, errno.ENOTEMPTY)
                        or attempt + 1 == self._DELETE_RETRIES
                    ):
                        raise
                    time.sleep(self._DELETE_SLEEP)

    def rename(self, src: str, dst: str):
        with self._lock:
            os.rename(src, dst)  # atomic directory swap
        return dst

    # ─────────────── copy / move helpers ───────────────
    def copy2(self, src: str, dst: str, *, follow_symlinks: bool = True):
        with self._lock:
            if os.path.exists(dst):
                self._break_link_if_needed(dst)
            return shutil.copy2(src, dst, follow_symlinks=follow_symlinks)

    def copytree(
        self,
        src: str,
        dst: str,
        *,
        symlinks: bool = False,
        ignore=None,
        dirs_exist_ok: bool = False,
        copy_function=None,
    ):
        copy_fn = copy_function or self.copy2
        with self._lock:
            return shutil.copytree(
                src,
                dst,
                symlinks=symlinks,
                ignore=ignore,
                dirs_exist_ok=dirs_exist_ok,
                copy_function=copy_fn,
            )

    def move(self, src: str, dst: str, *, copy_function=None):
        cf = copy_function or self.copy2
        with self._lock:
            return shutil.move(src, dst, copy_function=cf)

    def makedirs(self, path: str, mode: int = 0o777, *, exist_ok: bool = True):
        with self._lock:
            os.makedirs(path, mode=mode, exist_ok=exist_ok)
        return path

    def linkfile(self, src: str, dst: str, overwrite: bool = False) -> str:
        with self._lock:
            if not self._hardlink_supported:
                return shutil.copy2(src, dst)

            # Ensure destination directory exists
            parent = os.path.dirname(dst)
            if parent:
                os.makedirs(parent, exist_ok=True)

            # Handle existing dst file
            if os.path.exists(dst):
                if not overwrite:
                    raise FileExistsError(f"Destination '{dst}' already exists.")
                os.unlink(dst)

            try:
                os.link(src, dst)
            except OSError:
                shutil.copy2(src, dst)

            return dst

    # ─────────────── hard‑link “copy‑tree” ───────────────
    def linktree(
        self,
        src: str,
        dst: str,
        *,
        dirs_exist_ok: bool = False,
        ignore=None,
    ):
        with self._lock:
            # Fallback if no hard‑link support
            if not self._hardlink_supported:
                return self.copytree(
                    src, dst, dirs_exist_ok=dirs_exist_ok, ignore=ignore
                )

            if os.path.exists(dst):
                if not dirs_exist_ok:
                    raise FileExistsError(dst)
            else:
                os.makedirs(dst, exist_ok=True)
            shutil.copystat(src, dst, follow_symlinks=False)

            def _ignored(dirpath, entries):
                if ignore is None:
                    return set()
                return set(ignore(dirpath, entries))

            for dirpath, dirnames, filenames in os.walk(src):
                ignored = _ignored(dirpath, dirnames + filenames)
                for dirname in dirnames[:]:
                    if dirname in ignored:
                        dirnames.remove(dirname)
                        continue
                    rel = os.path.relpath(os.path.join(dirpath, dirname), src)
                    dst_dir = os.path.join(dst, rel)
                    if not os.path.exists(dst_dir):
                        os.makedirs(dst_dir, exist_ok=True)
                    shutil.copystat(
                        os.path.join(dirpath, dirname), dst_dir, follow_symlinks=False
                    )
                for filename in filenames:
                    if filename in ignored:
                        continue
                    src_file = os.path.join(dirpath, filename)
                    rel = os.path.relpath(src_file, src)
                    dst_file = os.path.join(dst, rel)
                    if os.path.exists(dst_file):
                        os.unlink(dst_file)
                    try:
                        os.link(src_file, dst_file)
                    except OSError:
                        shutil.copy2(src_file, dst_file)
            return dst
