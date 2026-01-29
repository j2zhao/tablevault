import os
from contextlib import contextmanager
from typing import Tuple, Any, Iterator
from pathlib import Path


@contextmanager
def _exclusive_file_lock(f) -> Iterator[None]:
    if os.name == "nt":
        import msvcrt

        # Lock the whole file. msvcrt.locking requires a byte count.
        # We'll lock a very large region starting at 0.
        try:
            f.flush()
        except Exception:
            pass

        try:
            f.seek(0)
        except Exception:
            pass

        # Use a huge size to cover "the whole file"
        nbytes = 0x7FFFFFFF
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, nbytes)
        try:
            yield
        finally:
            try:
                f.seek(0)
            except Exception:
                pass
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, nbytes)

    else:
        import fcntl

        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def log_tuple(log_file: str, record: Tuple[Any, ...], *, fsync: bool = True) -> None:
    path = Path(log_file).expanduser()
    if path.suffix == "" or path.is_dir():
        path.mkdir(parents=True, exist_ok=True)
        path = path / "log.txt"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)

    line = repr(record) + "\n"
    with open(path, "a", encoding="utf-8", buffering=1, newline="\n") as f:
        with _exclusive_file_lock(f):
            f.write(line)
            f.flush()
            if fsync:
                os.fsync(f.fileno())
