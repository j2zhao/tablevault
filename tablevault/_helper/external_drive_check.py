import os
import sys


def is_on_separate_mount(path: str, root: str = "/") -> bool:
    """True  → *path* resides on a different filesystem (mount) than *root*."""
    # ---------- Windows (not relevant in Colab) ----------
    if os.name == "nt":
        return (
            os.path.splitdrive(os.path.realpath(path))[0].upper()
            != os.path.splitdrive(os.path.realpath(root))[0].upper()
        )

    # ---------- Quick POSIX check via st_dev ----------
    try:
        if (
            os.stat(os.path.realpath(path)).st_dev
            != os.stat(os.path.realpath(root)).st_dev
        ):
            return True
    except OSError:
        return False  # cannot stat – safest fallback

    # ---------- Linux refinement using /proc/self/mountinfo ----------
    if sys.platform.startswith("linux"):

        def _parse_mountinfo():
            rows = []
            try:
                with open("/proc/self/mountinfo", "r", encoding="utf-8") as fh:
                    for line in fh:
                        parts = line.split()
                        rows.append(
                            (parts[4], parts[8], parts[9])
                        )  # (mountpoint,fstype,source)
            except FileNotFoundError:
                pass
            return sorted(rows, key=lambda r: len(r[0]), reverse=True)

        def _find_mount(p: str):
            for mnt in _parse_mountinfo():
                if p == mnt[0] or p.startswith(mnt[0].rstrip("/") + "/"):
                    return mnt
            return None

        rp, rr = map(os.path.realpath, (path, root))
        return _find_mount(rp) != _find_mount(rr)

    # ---------- macOS / *BSD fallback via psutil ----------
    try:
        import psutil

        mounts = {p.mountpoint for p in psutil.disk_partitions(all=True)}

        def _best(p):
            return max(
                (m for m in mounts if p.startswith(m.rstrip("/") + "/") or p == m),
                key=len,
                default=None,
            )

        return _best(os.path.realpath(path)) != _best(os.path.realpath(root))
    except ImportError:
        return False
