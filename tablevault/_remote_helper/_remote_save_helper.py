import logging
import shutil
from pathlib import Path
import subprocess

# ────────── logging helper ──────────
_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def configure_file_logger(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(str(log_file))
    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        logger.addHandler(fh)
    return logger


# ────────── purge helper ──────────
def _purge_directory(dir_path: Path, logger: logging.Logger) -> None:
    """Delete everything inside *dir_path* (keeps the directory itself)."""
    if not dir_path.exists():
        return
    for item in dir_path.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        except Exception:
            logger.warning("Could not remove %s", item, exc_info=True)


# ────────── tar-and-copy helper ──────────
def compress_and_copy(
    source_dir: str | Path,
    drive_dir: str | Path,
    log_file: str | Path,
) -> None:
    """
    Mirror *source_dir* into *drive_dir* via a .tar.gz archive.

    Steps:
    1.  Purge drive_dir (mirrors `rsync --delete`).
    2.  Create `<source_dir>.tar.gz` *next to* the source, containing **only
        the contents** of source_dir (no wrapper folder).
    3.  Move the archive into drive_dir, unpack it there, and delete the
        archive—leaving drive_dir an exact replica of source_dir.
    """
    src = Path(source_dir).expanduser().resolve()
    dst = Path(drive_dir).expanduser().resolve()
    logger = configure_file_logger(Path(log_file))

    if not src.is_dir():
        msg = f"Source directory not found: {src}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    try:
        dst.mkdir(parents=True, exist_ok=True)

        logger.info("Purging destination directory %s …", dst)
        _purge_directory(dst, logger)

        logger.info("Creating tar.gz beside %s …", src)
        archive_path = Path(
            shutil.make_archive(
                base_name=str(src),  # “/path/to/mydata” → mydata.tar.gz
                format="gztar",  # gzip-compressed tarball
                root_dir=str(src),  # pack *contents* only
                base_dir=".",
            )
        )
        logger.info("Archive created → %s", archive_path)
        final_archive = dst / archive_path.name
        shutil.move(archive_path, final_archive)
        logger.info("Archive moved → %s", final_archive)
        logger.info("Extracting archive in place …")
        shutil.unpack_archive(final_archive, extract_dir=dst, filter=None)
        final_archive.unlink()
        logger.info("Extraction complete and archive removed")
    except Exception:
        logger.exception("tar_gz_and_copy failed")
        raise


def rsync_to_drive(
    source_dir: str | Path,
    drive_dir: str | Path,
    log_file: str | Path,
) -> None:
    """
    Mirror *source_dir* into *drive_dir* with rsync, deleting extras.
    Result is identical to `zip_and_copy`.
    """
    src = Path(source_dir).expanduser().resolve()
    dst = Path(drive_dir).expanduser().resolve()
    logger = configure_file_logger(Path(log_file))

    cmd = [
        "rsync",
        "-avL",
        "--delete",
        "--progress",
        "--no-perms",
        f"{src}/",  # trailing slash → copy contents only
        str(dst),
    ]
    logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        if result.stdout:
            logger.info("rsync stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("rsync stderr:\n%s", result.stderr)
        logger.info("rsync synchronization complete")

    except FileNotFoundError as e:
        logger.error("'rsync' not found: is it installed?")
        raise e
    except subprocess.CalledProcessError as e:
        logger.error("rsync exited with code %s", e.returncode)
        logger.error("stdout:\n%s", e.stdout)
        logger.error("stderr:\n%s", e.stderr)
        raise
