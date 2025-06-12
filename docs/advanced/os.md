# Operating System Differences

Due to underlying file system differences, TableVault's behavior on Windows/external drives differs in the following three ways:

* The `metadata/lock.LOCK` file does not persist on Windows. This is a result of the underlying `filelock` library's implementation.

* Files are never marked as read-only on Windows/external drives (e.g. mounted Google Drive on Colab). Therefore, greater care must be taken to prevent accidental overwrites.

* Temporary files are copied rather than hardlinked for external drives. This can cause some slowdown in these systems.