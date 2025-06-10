# Operating System Differences

Due to underlying file system differences, TableVault's behavior on Windows differs in the following three ways:

* The `metadata/lock.LOCK` file does not persist on Windows. This is a result of the underlying `filelock` library's implementation.

* Files are never marked as read-only on Windows. Therefore, greater care must be taken to prevent accidental overwrites.