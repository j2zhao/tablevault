# Operating System Differences

Due to underlying file system differences, TableVault exhibits distinct behaviors on Windows and external drives in the following ways:

* The `metadata/lock.LOCK` file does not persist on Windows. This behavior is due to the implementation specifics of the underlying `filelock` library.

* Files are never marked as read-only on Windows or external drives (e.g., mounted Google Drive on Colab). Users must exercise additional caution to prevent unintended overwrites.

* Temporary files are copied rather than hardlinked when using external drives. This copying mechanism can lead to reduced performance in these environments.