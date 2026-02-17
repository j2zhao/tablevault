# tablevault/errors.py
from __future__ import annotations
from typing import Optional


class TableVaultError(Exception):
    """Base for all TableVault errors; include optional db context."""
    code: str = "unknown"

    def __init__(
        self,
        message: str,
        *,
        operation: Optional[str] = None,
        collection: Optional[str] = None,
        key: Optional[str] = None,
        arango_code: Optional[int] = None,
        arango_http: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.collection = collection
        self.key = key
        self.arango_code = arango_code
        self.arango_http = arango_http


class ValidationError(TableVaultError):
    """Bad user input or shape."""
    code = "validation_error"


class NotFoundError(TableVaultError):
    """Requested item/doc/collection key does not exist."""
    code = "not_found"


class DuplicateItemError(TableVaultError):
    """Unique/duplicate constraint violation for the given key."""
    code = "duplicate_item"


class ConflictError(TableVaultError):
    """Concurrent write / revision conflict; often retryable."""
    code = "conflict"


class LockTimeoutError(TableVaultError):
    """Failed to obtain or persist a lock/timestamp within the timeout."""
    code = "lock_timeout"


class DBError(TableVaultError):
    """Fallback for other database errors (unmapped Arango codes)."""
    code = "db_error"
