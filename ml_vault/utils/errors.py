# ml_vault/errors.py
from __future__ import annotations
from typing import Optional


class MLVaultError(Exception):
    """Base for all ML Vault errors; include optional db context."""
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


class ValidationError(MLVaultError):
    """Bad user input or shape."""
    code = "validation_error"


class NotFoundError(MLVaultError):
    """Requested item/doc/collection key does not exist."""
    code = "not_found"


class DuplicateItemError(MLVaultError):
    """Unique/duplicate constraint violation for the given key."""
    code = "duplicate_item"


class ConflictError(MLVaultError):
    """Concurrent write / revision conflict; often retryable."""
    code = "conflict"


class LockTimeoutError(MLVaultError):
    """Failed to obtain or persist a lock/timestamp within the timeout."""
    code = "lock_timeout"


class DBError(MLVaultError):
    """Fallback for other database errors (unmapped Arango codes)."""
    code = "db_error"
