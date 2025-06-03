class TableVaultError(Exception):
    """Base Class for all TableVault Errors."""

    pass


class TVArtifactError(TableVaultError):
    "Error Related to Artifact Datatype"

    pass


class TVArgumentError(TableVaultError):
    "Data not correctly specified (either YAML or call Arguments)"

    pass


class TVLockError(TableVaultError):
    "Could not access Locks"

    pass


class TVOpenAIError(TableVaultError):
    "Error Related to OpenAI"

    pass


class TVProcessError(TableVaultError):
    """Re-execute Completed process."""

    pass


class TVForcedError(TableVaultError):
    pass


class TVFileError(TableVaultError):
    pass


class TVBuilderError(TableVaultError):
    pass


class TVTableError(TableVaultError):
    pass


class TVImplementationError(TableVaultError):
    pass


class TableReferenceError(Exception):
    pass
