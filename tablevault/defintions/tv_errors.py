
class TableVaultError(Exception):
    """Base Class for all TableVault Errors."""

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
    """Re-execute Completed process"""


class TVFileError(TableVaultError):
    pass


class TVPromptError(TableVaultError):
    pass


class TVTableError(TableVaultError):
    pass


class TVImplementationError(TableVaultError):

    pass