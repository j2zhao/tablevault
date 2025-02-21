

class DataVaultError(Exception):
    """Base Class for all Datavault Errors."""
    pass

class DVArgumentError(DataVaultError):
    "Data not correctly specified (either YAML or call Arguments)"
    pass

class DVLockError(DataVaultError):
    "Could not access Locks"
    pass

class DVOpenAIError(DataVaultError):
    "Error Related to OpenAI"
    pass


class DVProcessDuplicationError(DataVaultError):
    """Re-execute Completed process"""

class DVFileError(DataVaultError):
    pass

class DVPromptError(DataVaultError):
    pass