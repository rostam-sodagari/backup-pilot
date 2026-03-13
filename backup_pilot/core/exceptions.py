class BackupPilotError(Exception):
    """Base class for all BackupPilot-specific errors."""


class ConnectionError(BackupPilotError):
    """Raised when a database connection cannot be established or validated."""


class BackupError(BackupPilotError):
    """Raised when a backup operation fails."""


class StorageError(BackupPilotError):
    """Raised when storing or retrieving backup artifacts fails."""


class RestoreError(BackupPilotError):
    """Raised when a restore operation fails."""
