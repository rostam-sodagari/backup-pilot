from __future__ import annotations

from backup_pilot.core.exceptions import BackupError
from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import BackupRequest, BackupResult


class IncrementalBackupStrategy(BackupStrategy):
    """
    Placeholder incremental backup strategy.

    For v1, most engines will not support true incremental backups via this CLI.
    Attempting to use this strategy will raise a clear error.
    """

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:  # pragma: no cover - simple guard
        raise BackupError("Incremental backups are not yet supported for this database type.")

