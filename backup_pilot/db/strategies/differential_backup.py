from __future__ import annotations

from backup_pilot.core.exceptions import BackupError
from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import BackupRequest, BackupResult


class DifferentialBackupStrategy(BackupStrategy):
    """
    Placeholder differential backup strategy.

    For v1, this strategy is not implemented and will raise a clear error.
    """

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:  # pragma: no cover - simple guard
        raise BackupError("Differential backups are not yet supported for this database type.")

