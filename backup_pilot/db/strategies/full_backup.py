from __future__ import annotations

from datetime import datetime, timezone

from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import BackupRequest, BackupResult, BackupStatus


class FullBackupStrategy(BackupStrategy):
    """
    Simple full backup strategy.
    """

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:
        # Connector is responsible for the actual backup stream; we just generate metadata.
        started_at = datetime.now(timezone.utc)
        backup_id = started_at.strftime("%Y%m%d%H%M%S")
        return BackupResult(
            backup_id=backup_id,
            status=BackupStatus.RUNNING,
            started_at=started_at,
        )
