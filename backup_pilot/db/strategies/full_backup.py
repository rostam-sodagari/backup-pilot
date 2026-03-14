from __future__ import annotations

from datetime import datetime, timezone

from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import (
    BackupRequest,
    BackupResult,
    BackupStatus,
    BackupType,
)
from backup_pilot.metadata.store import BackupMetadataStore, BackupPoint


class FullBackupStrategy(BackupStrategy):
    """
    Full backup strategy.

    A full backup always captures a complete logical dump via the
    connector. When a metadata store is provided, it records this
    backup for history; the state is available for future incremental
    and differential support.
    """

    def __init__(
        self,
        metadata_store: BackupMetadataStore | None = None,
        job_id: str | None = None,
    ) -> None:
        self._store = metadata_store
        self._job_id = job_id

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:
        started_at = datetime.now(timezone.utc)
        backup_id = started_at.strftime("%Y%m%d%H%M%S")
        return BackupResult(
            backup_id=backup_id,
            status=BackupStatus.RUNNING,
            started_at=started_at,
        )

    def record_success(
        self, backup_id: str, backup_type: BackupType, created_at: datetime
    ) -> None:
        """Record a completed full backup in the metadata store after successful upload."""
        if self._store and self._job_id:
            point = BackupPoint(
                backup_id=backup_id,
                backup_type=backup_type,
                created_at=created_at,
                position={},
            )
            self._store.update_after_full(job_id=self._job_id, point=point)
