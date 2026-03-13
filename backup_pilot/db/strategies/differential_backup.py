from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

from backup_pilot.core.exceptions import BackupError
from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import BackupRequest, BackupResult, BackupStatus, DatabaseType
from backup_pilot.metadata.store import BackupMetadataStore, BackupPoint


class DifferentialBackupStrategy(BackupStrategy):
    """
    Differential backup strategy based on database-native change logs.

    Differential backups capture changes since the last full backup for
    the job. This implementation records positions and performs a
    logical backup via the connector for now.
    """

    def __init__(self, metadata_store: BackupMetadataStore, job_id: str) -> None:
        self._store = metadata_store
        self._job_id = job_id

    def _build_position(
        self, connector: DatabaseConnector, request: BackupRequest
    ) -> Dict[str, Any]:
        if request.db_type == DatabaseType.MYSQL:
            getter = getattr(connector, "get_current_binlog_position", None)
            if not callable(getter):
                raise BackupError("Differential backups require binlog support.")
            return {"engine": "mysql", **getter()}
        if request.db_type == DatabaseType.POSTGRESQL:
            getter = getattr(connector, "get_current_lsn", None)
            if not callable(getter):
                raise BackupError("Differential backups require WAL support.")
            return {"engine": "postgresql", **getter()}
        if request.db_type == DatabaseType.MONGODB:
            getter = getattr(connector, "get_current_oplog_timestamp", None)
            if not callable(getter):
                raise BackupError("Differential backups require oplog support.")
            return {"engine": "mongodb", **getter()}
        raise BackupError(
            f"Differential backups are not supported for {request.db_type.value}."
        )

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:
        state = self._store.get_job_state(self._job_id)
        if not state.last_full:
            raise BackupError(
                "No full backup state found for this job. "
                "Run a full backup before attempting a differential backup."
            )

        started_at = datetime.now(timezone.utc)
        backup_id = started_at.strftime("%Y%m%d%H%M%S")

        stream = connector.create_backup_stream(request)

        position = self._build_position(connector, request)
        point = BackupPoint(
            backup_id=backup_id,
            backup_type=request.backup_type,
            created_at=started_at,
            position=position,
        )
        self._store.update_after_differential(job_id=self._job_id, point=point)

        return BackupResult(
            backup_id=backup_id,
            status=BackupStatus.RUNNING,
            started_at=started_at,
        )
