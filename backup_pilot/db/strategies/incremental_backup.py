from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

from backup_pilot.core.exceptions import BackupError, ConnectionError
from backup_pilot.core.interfaces import BackupStrategy, DatabaseConnector
from backup_pilot.core.models import BackupRequest, BackupResult, BackupStatus, DatabaseType
from backup_pilot.metadata.store import BackupMetadataStore, BackupPoint


class IncrementalBackupStrategy(BackupStrategy):
    """
    Simple incremental backup strategy built on database-native logs.

    This strategy relies on an external `BackupMetadataStore` to track
    the last backup position per logical job and uses engine-specific
    helpers on the connector to obtain the current change-log position.
    """

    def __init__(self, metadata_store: BackupMetadataStore, job_id: str) -> None:
        self._store = metadata_store
        self._job_id = job_id

    def _position_key(self, request: BackupRequest) -> str:
        return request.db_type.value

    def _build_position(
        self, connector: DatabaseConnector, request: BackupRequest
    ) -> Dict[str, Any]:
        if request.db_type == DatabaseType.MYSQL:
            getter = getattr(connector, "get_current_binlog_position", None)
            if not callable(getter):
                raise BackupError("Incremental backups require binlog support.")
            try:
                position = getter()
            except ConnectionError as exc:
                raise BackupError(
                    "Incremental MySQL backups require binary logging to be enabled "
                    "and `SHOW MASTER STATUS` to return a valid row. "
                    "Please ensure binlog is enabled and accessible for this user."
                ) from exc
            return {"engine": "mysql", **position}
        if request.db_type == DatabaseType.POSTGRESQL:
            getter = getattr(connector, "get_current_lsn", None)
            if not callable(getter):
                raise BackupError("Incremental backups require WAL support.")
            try:
                position = getter()
            except ConnectionError as exc:
                raise BackupError(
                    "Incremental PostgreSQL backups require WAL to be enabled "
                    "and the current LSN to be readable for this user."
                ) from exc
            return {"engine": "postgresql", **position}
        if request.db_type == DatabaseType.MONGODB:
            getter = getattr(connector, "get_current_oplog_timestamp", None)
            if not callable(getter):
                raise BackupError("Incremental backups require oplog support.")
            try:
                position = getter()
            except ConnectionError as exc:
                raise BackupError(
                    "Incremental MongoDB backups require the replica set oplog "
                    "to be available and readable for this user."
                ) from exc
            return {"engine": "mongodb", **position}
        raise BackupError(f"Incremental backups are not supported for {request.db_type.value}.")

    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:
        state = self._store.get_job_state(self._job_id)
        if not state.last_backup:
            raise BackupError(
                "No previous backup state found for this job. "
                "Run a full backup before attempting an incremental backup."
            )

        started_at = datetime.now(timezone.utc)
        backup_id = started_at.strftime("%Y%m%d%H%M%S")

        # At this version, we still take a logical dump using the connector
        # but record change-log positions so that future implementations can
        # narrow the data set based on logs.
        stream = connector.create_backup_stream(request)

        position = self._build_position(connector, request)
        point = BackupPoint(
            backup_id=backup_id,
            backup_type=request.backup_type,
            created_at=started_at,
            position=position,
        )
        self._store.update_after_incremental(job_id=self._job_id, point=point)

        return BackupResult(
            backup_id=backup_id,
            status=BackupStatus.RUNNING,
            started_at=started_at,
        )
