from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from backup_pilot.core.exceptions import BackupError
from backup_pilot.core.interfaces import (
    BackupStrategy,
    Compressor,
    DatabaseConnector,
    Encryptor,
    LoggerLike,
    Notifier,
    StorageBackend,
)
from backup_pilot.core.models import (
    BackupRequest,
    BackupResult,
    BackupStatus,
    BackupType,
    DatabaseType,
)


class BackupService:
    """
    Orchestrates backup operations end-to-end.
    """

    def __init__(
        self,
        connector: DatabaseConnector,
        strategy: BackupStrategy,
        storage: StorageBackend,
        compressor: Compressor,
        encryptor: Encryptor,
        notifier: Optional[Notifier],
        logger: LoggerLike,
        *,
        profile_name: Optional[str] = None,
        db_profile_name: Optional[str] = None,
        db_type: Optional[DatabaseType | str] = None,
        storage_profile_name: Optional[str] = None,
        storage_type: Optional[str] = None,
        backup_type: Optional[BackupType | str] = None,
        encryption_mode: Optional[str] = None,
    ) -> None:
        self._connector = connector
        self._strategy = strategy
        self._storage = storage
        self._compressor = compressor
        self._encryptor = encryptor
        self._notifier = notifier
        self._logger = logger

        # Base logging context reused across all log entries for this service.
        self._base_log_context: Dict[str, Any] = {
            "profile_name": profile_name,
            "db_profile_name": db_profile_name,
            "db_type": getattr(db_type, "value", db_type),
            "storage_profile_name": storage_profile_name,
            "storage_type": storage_type,
            "backup_type": getattr(backup_type, "value", backup_type),
            "encryption_mode": encryption_mode,
        }

    def _log_extra(self, **overrides: Any) -> Dict[str, Dict[str, Any]]:
        """
        Build a standardized extra payload for logging.

        All backup-related log entries share the same base context so log consumers
        can rely on stable keys across start, success, and error paths.
        """
        merged: Dict[str, Any] = {**self._base_log_context, **overrides}
        return {"extra": merged}

    def run_backup(self, request: BackupRequest) -> BackupResult:
        started_at = datetime.utcnow()
        result = BackupResult(
            backup_id="",
            status=BackupStatus.PENDING,
            started_at=started_at,
            db_profile_name=request.profile_name,
            db_type=request.db_type,
        )

        self._logger.info(
            "Starting backup",
            **self._log_extra(
                profile_name=request.profile_name
                or self._base_log_context.get("profile_name"),
                db_type=getattr(request.db_type, "value", request.db_type),
                backup_type=getattr(request.backup_type, "value", request.backup_type),
                request=request.model_dump(),
            ),
        )

        try:
            self._connector.test_connection()
        except Exception as exc:  # pragma: no cover - simple pass-through
            self._logger.exception(
                "Connection test failed",
                **self._log_extra(
                    profile_name=request.profile_name
                    or self._base_log_context.get("profile_name"),
                    db_type=getattr(request.db_type, "value", request.db_type),
                    backup_type=getattr(
                        request.backup_type, "value", request.backup_type
                    ),
                    status=BackupStatus.FAILED.value,
                ),
            )
            result.status = BackupStatus.FAILED
            result.finished_at = datetime.utcnow()
            result.message = str(exc)
            if self._notifier:
                self._notifier.notify_failure(result, exc)
            raise BackupError("Database connection test failed") from exc

        try:
            # Strategy is responsible for creating a raw backup stream and basic metadata.
            strategy_result = self._strategy.run(self._connector, request)
            raw_stream = self._connector.create_backup_stream(request)

            compressed_stream = self._compressor.compress(raw_stream)
            stream_to_upload = self._encryptor.encrypt(compressed_stream)

            backup_id = (
                strategy_result.backup_id
                or strategy_result.started_at.strftime("%Y%m%d%H%M%S")
            )
            location = self._storage.upload(backup_id, stream_to_upload)

            result.backup_id = backup_id
            result.status = BackupStatus.SUCCESS
            result.finished_at = datetime.utcnow()
            result.storage_location = location
            result.message = "Backup completed successfully."

            self._logger.info(
                "Backup completed",
                **self._log_extra(
                    profile_name=request.profile_name
                    or self._base_log_context.get("profile_name"),
                    db_type=getattr(request.db_type, "value", request.db_type),
                    backup_type=getattr(
                        request.backup_type, "value", request.backup_type
                    ),
                    backup_id=backup_id,
                    storage_location=location,
                    duration_seconds=(result.finished_at - started_at).total_seconds(),
                    status=BackupStatus.SUCCESS.value,
                ),
            )

            if self._notifier:
                self._notifier.notify_success(result)

            return result

        except Exception as exc:
            result.status = BackupStatus.FAILED
            result.finished_at = datetime.utcnow()
            result.message = str(exc)
            if self._notifier:
                self._notifier.notify_failure(result, exc)
            self._logger.exception(
                "Backup failed",
                **self._log_extra(
                    profile_name=request.profile_name
                    or self._base_log_context.get("profile_name"),
                    db_type=getattr(request.db_type, "value", request.db_type),
                    backup_type=getattr(
                        request.backup_type, "value", request.backup_type
                    ),
                    backup_id=result.backup_id or None,
                    storage_location=result.storage_location,
                    status=BackupStatus.FAILED.value,
                ),
            )
            raise BackupError("Backup operation failed") from exc
