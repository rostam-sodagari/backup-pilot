from __future__ import annotations

from datetime import datetime
from typing import Optional

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
from backup_pilot.core.models import BackupRequest, BackupResult, BackupStatus


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
    ) -> None:
        self._connector = connector
        self._strategy = strategy
        self._storage = storage
        self._compressor = compressor
        self._encryptor = encryptor
        self._notifier = notifier
        self._logger = logger

    def run_backup(self, request: BackupRequest) -> BackupResult:
        started_at = datetime.utcnow()
        result = BackupResult(
            backup_id="",
            status=BackupStatus.PENDING,
            started_at=started_at,
        )

        self._logger.info("Starting backup", extra={"request": request.model_dump()})

        try:
            self._connector.test_connection()
        except Exception as exc:  # pragma: no cover - simple pass-through
            self._logger.exception("Connection test failed")
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

            backup_id = strategy_result.backup_id or strategy_result.started_at.strftime(
                "%Y%m%d%H%M%S"
            )
            location = self._storage.upload(backup_id, stream_to_upload)

            result.backup_id = backup_id
            result.status = BackupStatus.SUCCESS
            result.finished_at = datetime.utcnow()
            result.storage_location = location
            result.message = "Backup completed successfully."

            self._logger.info(
                "Backup completed",
                extra={
                    "backup_id": backup_id,
                    "location": location,
                    "duration_seconds": (result.finished_at - started_at).total_seconds(),
                },
            )

            if self._notifier:
                self._notifier.notify_success(result)

            return result

        except Exception as exc:
            self._logger.exception("Backup failed")
            result.status = BackupStatus.FAILED
            result.finished_at = datetime.utcnow()
            result.message = str(exc)
            if self._notifier:
                self._notifier.notify_failure(result, exc)
            raise BackupError("Backup operation failed") from exc

