from __future__ import annotations

from datetime import datetime
from typing import Optional

from backup_pilot.core.exceptions import RestoreError
from backup_pilot.core.interfaces import (
    Compressor,
    DatabaseConnector,
    Encryptor,
    LoggerLike,
    Notifier,
    StorageBackend,
)
from backup_pilot.core.models import BackupStatus, RestoreRequest, RestoreResult


class RestoreService:
    """
    Orchestrates restore operations end-to-end.
    """

    def __init__(
        self,
        connector: DatabaseConnector,
        storage: StorageBackend,
        compressor: Compressor,
        encryptor: Encryptor,
        notifier: Optional[Notifier],
        logger: LoggerLike,
    ) -> None:
        self._connector = connector
        self._storage = storage
        self._compressor = compressor
        self._encryptor = encryptor
        self._notifier = notifier
        self._logger = logger

    def run_restore(self, request: RestoreRequest) -> RestoreResult:
        started_at = datetime.utcnow()
        result = RestoreResult(
            status=BackupStatus.PENDING,
            started_at=started_at,
        )

        self._logger.info("Starting restore", extra={"request": request.model_dump()})

        try:
            self._connector.test_connection()
        except Exception as exc:  # pragma: no cover - simple pass-through
            self._logger.exception("Connection test failed for restore")
            result.status = BackupStatus.FAILED
            result.finished_at = datetime.utcnow()
            result.message = str(exc)
            if self._notifier:
                self._notifier.notify_failure(result, exc)
            raise RestoreError("Database connection test failed for restore") from exc

        try:
            encrypted_stream = self._storage.download(request.backup_id)
            compressed_stream = self._encryptor.decrypt(encrypted_stream)
            raw_stream = self._compressor.decompress(compressed_stream)

            self._connector.restore_from_stream(request, raw_stream)

            result.status = BackupStatus.SUCCESS
            result.finished_at = datetime.utcnow()
            result.message = "Restore completed successfully."

            self._logger.info(
                "Restore completed",
                extra={
                    "backup_id": request.backup_id,
                    "duration_seconds": (
                        result.finished_at - started_at
                    ).total_seconds(),
                },
            )

            if self._notifier:
                self._notifier.notify_success(result)

            return result

        except Exception as exc:
            self._logger.exception("Restore failed")
            result.status = BackupStatus.FAILED
            result.finished_at = datetime.utcnow()
            result.message = str(exc)
            if self._notifier:
                self._notifier.notify_failure(result, exc)
            raise RestoreError("Restore operation failed") from exc
