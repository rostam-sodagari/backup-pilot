from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any

import pytest

from cryptography.fernet import Fernet

from backup_pilot.core.models import BackupRequest, BackupType, DatabaseType, RestoreRequest
from backup_pilot.encryption import FernetEncryptor
from backup_pilot.services.backup_service import BackupService
from backup_pilot.services.connection_service import ConnectionService
from backup_pilot.services.restore_service import RestoreService


class DummyLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def info(self, msg: str, *args, **kwargs) -> None:  # pragma: no cover - trivial
        self.messages.append((msg, kwargs))

    def warning(self, msg: str, *args, **kwargs) -> None:  # pragma: no cover - trivial
        self.messages.append((msg, kwargs))

    def error(self, msg: str, *args, **kwargs) -> None:  # pragma: no cover - trivial
        self.messages.append((msg, kwargs))

    def exception(self, msg: str, *args, **kwargs) -> None:  # pragma: no cover - trivial
        self.messages.append((msg, kwargs))


class DummyConnector:
    def __init__(self) -> None:
        self.connected = False
        self.backed_up = False
        self.restored = False

    def test_connection(self) -> None:
        self.connected = True

    def create_backup_stream(self, request: BackupRequest) -> BytesIO:
        self.backed_up = True
        return BytesIO(b"dummy-backup-data")

    def restore_from_stream(self, request: RestoreRequest, stream: BytesIO) -> None:
        assert stream.read() == b"dummy-backup-data"
        self.restored = True


class DummyStrategy:
    def run(self, connector: DummyConnector, request: BackupRequest):
        from backup_pilot.core.models import BackupResult, BackupStatus

        return BackupResult(
            backup_id="test-backup-id",
            status=BackupStatus.SUCCESS,
            started_at=datetime.utcnow(),
        )


class DummyStorage:
    def __init__(self) -> None:
        self.uploaded: dict[str, bytes] = {}
        self.downloaded_id: str | None = None

    def upload(self, backup_id: str, stream: BytesIO) -> str:
        self.uploaded[backup_id] = stream.read()
        return f"local://{backup_id}"

    def download(self, backup_id: str) -> BytesIO:
        self.downloaded_id = backup_id
        return BytesIO(self.uploaded[backup_id])

    def delete(self, backup_id: str) -> None:
        self.uploaded.pop(backup_id, None)


class DummyCompressor:
    def compress(self, raw_stream: BytesIO) -> BytesIO:
        return raw_stream

    def decompress(self, compressed_stream: BytesIO) -> BytesIO:
        return compressed_stream


class DummyEncryptor:
    def encrypt(self, stream: BytesIO) -> BytesIO:
        return stream

    def decrypt(self, encrypted_stream: BytesIO) -> BytesIO:
        return encrypted_stream


class DummyNotifier:
    def __init__(self) -> None:
        self.success_called = False
        self.failure_called = False

    def notify_success(self, result) -> None:  # pragma: no cover - trivial
        self.success_called = True

    def notify_failure(self, result, error: Exception) -> None:  # pragma: no cover - trivial
        self.failure_called = True


def test_backup_service_happy_path():
    connector = DummyConnector()
    strategy = DummyStrategy()
    storage = DummyStorage()
    compressor = DummyCompressor()
    encryptor = DummyEncryptor()
    notifier = DummyNotifier()
    logger = DummyLogger()

    service = BackupService(
        connector=connector,
        strategy=strategy,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
    )

    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.FULL,
    )

    result = service.run_backup(request)

    assert connector.connected is True
    assert connector.backed_up is True
    assert result.backup_id == "test-backup-id"
    assert result.storage_location == "local://test-backup-id"
    assert notifier.success_called is True


def test_restore_service_happy_path():
    connector = DummyConnector()
    storage = DummyStorage()
    compressor = DummyCompressor()
    encryptor = DummyEncryptor()
    notifier = DummyNotifier()
    logger = DummyLogger()

    # seed storage with a backup
    storage.upload("test-backup-id", BytesIO(b"dummy-backup-data"))

    service = RestoreService(
        connector=connector,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
    )

    request = RestoreRequest(
        db_type=DatabaseType.MYSQL,
        backup_id="test-backup-id",
    )

    result = service.run_restore(request)

    assert connector.connected is True
    assert connector.restored is True
    assert result.status.name == "SUCCESS"
    assert notifier.success_called is True


def test_backup_and_restore_with_fernet_encryption():
    """Full pipeline: backup with Fernet encryption, then restore with same key."""
    key = Fernet.generate_key()
    encryptor = FernetEncryptor(key)

    connector = DummyConnector()
    strategy = DummyStrategy()
    storage = DummyStorage()
    compressor = DummyCompressor()
    notifier = DummyNotifier()
    logger = DummyLogger()

    backup_service = BackupService(
        connector=connector,
        strategy=strategy,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
    )
    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.FULL,
    )
    result = backup_service.run_backup(request)
    assert result.backup_id == "test-backup-id"
    # Stored content should be encrypted (not raw "dummy-backup-data")
    stored = storage.uploaded["test-backup-id"]
    assert stored != b"dummy-backup-data"

    # Restore with same encryptor (decrypts then decompresses)
    connector2 = DummyConnector()
    restore_service = RestoreService(
        connector=connector2,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
    )
    restore_result = restore_service.run_restore(
        RestoreRequest(db_type=DatabaseType.MYSQL, backup_id="test-backup-id")
    )
    assert restore_result.status.name == "SUCCESS"
    assert connector2.restored is True


def test_connection_service_uses_connector():
    connector = DummyConnector()
    logger = DummyLogger()
    service = ConnectionService(connector=connector, logger=logger)

    service.test_connection()

    assert connector.connected is True

