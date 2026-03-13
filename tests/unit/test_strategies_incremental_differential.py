from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from backup_pilot.core.models import BackupRequest, BackupType, DatabaseType
from backup_pilot.db.strategies.differential_backup import DifferentialBackupStrategy
from backup_pilot.db.strategies.incremental_backup import IncrementalBackupStrategy
from backup_pilot.metadata.store import BackupMetadataStore, BackupPoint


class DummyConnector:
    def __init__(self) -> None:
        self.stream_created = False

    def test_connection(self) -> None:
        pass

    def create_backup_stream(self, request: BackupRequest) -> BytesIO:
        self.stream_created = True
        return BytesIO(b"dummy")

    def restore_from_stream(self, request, stream) -> None:
        pass

    def get_current_binlog_position(self):
        return {"file": "binlog.000001", "position": 42}

    def get_current_lsn(self):
        return {"lsn": "0/16B6C50"}

    def get_current_oplog_timestamp(self):
        return {"ts": {"t": 1, "i": 2}, "wall": None}


def _seed_full_backup(store: BackupMetadataStore, job_id: str) -> None:
    created_at = datetime.now(timezone.utc)
    point = BackupPoint(
        backup_id="full-001",
        backup_type=BackupType.FULL,
        created_at=created_at,
        position={},
    )
    store.update_after_full(job_id=job_id, point=point)


def test_incremental_requires_previous_backup(tmp_path: Path) -> None:
    store = BackupMetadataStore(tmp_path)
    job_id = "job-inc"
    connector = DummyConnector()
    strategy = IncrementalBackupStrategy(store, job_id)

    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.INCREMENTAL,
    )

    from backup_pilot.core.exceptions import BackupError

    try:
        strategy.run(connector, request)
        assert False, "Expected BackupError when no previous backup exists"
    except BackupError:
        pass


def test_incremental_uses_previous_backup_state(tmp_path: Path) -> None:
    store = BackupMetadataStore(tmp_path)
    job_id = "job-inc2"
    connector = DummyConnector()

    # Seed a previous full backup so incremental is allowed.
    _seed_full_backup(store, job_id)

    strategy = IncrementalBackupStrategy(store, job_id)
    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.INCREMENTAL,
    )

    result = strategy.run(connector, request)
    assert connector.stream_created is True
    assert result.backup_id


def test_differential_requires_full_backup(tmp_path: Path) -> None:
    store = BackupMetadataStore(tmp_path)
    job_id = "job-diff"
    connector = DummyConnector()
    strategy = DifferentialBackupStrategy(store, job_id)

    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.DIFFERENTIAL,
    )

    from backup_pilot.core.exceptions import BackupError

    try:
        strategy.run(connector, request)
        assert False, "Expected BackupError when no full backup exists"
    except BackupError:
        pass


def test_differential_uses_full_backup_state(tmp_path: Path) -> None:
    store = BackupMetadataStore(tmp_path)
    job_id = "job-diff2"
    connector = DummyConnector()

    _seed_full_backup(store, job_id)

    strategy = DifferentialBackupStrategy(store, job_id)
    request = BackupRequest(
        db_type=DatabaseType.MYSQL,
        backup_type=BackupType.DIFFERENTIAL,
    )

    result = strategy.run(connector, request)
    assert connector.stream_created is True
    assert result.backup_id

