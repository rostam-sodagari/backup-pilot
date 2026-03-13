from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


from backup_pilot.config.models import (
    AppConfig,
    BackupProfile,
    DatabaseProfile,
    StorageProfile,
)
from backup_pilot.core.models import BackupRecord, DatabaseType, BackupType
from backup_pilot.services.rotation_service import run_rotation


def test_run_rotation_empty_history(tmp_path: Path) -> None:
    history_path = tmp_path / "history.jsonl"
    history_path.touch()
    cfg = AppConfig(
        databases={"d1": DatabaseProfile(type=DatabaseType.MYSQL)},
        storage={
            "local": StorageProfile(
                type="local", options={"root_dir": str(tmp_path / "backups")}
            )
        },
        backups={
            "p1": BackupProfile(database="d1", storage="local", retention_count=2)
        },
    )
    removed = run_rotation(config=cfg, history_path=history_path)
    assert removed == 0


def test_run_rotation_retention_count(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    (backup_dir / "id1.bak").write_bytes(b"a")
    (backup_dir / "id2.bak").write_bytes(b"b")
    (backup_dir / "id3.bak").write_bytes(b"c")

    history_path = tmp_path / "history.jsonl"
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records = [
        BackupRecord(
            backup_id="id1",
            profile_name="p1",
            db_type=DatabaseType.MYSQL,
            created_at=base,
        ),
        BackupRecord(
            backup_id="id2",
            profile_name="p1",
            db_type=DatabaseType.MYSQL,
            created_at=base,
        ),
        BackupRecord(
            backup_id="id3",
            profile_name="p1",
            db_type=DatabaseType.MYSQL,
            created_at=base,
        ),
    ]
    with history_path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(r.model_dump_json() + "\n")

    cfg = AppConfig(
        databases={"d1": DatabaseProfile(type=DatabaseType.MYSQL)},
        storage={
            "local": StorageProfile(type="local", options={"root_dir": str(backup_dir)})
        },
        backups={
            "p1": BackupProfile(database="d1", storage="local", retention_count=1)
        },
    )
    removed = run_rotation(config=cfg, history_path=history_path)
    assert removed == 2
    # retention_count=1 keeps the newest (first after sort); we had id1, id2, id3 same time so order is stable
    assert (backup_dir / "id1.bak").exists() is True
    assert (backup_dir / "id2.bak").exists() is False
    assert (backup_dir / "id3.bak").exists() is False

    lines = history_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1
    kept = BackupRecord.model_validate_json(lines[0])
    assert kept.backup_id == "id1"


def test_run_rotation_no_retention_skipped(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    (backup_dir / "id1.bak").write_bytes(b"a")
    history_path = tmp_path / "history.jsonl"
    record = BackupRecord(
        backup_id="id1",
        profile_name="p1",
        db_type=DatabaseType.MYSQL,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    history_path.write_text(record.model_dump_json() + "\n", encoding="utf-8")

    cfg = AppConfig(
        databases={"d1": DatabaseProfile(type=DatabaseType.MYSQL)},
        storage={
            "local": StorageProfile(type="local", options={"root_dir": str(backup_dir)})
        },
        backups={"p1": BackupProfile(database="d1", storage="local")},  # no retention
    )
    removed = run_rotation(config=cfg, history_path=history_path)
    assert removed == 0
    assert (backup_dir / "id1.bak").exists()


def test_run_rotation_never_deletes_full_with_incremental(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()

    # Old full that has a newer incremental depending on it.
    (backup_dir / "full1.bak").write_bytes(b"full1")
    (backup_dir / "inc1.bak").write_bytes(b"inc1")

    history_path = tmp_path / "history.jsonl"
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records = [
        BackupRecord(
            backup_id="full1",
            profile_name="p1",
            db_type=DatabaseType.MYSQL,
            created_at=base,
            backup_type=BackupType.FULL,
        ),
        BackupRecord(
            backup_id="inc1",
            profile_name="p1",
            db_type=DatabaseType.MYSQL,
            created_at=base.replace(day=2),
            backup_type=BackupType.INCREMENTAL,
        ),
    ]
    with history_path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(r.model_dump_json() + "\n")

    cfg = AppConfig(
        databases={"d1": DatabaseProfile(type=DatabaseType.MYSQL)},
        storage={
            "local": StorageProfile(type="local", options={"root_dir": str(backup_dir)})
        },
        backups={
            # Aggressive retention that would normally delete the older full
            "p1": BackupProfile(database="d1", storage="local", retention_count=1)
        },
    )

    removed = run_rotation(config=cfg, history_path=history_path)
    # Rotation should not remove the full backup, even though it is older,
    # because it has a related incremental backup.
    assert removed == 0
    assert (backup_dir / "full1.bak").exists()
    assert (backup_dir / "inc1.bak").exists()

    lines = history_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
