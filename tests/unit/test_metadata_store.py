from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from backup_pilot.core.models import BackupType
from backup_pilot.metadata.store import BackupMetadataStore, BackupPoint


def test_metadata_store_persists_and_loads_job_state(tmp_path: Path) -> None:
    store = BackupMetadataStore(tmp_path)
    job_id = "test-job"

    created_at = datetime.now(timezone.utc)
    point = BackupPoint(
        backup_id="backup-001",
        backup_type=BackupType.FULL,
        created_at=created_at,
        position={"engine": "mysql", "file": "binlog.000001", "position": 1234},
    )

    store.update_after_full(job_id=job_id, point=point)

    loaded = store.get_job_state(job_id)
    assert loaded.last_full is not None
    assert loaded.last_full.backup_id == "backup-001"
    assert loaded.last_full.backup_type == BackupType.FULL
    assert loaded.last_full.position["file"] == "binlog.000001"
