from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, TypedDict

from backup_pilot.core.models import BackupType


class _BackupPoint(TypedDict, total=False):
    """
    Lightweight representation of a backup point in time.

    This intentionally keeps the shape generic so that engine-specific
    change-log positions (e.g. MySQL binlog coordinates, PostgreSQL WAL
    LSNs, MongoDB oplog timestamps) can be stored in the opaque
    `position` field.
    """

    backup_id: str
    backup_type: str
    created_at: str
    position: dict


class JobStateDict(TypedDict, total=False):
    """
    On-disk representation of a job's backup state.
    """

    job_id: str
    last_full: _BackupPoint
    last_backup: _BackupPoint


@dataclass
class BackupPoint:
    """
    In-memory representation of a backup point.
    """

    backup_id: str
    backup_type: BackupType
    created_at: datetime
    position: dict


@dataclass
class JobState:
    """
    Aggregated state for a backup job, including its last full backup
    and last backup of any type.
    """

    job_id: str
    last_full: Optional[BackupPoint] = None
    last_backup: Optional[BackupPoint] = None


class BackupMetadataStore:
    """
    File-based metadata store for backup job state.

    Each job_id is persisted as a JSON file under a configurable root
    directory. This implementation is intentionally simple and does not
    try to handle concurrent writers across multiple processes.
    """

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, job_id: str) -> Path:
        safe_id = job_id.replace("/", "_")
        return self._root_dir / f"{safe_id}.json"

    def get_job_state(self, job_id: str) -> JobState:
        path = self._path_for(job_id)
        if not path.exists():
            return JobState(job_id=job_id)

        import json

        data: JobStateDict
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        def _from_point(raw: Optional[_BackupPoint]) -> Optional[BackupPoint]:
            if not raw:
                return None
            created_at = datetime.fromisoformat(raw["created_at"])
            return BackupPoint(
                backup_id=raw["backup_id"],
                backup_type=BackupType(raw["backup_type"]),
                created_at=created_at,
                position=raw.get("position") or {},
            )

        return JobState(
            job_id=data.get("job_id", job_id),
            last_full=_from_point(data.get("last_full")),
            last_backup=_from_point(data.get("last_backup")),
        )

    def _save(self, state: JobState) -> None:
        import json

        def _to_point(point: Optional[BackupPoint]) -> Optional[_BackupPoint]:
            if not point:
                return None
            return _BackupPoint(
                backup_id=point.backup_id,
                backup_type=point.backup_type.value,
                created_at=point.created_at.isoformat(),
                position=point.position,
            )

        payload: JobStateDict = JobStateDict(
            job_id=state.job_id,
            last_full=_to_point(state.last_full),  # type: ignore[assignment]
            last_backup=_to_point(state.last_backup),  # type: ignore[assignment]
        )

        path = self._path_for(state.job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)

    def update_after_full(self, *, job_id: str, point: BackupPoint) -> JobState:
        """
        Record a completed full backup for the given job.
        """
        state = self.get_job_state(job_id)
        state.last_full = point
        state.last_backup = point
        self._save(state)
        return state
