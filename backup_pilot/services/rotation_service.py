from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from backup_pilot.config.models import AppConfig
from backup_pilot.core.models import BackupRecord
from backup_pilot.storage.factory import create_storage_backend


def run_rotation(
    config: AppConfig,
    history_path: Path,
    profile_filter: Optional[str] = None,
    logger: Optional[object] = None,
) -> int:
    """
    Apply retention policy per profile: delete backups beyond retention_count
    and/or older than retention_days, then rewrite the history file.
    Returns the number of backups removed.
    """
    if not history_path.exists():
        return 0

    records: list[BackupRecord] = []
    with history_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = BackupRecord.model_validate_json(line)
            except Exception:
                continue
            if profile_filter and record.profile_name != profile_filter:
                continue
            records.append(record)

    if not records:
        return 0

    # Group by profile
    by_profile: dict[str, list[BackupRecord]] = defaultdict(list)
    for r in records:
        name = r.profile_name or ""
        by_profile[name].append(r)

    removed_ids: set[str] = set()
    now = datetime.now(timezone.utc)

    for profile_name, profile_records in by_profile.items():
        if profile_name not in config.backups:
            continue
        backup_profile = config.backups[profile_name]
        retention_count = backup_profile.retention_count
        retention_days = backup_profile.retention_days
        if retention_count is None and retention_days is None:
            continue

        storage_profile = config.storage[backup_profile.storage]
        storage = create_storage_backend(
            {"type": storage_profile.type, **storage_profile.options}
        )

        # Sort newest first
        profile_records.sort(key=lambda r: r.created_at, reverse=True)

        to_remove: list[BackupRecord] = []
        for i, record in enumerate(profile_records):
            remove = False
            if retention_count is not None and i >= retention_count:
                remove = True
            if retention_days is not None:
                # Treat created_at as naive UTC if it has no tzinfo
                created = record.created_at
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                if created < now - timedelta(days=retention_days):
                    remove = True

            if remove:
                to_remove.append(record)

        for record in to_remove:
            try:
                storage.delete(record.backup_id)
                removed_ids.add(record.backup_id)
                if logger and hasattr(logger, "info"):
                    logger.info(
                        "Rotated backup",
                        extra={"backup_id": record.backup_id, "profile": profile_name},
                    )
            except Exception as exc:
                if logger and hasattr(logger, "warning"):
                    logger.warning(
                        "Failed to delete backup %s: %s",
                        record.backup_id,
                        exc,
                    )

    if not removed_ids:
        return 0

    # Rewrite history file without removed records
    kept_lines: list[str] = []
    with history_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                record = BackupRecord.model_validate_json(line_stripped)
                if record.backup_id not in removed_ids:
                    kept_lines.append(line.rstrip("\n"))
            except Exception:
                kept_lines.append(line.rstrip("\n"))

    with history_path.open("w", encoding="utf-8") as fh:
        for kept in kept_lines:
            fh.write(kept + "\n")

    return len(removed_ids)
