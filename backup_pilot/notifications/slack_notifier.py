from __future__ import annotations

from typing import Any, Dict

import requests

from backup_pilot.core.models import BackupResult, RestoreResult
from backup_pilot.logging.logger import get_logger
from backup_pilot.notifications.base import NotifierBase


class SlackNotifier(NotifierBase):
    def __init__(self, webhook_url: str) -> None:
        self._webhook_url = webhook_url

    def _build_payload(
        self,
        title: str,
        result: BackupResult | RestoreResult,
        error: Exception | None = None,
    ) -> Dict[str, Any]:
        text = f"*{title}*\nStatus: {result.status.value}"
        if getattr(result, "backup_id", None):
            text += f"\nBackup ID: {getattr(result, 'backup_id')}"
        if getattr(result, "storage_location", None):
            text += f"\nLocation: {result.storage_location}"
        if getattr(result, "db_profile_name", None):
            text += f"\nDB profile: {result.db_profile_name}"
        if getattr(result, "db_type", None):
            # db_type is an enum when present
            db_type_value = getattr(result, "db_type").value
            text += f"\nDB type: {db_type_value}"
        if getattr(result, "storage_profile_name", None):
            text += f"\nStorage: {result.storage_profile_name}"
        if getattr(result, "storage_type", None):
            text += f"\nStorage type: {result.storage_type}"
        if getattr(result, "encryption_mode", None):
            text += f"\nEncryption: {result.encryption_mode}"
        if getattr(result, "error_code", None):
            text += f"\nError code: {result.error_code}"
        if error:
            text += f"\nError: `{error}`"
        if result.finished_at and result.started_at:
            delta = result.finished_at - result.started_at
            text += f"\nDuration: {delta.total_seconds():.1f}s"
        return {"text": text}

    def notify_success(
        self, result: BackupResult | RestoreResult
    ) -> None:  # pragma: no cover - outbound HTTP
        try:
            payload = self._build_payload("BackupPilot job succeeded", result)
            requests.post(self._webhook_url, json=payload, timeout=5)
        except Exception as exc:
            get_logger().warning("Slack notification failed: %s", exc)

    def notify_failure(
        self, result: BackupResult | RestoreResult, error: Exception
    ) -> None:  # pragma: no cover - outbound HTTP
        try:
            payload = self._build_payload("BackupPilot job failed", result, error)
            requests.post(self._webhook_url, json=payload, timeout=5)
        except Exception as exc:
            get_logger().warning("Slack notification failed: %s", exc)
