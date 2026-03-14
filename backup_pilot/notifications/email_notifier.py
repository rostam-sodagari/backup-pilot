from __future__ import annotations

import smtplib
from email.message import EmailMessage

from backup_pilot.core.models import BackupResult, RestoreResult
from backup_pilot.logging.logger import get_logger
from backup_pilot.notifications.base import NotifierBase


class EmailNotifier(NotifierBase):
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str | None,
        password: str | None,
        from_addr: str,
        to_addr: str,
    ) -> None:
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._username = username
        self._password = password
        self._from = from_addr
        self._to = to_addr

    def _send(
        self, subject: str, body: str
    ) -> None:  # pragma: no cover - external SMTP
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self._from
        msg["To"] = self._to
        msg.set_content(body)

        with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
            if self._username and self._password:
                server.starttls()
                server.login(self._username, self._password)
            server.send_message(msg)

    def notify_success(self, result: BackupResult | RestoreResult) -> None:
        try:
            subject = "BackupPilot job succeeded"
            body = f"Status: {result.status.value}"
            if getattr(result, "backup_id", None):
                body += f"\nBackup ID: {getattr(result, 'backup_id')}"
            if getattr(result, "storage_location", None):
                body += f"\nLocation: {result.storage_location}"
            if result.finished_at and result.started_at:
                delta = result.finished_at - result.started_at
                body += f"\nDuration: {delta.total_seconds():.1f}s"
            self._send(subject, body)
        except Exception as exc:
            get_logger().warning("Email notification failed: %s", exc)

    def notify_failure(
        self, result: BackupResult | RestoreResult, error: Exception
    ) -> None:
        try:
            subject = "BackupPilot job failed"
            body = f"Status: {result.status.value}\nError: {error}"
            if getattr(result, "backup_id", None):
                body += f"\nBackup ID: {getattr(result, 'backup_id')}"
            if getattr(result, "db_profile_name", None):
                body += f"\nDB profile: {result.db_profile_name}"
            if getattr(result, "db_type", None):
                body += f"\nDB type: {getattr(result, 'db_type').value}"
            if getattr(result, "storage_profile_name", None):
                body += f"\nStorage: {result.storage_profile_name}"
            if getattr(result, "storage_type", None):
                body += f"\nStorage type: {result.storage_type}"
            if getattr(result, "encryption_mode", None):
                body += f"\nEncryption: {result.encryption_mode}"
            if getattr(result, "error_code", None):
                body += f"\nError code: {result.error_code}"
            self._send(subject, body)
        except Exception as exc:
            get_logger().warning("Email notification failed: %s", exc)
