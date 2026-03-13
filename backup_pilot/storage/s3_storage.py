from __future__ import annotations

from io import BytesIO
from typing import BinaryIO

import boto3

from backup_pilot.core.exceptions import StorageError
from backup_pilot.storage.base import StorageBackendBase


class S3StorageBackend(StorageBackendBase):
    """
    AWS S3 storage backend using boto3.
    """

    def __init__(self, bucket: str, prefix: str | None = None) -> None:
        self._bucket = bucket
        self._prefix = (prefix or "").rstrip("/")
        self._client = boto3.client("s3")

    def _key_for(self, backup_id: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{backup_id}.bak"
        return f"{backup_id}.bak"

    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        key = self._key_for(backup_id)
        try:
            self._client.upload_fileobj(stream, self._bucket, key)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to upload backup to S3") from exc
        return f"s3://{self._bucket}/{key}"

    def download(self, backup_id: str) -> BinaryIO:
        key = self._key_for(backup_id)
        buffer = BytesIO()
        try:
            self._client.download_fileobj(self._bucket, key, buffer)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to download backup from S3") from exc
        buffer.seek(0)
        return buffer

    def delete(self, backup_id: str) -> None:
        key = self._key_for(backup_id)
        try:
            self._client.delete_object(Bucket=self._bucket, Key=key)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to delete backup from S3") from exc
