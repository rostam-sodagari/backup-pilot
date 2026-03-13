from __future__ import annotations

from io import BytesIO
from typing import BinaryIO

from google.cloud import storage as gcs

from backup_pilot.core.exceptions import StorageError
from backup_pilot.storage.base import StorageBackendBase


class GCSStorageBackend(StorageBackendBase):
    """
    Google Cloud Storage backend.
    """

    def __init__(self, bucket: str, prefix: str | None = None) -> None:
        self._bucket_name = bucket
        self._prefix = (prefix or "").rstrip("/")
        self._client = gcs.Client()

    def _blob_name(self, backup_id: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{backup_id}.bak"
        return f"{backup_id}.bak"

    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        blob_name = self._blob_name(backup_id)
        try:
            bucket = self._client.bucket(self._bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_file(stream)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to upload backup to GCS") from exc
        return f"gs://{self._bucket_name}/{blob_name}"

    def download(self, backup_id: str) -> BinaryIO:
        blob_name = self._blob_name(backup_id)
        buffer = BytesIO()
        try:
            bucket = self._client.bucket(self._bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_file(buffer)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to download backup from GCS") from exc
        buffer.seek(0)
        return buffer

    def delete(self, backup_id: str) -> None:
        blob_name = self._blob_name(backup_id)
        try:
            bucket = self._client.bucket(self._bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to delete backup from GCS") from exc

