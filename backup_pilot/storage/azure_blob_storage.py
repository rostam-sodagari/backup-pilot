from __future__ import annotations

from io import BytesIO
from typing import BinaryIO

from azure.storage.blob import BlobServiceClient

from backup_pilot.core.exceptions import StorageError
from backup_pilot.storage.base import StorageBackendBase


class AzureBlobStorageBackend(StorageBackendBase):
    """
    Azure Blob Storage backend.
    """

    def __init__(
        self,
        container: str,
        connection_string: str | None = None,
        prefix: str | None = None,
    ) -> None:
        self._container = container
        self._prefix = (prefix or "").rstrip("/")
        if connection_string:
            self._client = BlobServiceClient.from_connection_string(connection_string)
        else:
            self._client = BlobServiceClient.from_connection_string(
                ""
            )  # pragma: no cover - placeholder

    def _blob_name(self, backup_id: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{backup_id}.bak"
        return f"{backup_id}.bak"

    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        blob_name = self._blob_name(backup_id)
        try:
            container_client = self._client.get_container_client(self._container)
            container_client.upload_blob(name=blob_name, data=stream, overwrite=True)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError("Failed to upload backup to Azure Blob Storage") from exc
        return f"azure://{self._container}/{blob_name}"

    def download(self, backup_id: str) -> BinaryIO:
        blob_name = self._blob_name(backup_id)
        buffer = BytesIO()
        try:
            container_client = self._client.get_container_client(self._container)
            downloader = container_client.download_blob(blob_name)
            buffer.write(downloader.readall())
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError(
                "Failed to download backup from Azure Blob Storage"
            ) from exc
        buffer.seek(0)
        return buffer

    def delete(self, backup_id: str) -> None:
        blob_name = self._blob_name(backup_id)
        try:
            container_client = self._client.get_container_client(self._container)
            container_client.delete_blob(blob_name)
        except Exception as exc:  # pragma: no cover - network specific
            raise StorageError(
                "Failed to delete backup from Azure Blob Storage"
            ) from exc
