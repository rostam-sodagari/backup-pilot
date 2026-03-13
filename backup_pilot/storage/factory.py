from __future__ import annotations

from typing import Any, Dict

from backup_pilot.storage.azure_blob_storage import AzureBlobStorageBackend
from backup_pilot.storage.gcs_storage import GCSStorageBackend
from backup_pilot.storage.local_storage import LocalStorageBackend
from backup_pilot.storage.s3_storage import S3StorageBackend


def create_storage_backend(config: Dict[str, Any]):
    """
    Create a storage backend from a simple config dictionary.

    Expected shape:
    {
        "type": "local" | "s3" | "gcs" | "azure",
        ... provider-specific fields ...
    }
    """
    backend_type = config.get("type")
    if backend_type == "local":
        return LocalStorageBackend(root_dir=config["root_dir"])
    if backend_type == "s3":
        return S3StorageBackend(bucket=config["bucket"], prefix=config.get("prefix"))
    if backend_type == "gcs":
        return GCSStorageBackend(bucket=config["bucket"], prefix=config.get("prefix"))
    if backend_type == "azure":
        return AzureBlobStorageBackend(
            container=config["container"],
            connection_string=config.get("connection_string"),
            prefix=config.get("prefix"),
        )
    raise ValueError(f"Unsupported storage backend type: {backend_type}")

