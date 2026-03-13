from __future__ import annotations

from io import BufferedReader
from pathlib import Path
from typing import BinaryIO

from backup_pilot.core.exceptions import StorageError
from backup_pilot.storage.base import StorageBackendBase


class LocalStorageBackend(StorageBackendBase):
    """
    Stores backups on the local filesystem under a root directory.
    """

    def __init__(self, root_dir: str) -> None:
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, backup_id: str) -> Path:
        return self._root / f"{backup_id}.bak"

    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        path = self._path_for(backup_id)
        try:
            with open(path, "wb") as fh:
                for chunk in iter(lambda: stream.read(8192), b""):
                    if not chunk:
                        break
                    fh.write(chunk)
        except Exception as exc:  # pragma: no cover - filesystem specific
            raise StorageError(f"Failed to write local backup file: {path}") from exc
        return str(path)

    def download(self, backup_id: str) -> BinaryIO:
        path = self._path_for(backup_id)
        if not path.exists():  # pragma: no cover - trivial
            raise StorageError(f"Local backup file does not exist: {path}")
        return BufferedReader(open(path, "rb"))

    def delete(self, backup_id: str) -> None:
        path = self._path_for(backup_id)
        if path.exists():
            try:
                path.unlink()
            except Exception as exc:  # pragma: no cover - filesystem specific
                raise StorageError(
                    f"Failed to delete local backup file: {path}"
                ) from exc
