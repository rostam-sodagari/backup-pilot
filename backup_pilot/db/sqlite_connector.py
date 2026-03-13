from __future__ import annotations

import shutil
from io import BufferedReader
from pathlib import Path
from typing import BinaryIO

from backup_pilot.core.exceptions import ConnectionError
from backup_pilot.core.interfaces import DatabaseConnector
from backup_pilot.core.models import BackupRequest, RestoreRequest
from backup_pilot.db.base import DBConnectionParams


class SQLiteConnector(DatabaseConnector):
    """
    SQLite implementation using filesystem-level backup.
    """

    def __init__(self, params: DBConnectionParams) -> None:
        if not params.path:
            raise ValueError("SQLiteConnector requires a file path")
        self._path = Path(params.path)

    def test_connection(self) -> None:
        if not self._path.exists():  # pragma: no cover - trivial
            raise ConnectionError(f"SQLite database file does not exist: {self._path}")

    def create_backup_stream(self, request: BackupRequest) -> BinaryIO:
        # For simplicity, perform a file-level copy.
        if not self._path.exists():  # pragma: no cover - trivial
            raise ConnectionError(f"SQLite database file does not exist: {self._path}")
        tmp_path = self._path.with_suffix(self._path.suffix + ".bak")
        shutil.copy2(self._path, tmp_path)
        return open(tmp_path, "rb")

    def restore_from_stream(self, request: RestoreRequest, stream: BinaryIO) -> None:
        tmp_path = self._path.with_suffix(self._path.suffix + ".restore")
        with open(tmp_path, "wb") as f:
            shutil.copyfileobj(stream, f)
        shutil.move(tmp_path, self._path)

