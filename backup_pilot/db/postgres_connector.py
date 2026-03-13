from __future__ import annotations

import os
import subprocess
from io import BufferedReader
from typing import BinaryIO

from backup_pilot.core.exceptions import ConnectionError
from backup_pilot.core.interfaces import DatabaseConnector
from backup_pilot.core.models import BackupRequest, RestoreRequest
from backup_pilot.db.base import DBConnectionParams


class PostgresConnector(DatabaseConnector):
    """
    PostgreSQL implementation using `psql` and `pg_dump`.
    """

    def __init__(self, params: DBConnectionParams) -> None:
        self._p = params

    def _base_env(self) -> dict[str, str]:
        env = os.environ.copy()
        if self._p.password:
            env["PGPASSWORD"] = self._p.password
        return env

    def test_connection(self) -> None:
        cmd = [
            "psql",
            "-h",
            self._p.host or "localhost",
            "-p",
            str(self._p.port or 5432),
            "-U",
            self._p.username or "",
            "-c",
            "SELECT 1;",
        ]
        if self._p.database:
            cmd.extend(["-d", self._p.database])

        try:
            subprocess.check_call(cmd, env=self._base_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as exc:  # pragma: no cover - depends on local tooling
            raise ConnectionError("Failed to connect to PostgreSQL") from exc

    def create_backup_stream(self, request: BackupRequest) -> BinaryIO:
        cmd = [
            "pg_dump",
            "-h",
            self._p.host or "localhost",
            "-p",
            str(self._p.port or 5432),
            "-U",
            self._p.username or "",
            "-Fc",
        ]
        if self._p.database:
            cmd.extend(["-d", self._p.database])

        proc = subprocess.Popen(
            cmd,
            env=self._base_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        return BufferedReader(proc.stdout)

    def restore_from_stream(self, request: RestoreRequest, stream: BinaryIO) -> None:
        # Use pg_restore for custom format, piped from stdin.
        cmd = [
            "pg_restore",
            "-h",
            self._p.host or "localhost",
            "-p",
            str(self._p.port or 5432),
            "-U",
            self._p.username or "",
            "-d",
            self._p.database or "",
        ]
        proc = subprocess.Popen(
            cmd,
            env=self._base_env(),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        assert proc.stdin is not None
        for chunk in iter(lambda: stream.read(8192), b""):
            proc.stdin.write(chunk)
        proc.stdin.close()
        ret = proc.wait()
        if ret != 0:  # pragma: no cover - depends on local tooling
            raise ConnectionError("PostgreSQL restore failed")

