from __future__ import annotations

import os
import subprocess
from io import BufferedReader
from typing import BinaryIO

from backup_pilot.core.exceptions import ConnectionError
from backup_pilot.core.interfaces import DatabaseConnector
from backup_pilot.core.models import BackupRequest, RestoreRequest
from backup_pilot.db.base import DBConnectionParams


class MySQLConnector(DatabaseConnector):
    """
    MySQL implementation using the `mysql` and `mysqldump` client tools.
    """

    def __init__(self, params: DBConnectionParams) -> None:
        self._p = params

    def _base_env(self) -> dict[str, str]:
        env = os.environ.copy()
        if self._p.password:
            env["MYSQL_PWD"] = self._p.password
        return env

    def test_connection(self) -> None:
        cmd = [
            "mysql",
            "-h",
            self._p.host or "localhost",
            "-P",
            str(self._p.port or 3306),
            "-u",
            self._p.username or "",
            "-e",
            "SELECT 1;",
        ]
        if self._p.database:
            cmd.extend(["-D", self._p.database])

        try:
            subprocess.check_call(
                cmd,
                env=self._base_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:  # pragma: no cover - depends on local tooling
            raise ConnectionError("Failed to connect to MySQL") from exc

    def create_backup_stream(self, request: BackupRequest) -> BinaryIO:
        cmd = [
            "mysqldump",
            "-h",
            self._p.host or "localhost",
            "-P",
            str(self._p.port or 3306),
            "-u",
            self._p.username or "",
            "--single-transaction",
        ]
        if self._p.database:
            cmd.append(self._p.database)

        proc = subprocess.Popen(
            cmd,
            env=self._base_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        return BufferedReader(proc.stdout)

    def restore_from_stream(self, request: RestoreRequest, stream: BinaryIO) -> None:
        cmd = [
            "mysql",
            "-h",
            self._p.host or "localhost",
            "-P",
            str(self._p.port or 3306),
            "-u",
            self._p.username or "",
        ]
        if self._p.database:
            cmd.extend(["-D", self._p.database])

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
            raise ConnectionError("MySQL restore failed")
