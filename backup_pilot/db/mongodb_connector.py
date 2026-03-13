from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from io import BufferedReader
from typing import BinaryIO, Dict, Any

from backup_pilot.core.exceptions import ConnectionError
from backup_pilot.core.interfaces import DatabaseConnector
from backup_pilot.core.models import BackupRequest, RestoreRequest
from backup_pilot.db.base import DBConnectionParams


class MongoDBConnector(DatabaseConnector):
    """
    MongoDB implementation using `mongosh`, `mongodump`, and `mongorestore`.
    """

    def __init__(self, params: DBConnectionParams) -> None:
        self._p = params

    def _base_uri(self) -> str:
        if self._p.uri:
            return self._p.uri
        host = self._p.host or "localhost"
        port = self._p.port or 27017
        auth = ""
        if self._p.username and self._p.password:
            auth = f"{self._p.username}:{self._p.password}@"
        return f"mongodb://{auth}{host}:{port}"

    def test_connection(self) -> None:
        cmd = [
            "mongosh",
            self._base_uri(),
            "--eval",
            "db.runCommand({ ping: 1 })",
        ]
        try:
            subprocess.check_call(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except Exception as exc:  # pragma: no cover - depends on local tooling
            raise ConnectionError("Failed to connect to MongoDB") from exc

    def create_backup_stream(self, request: BackupRequest) -> BinaryIO:
        # Use archive mode to stream to stdout.
        cmd = [
            "mongodump",
            f"--uri={self._base_uri()}",
            "--archive=-",
        ]
        if self._p.database:
            cmd.append(f"--db={self._p.database}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        return BufferedReader(proc.stdout)

    def get_current_oplog_timestamp(self) -> Dict[str, Any]:
        """
        Return the current oplog timestamp for a replica set member.

        This is a best-effort helper for incremental/differential backups
        that consume oplog entries from a recorded timestamp.
        """
        cmd = [
            "mongosh",
            self._base_uri(),
            "--quiet",
            "--eval",
            "var last = db.getSiblingDB('local').oplog.rs.find().sort({ $natural: -1 }).limit(1).next(); "
            "if (last && last.ts) { "
            "  printjson({ ts: last.ts, wall: last.wall || null }); "
            "} else { printjson({ ts: null, wall: null }); }",
        ]
        try:
            raw = subprocess.check_output(
                cmd,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except Exception as exc:  # pragma: no cover - depends on local tooling
            raise ConnectionError("Failed to obtain MongoDB oplog position") from exc

        # The output will be a JSON document on the last line.
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if not lines:  # pragma: no cover - defensive branch
            return {"ts": None, "wall": None}

        import json

        try:
            doc = json.loads(lines[-1])
        except Exception:  # pragma: no cover - parse failures
            return {"ts": None, "wall": None}

        return {"ts": doc.get("ts"), "wall": doc.get("wall")}

    def restore_from_stream(self, request: RestoreRequest, stream: BinaryIO) -> None:
        cmd = [
            "mongorestore",
            f"--uri={self._base_uri()}",
            "--archive=-",
            "--drop",
        ]
        if self._p.database:
            cmd.append(f"--nsInclude={self._p.database}.*")

        proc = subprocess.Popen(
            cmd,
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
            raise ConnectionError("MongoDB restore failed")
