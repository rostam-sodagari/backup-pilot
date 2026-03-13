from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from backup_pilot.core.models import DatabaseType


@dataclass
class DBConnectionParams:
    db_type: DatabaseType
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    uri: Optional[str] = None
    path: Optional[str] = None  # for SQLite

