from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class DatabaseType(str, Enum):
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"
    SQLITE = "sqlite"


class BackupType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class BackupStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class BackupRequest(BaseModel):
    db_type: DatabaseType
    backup_type: BackupType
    profile_name: Optional[str] = None
    options: Dict[str, str] = {}


class BackupResult(BaseModel):
    backup_id: str
    status: BackupStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    storage_location: Optional[str] = None
    message: Optional[str] = None


class RestoreRequest(BaseModel):
    db_type: DatabaseType
    backup_id: str
    profile_name: Optional[str] = None
    tables: Optional[List[str]] = None
    collections: Optional[List[str]] = None
    options: Dict[str, str] = {}


class RestoreResult(BaseModel):
    status: BackupStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    message: Optional[str] = None


class BackupRecord(BaseModel):
    """
    Lightweight record of a completed backup, used for local history listing.
    """

    backup_id: str
    profile_name: Optional[str] = None
    db_type: DatabaseType
    database_name: Optional[str] = None
    storage_location: Optional[str] = None
    created_at: datetime
    finished_at: Optional[datetime] = None
    size_bytes: Optional[int] = None

