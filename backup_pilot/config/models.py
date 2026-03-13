from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, Field

from backup_pilot.core.models import BackupType, DatabaseType


class LoggingConfig(BaseModel):
    level: Optional[str] = "INFO"
    file: Optional[str] = None
    json_format: Optional[bool] = Field(
        False, alias="json"
    )  # use JSON formatter when True


class DatabaseProfile(BaseModel):
    type: DatabaseType
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    uri: Optional[str] = None
    path: Optional[str] = None


class StorageProfile(BaseModel):
    type: str
    options: Dict[str, str]


class NotificationConfig(BaseModel):
    slack: Optional[Dict[str, str]] = None
    email: Optional[Dict[str, str]] = None


class BackupProfile(BaseModel):
    database: str
    storage: str
    backup_type: BackupType = BackupType.FULL
    compression: str = "gzip"
    encryption: str = "none"
    retention_count: Optional[int] = None
    retention_days: Optional[int] = None


class AppConfig(BaseModel):
    databases: Dict[str, DatabaseProfile]
    storage: Dict[str, StorageProfile]
    backups: Dict[str, BackupProfile]
    notifications: Optional[NotificationConfig] = None
    logging: Optional[LoggingConfig] = None
