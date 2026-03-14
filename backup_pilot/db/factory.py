from __future__ import annotations

from pathlib import Path

from backup_pilot.core.models import BackupType, DatabaseType
from backup_pilot.db.base import DBConnectionParams
from backup_pilot.db.mongodb_connector import MongoDBConnector
from backup_pilot.db.mysql_connector import MySQLConnector
from backup_pilot.db.postgres_connector import PostgresConnector
from backup_pilot.db.sqlite_connector import SQLiteConnector
from backup_pilot.db.strategies.full_backup import FullBackupStrategy
from backup_pilot.metadata.store import BackupMetadataStore


def create_connector(params: DBConnectionParams):
    if params.db_type == DatabaseType.MYSQL:
        return MySQLConnector(params)
    if params.db_type == DatabaseType.POSTGRESQL:
        return PostgresConnector(params)
    if params.db_type == DatabaseType.MONGODB:
        return MongoDBConnector(params)
    if params.db_type == DatabaseType.SQLITE:
        return SQLiteConnector(params)
    raise ValueError(f"Unsupported database type: {params.db_type}")


def create_strategy(
    backup_type: BackupType,
    *,
    job_id: str,
    metadata_dir: str | None = None,
):
    """
    Factory for backup strategies.

    job_id is used for metadata store state (e.g. for future use). The
    metadata directory defaults to a `.backup_pilot` folder next to the
    current working directory.
    """
    root = Path(metadata_dir) if metadata_dir else Path(".backup_pilot")
    store = BackupMetadataStore(root)
    return FullBackupStrategy(metadata_store=store, job_id=job_id)
