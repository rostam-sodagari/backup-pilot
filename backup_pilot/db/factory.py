from __future__ import annotations

from pathlib import Path

from backup_pilot.core.models import BackupType, DatabaseType
from backup_pilot.db.base import DBConnectionParams
from backup_pilot.db.mongodb_connector import MongoDBConnector
from backup_pilot.db.mysql_connector import MySQLConnector
from backup_pilot.db.postgres_connector import PostgresConnector
from backup_pilot.db.sqlite_connector import SQLiteConnector
from backup_pilot.db.strategies.differential_backup import DifferentialBackupStrategy
from backup_pilot.db.strategies.full_backup import FullBackupStrategy
from backup_pilot.db.strategies.incremental_backup import IncrementalBackupStrategy
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

    A job_id is required so that full/incremental/differential runs can
    share state via the metadata store. The metadata directory defaults
    to a `.backup_pilot` folder next to the current working directory.
    """
    root = Path(metadata_dir) if metadata_dir else Path(".backup_pilot")
    store = BackupMetadataStore(root)

    if backup_type == BackupType.FULL:
        return FullBackupStrategy(metadata_store=store, job_id=job_id)
    if backup_type == BackupType.INCREMENTAL:
        return IncrementalBackupStrategy(store, job_id)
    if backup_type == BackupType.DIFFERENTIAL:
        return DifferentialBackupStrategy(store, job_id)
    raise ValueError(f"Unsupported backup type: {backup_type}")
