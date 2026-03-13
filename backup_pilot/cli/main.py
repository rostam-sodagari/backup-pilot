from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from backup_pilot.config.loader import load_config
from backup_pilot.core.models import (
    BackupRecord,
    BackupRequest,
    RestoreRequest,
)
from backup_pilot.db.base import DBConnectionParams
from backup_pilot.db.factory import create_connector, create_strategy
from backup_pilot.logging.logger import configure_logger_from_config, get_logger
from backup_pilot.notifications.factory import create_notifiers
from backup_pilot.services.backup_service import BackupService
from backup_pilot.services.connection_service import ConnectionService
from backup_pilot.services.restore_service import RestoreService
from backup_pilot.services.rotation_service import run_rotation
from backup_pilot.cli.wizard import wizard_app
from backup_pilot.storage.factory import create_storage_backend
from backup_pilot.compression.factory import create_compressor
from backup_pilot.encryption.factory import create_encryptor

app = typer.Typer(help="BackupPilot - database backup and restore CLI.")
app.add_typer(wizard_app, name="wizard")


def _load_environment() -> None:
    """
    Load environment variables from a .env file in the current working directory.

    OS-level environment variables always take precedence over values from .env.
    """
    load_dotenv(override=False)


def _history_file_for(config_file: str) -> Path:
    """
    Derive a history file path from the config file path.

    This keeps backup history local to each configuration file and
    independent of the storage backend implementation.
    """
    cfg_path = Path(config_file)
    # backup_pilot.yaml -> backup_pilot.history.jsonl (or similar)
    return cfg_path.with_suffix(".history.jsonl")


def _append_backup_history(
    *,
    config_file: str,
    profile: str,
    db_profile,
    result,
) -> None:
    """
    Append a single backup record to the history file.
    """
    history_path = _history_file_for(config_file)

    database_name: Optional[str] = None
    if getattr(db_profile, "database", None):
        database_name = db_profile.database
    elif getattr(db_profile, "path", None):
        database_name = db_profile.path
    elif getattr(db_profile, "uri", None):
        database_name = db_profile.uri

    size_bytes: Optional[int] = None
    location = result.storage_location
    if location and not any(
        location.startswith(prefix) for prefix in ("s3://", "gs://", "azure://")
    ):
        # Best-effort size detection for local filesystem paths.
        if os.path.exists(location):
            try:
                size_bytes = os.path.getsize(location)
            except OSError:
                size_bytes = None

    record = BackupRecord(
        backup_id=result.backup_id,
        profile_name=profile,
        db_type=db_profile.type,
        database_name=database_name,
        storage_location=location,
        created_at=result.started_at,
        finished_at=result.finished_at,
        size_bytes=size_bytes,
        backup_type=backup_profile.backup_type,
    )

    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as fh:
        fh.write(record.model_dump_json() + "\n")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show the BackupPilot version and exit.",
    ),
) -> None:
    """
    Entry point for the BackupPilot CLI.
    """
    from backup_pilot import __version__

    # Ensure .env (if present) is loaded before any subcommand runs.
    _load_environment()

    if version:
        typer.echo(f"backup-pilot {__version__}")
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@app.command("backup")
def backup(
    profile: str = typer.Option(
        ..., "--profile", "-p", help="Backup profile name from config."
    ),
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file.",
    ),
) -> None:
    """
    Run a database backup.
    """
    cfg = load_config(config_file)
    configure_logger_from_config(cfg.logging)
    logger = get_logger()

    backup_profile = cfg.backups[profile]
    db_profile = cfg.databases[backup_profile.database]
    storage_profile = cfg.storage[backup_profile.storage]

    job_id = f"{backup_profile.database}:{profile}"

    db_params = DBConnectionParams(
        db_type=db_profile.type,
        host=db_profile.host,
        port=db_profile.port,
        username=db_profile.username,
        password=db_profile.password,
        database=db_profile.database,
        uri=db_profile.uri,
        path=db_profile.path,
    )

    connector = create_connector(db_params)
    strategy = create_strategy(
        backup_profile.backup_type,
        job_id=job_id,
    )
    storage = create_storage_backend(
        {"type": storage_profile.type, **storage_profile.options}
    )
    compressor = create_compressor(backup_profile.compression)
    encryptor = create_encryptor(
        backup_profile.encryption,
        key=os.environ.get("BACKUP_PILOT_ENCRYPTION_KEY"),
    )

    notifiers = create_notifiers(
        cfg.notifications.model_dump() if cfg.notifications else None
    )
    notifier = notifiers[0] if notifiers else None

    service = BackupService(
        connector=connector,
        strategy=strategy,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
        profile_name=profile,
        db_profile_name=backup_profile.database,
        db_type=db_profile.type,
        storage_profile_name=backup_profile.storage,
        storage_type=storage_profile.type,
        backup_type=backup_profile.backup_type,
        encryption_mode=backup_profile.encryption,
    )

    request = BackupRequest(
        db_type=db_profile.type,
        backup_type=backup_profile.backup_type,
        profile_name=profile,
    )

    result = service.run_backup(request)
    # Enrich result with context for logging and notifications.
    result.db_profile_name = backup_profile.database
    result.db_type = db_profile.type
    result.storage_profile_name = backup_profile.storage
    result.storage_type = storage_profile.type
    result.encryption_mode = backup_profile.encryption
    _append_backup_history(
        config_file=config_file,
        profile=profile,
        db_profile=db_profile,
        result=result,
    )
    typer.echo(
        f"Backup completed with ID {result.backup_id} at {result.storage_location}"
    )


@app.command("restore")
def restore(
    profile: str = typer.Option(
        ..., "--profile", "-p", help="Backup profile name from config."
    ),
    backup_id: str = typer.Option(..., "--backup-id", help="Backup ID to restore."),
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file.",
    ),
) -> None:
    """
    Restore a database from a backup.
    """
    cfg = load_config(config_file)
    configure_logger_from_config(cfg.logging)
    logger = get_logger()

    backup_profile = cfg.backups[profile]
    db_profile = cfg.databases[backup_profile.database]
    storage_profile = cfg.storage[backup_profile.storage]

    db_params = DBConnectionParams(
        db_type=db_profile.type,
        host=db_profile.host,
        port=db_profile.port,
        username=db_profile.username,
        password=db_profile.password,
        database=db_profile.database,
        uri=db_profile.uri,
        path=db_profile.path,
    )

    connector = create_connector(db_params)
    storage = create_storage_backend(
        {"type": storage_profile.type, **storage_profile.options}
    )
    compressor = create_compressor(backup_profile.compression)
    encryptor = create_encryptor(
        backup_profile.encryption,
        key=os.environ.get("BACKUP_PILOT_ENCRYPTION_KEY"),
    )

    notifiers = create_notifiers(
        cfg.notifications.model_dump() if cfg.notifications else None
    )
    notifier = notifiers[0] if notifiers else None

    service = RestoreService(
        connector=connector,
        storage=storage,
        compressor=compressor,
        encryptor=encryptor,
        notifier=notifier,
        logger=logger,
    )

    request = RestoreRequest(
        db_type=db_profile.type,
        backup_id=backup_id,
        profile_name=profile,
    )

    result = service.run_restore(request)
    typer.echo(f"Restore completed with status {result.status.value}")


@app.command("test-connection")
def test_connection(
    db_profile: str = typer.Option(
        ..., "--db-profile", "-d", help="Database profile name from config."
    ),
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file.",
    ),
) -> None:
    """
    Test database connectivity with the provided parameters or profile.
    """
    cfg = load_config(config_file)
    configure_logger_from_config(cfg.logging)
    logger = get_logger()
    profile = cfg.databases[db_profile]

    db_params = DBConnectionParams(
        db_type=profile.type,
        host=profile.host,
        port=profile.port,
        username=profile.username,
        password=profile.password,
        database=profile.database,
        uri=profile.uri,
        path=profile.path,
    )
    connector = create_connector(db_params)
    service = ConnectionService(connector=connector, logger=logger)
    service.test_connection()
    typer.echo("Connection successful.")


@app.command("rotate")
def rotate(
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file.",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Run rotation only for this backup profile.",
    ),
) -> None:
    """
    Apply retention policy: delete backups beyond retention_count and/or older than retention_days.
    """
    cfg = load_config(config_file)
    configure_logger_from_config(cfg.logging)
    logger = get_logger()
    history_path = _history_file_for(config_file)
    removed = run_rotation(
        config=cfg,
        history_path=history_path,
        profile_filter=profile,
        logger=logger,
    )
    typer.echo(f"Rotation complete. Removed {removed} backup(s).")


@app.command("list-configs")
def list_configs(
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file.",
    ),
) -> None:
    """
    List configured database and storage profiles.
    """
    cfg = load_config(config_file)
    configure_logger_from_config(cfg.logging)
    typer.echo("Database profiles:")
    for name in cfg.databases:
        typer.echo(f"  - {name}")
    typer.echo("Storage profiles:")
    for name in cfg.storage:
        typer.echo(f"  - {name}")
    typer.echo("Backup profiles:")
    for name in cfg.backups:
        typer.echo(f"  - {name}")


@app.command("list-backups")
def list_backups(
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file whose backups to list.",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Filter backups by profile name.",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        "-n",
        help="Maximum number of most recent backups to show (0 for all).",
    ),
) -> None:
    """
    List previously recorded backups for a given configuration file.
    """
    try:
        cfg = load_config(config_file)
        configure_logger_from_config(cfg.logging)
    except Exception:
        pass  # use default logging when config is missing or invalid
    history_path = _history_file_for(config_file)
    if not history_path.exists():
        typer.echo("No backups have been recorded yet for this configuration file.")
        return

    records: list[BackupRecord] = []
    with history_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                record = BackupRecord.model_validate(data)
            except Exception:
                continue

            if profile and record.profile_name != profile:
                continue
            records.append(record)

    records.sort(key=lambda r: r.created_at, reverse=True)
    if limit > 0:
        records = records[:limit]

    if not records:
        typer.echo("No matching backups found.")
        return

    typer.echo("Recorded backups:")
    for r in records:
        size_str = f"{r.size_bytes} bytes" if r.size_bytes is not None else "unknown"
        typer.echo(
            f"- {r.backup_id} | profile={r.profile_name or '-'} | "
            f"db={r.database_name or '-'} ({r.db_type.value}) | "
            f"created_at={r.created_at.isoformat()} | size={size_str} | "
            f"location={r.storage_location or '-'}"
        )


if __name__ == "__main__":
    app()
