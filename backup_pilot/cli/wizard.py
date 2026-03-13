from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
import yaml

from backup_pilot.config.loader import load_config

wizard_app = typer.Typer(help="Interactive wizard for configuring and running backups.")


@wizard_app.command("run")
def run_wizard(
    config_file: str = typer.Option(
        "backup_pilot.yaml",
        "--config-file",
        "-c",
        help="Path to configuration file to create or update.",
    ),
    execute: bool = typer.Option(
        True,
        "--execute/--no-execute",
        help="Run the backup immediately after saving configuration.",
    ),
) -> None:
    """
    Launch an interactive wizard to guide backup configuration.

    This helper focuses on a single backup profile at a time and can
    either apply the new configuration and run it, or just save the
    updated file.
    """
    typer.echo("Welcome to BackupPilot interactive wizard.")

    # Basic connection details.
    db_type = typer.prompt(
        "Database type (mysql/postgresql/mongodb/sqlite)",
        default="mysql",
    ).strip()
    db_name = typer.prompt("Database profile name", default="local_db").strip()
    host = typer.prompt("Database host", default="localhost").strip()
    port = typer.prompt("Database port", default="3306").strip()
    username = typer.prompt("Database username", default="root").strip()
    password = typer.prompt("Database password (leave blank to use env/.env)", default="", hide_input=True)
    database = typer.prompt("Database name (or leave blank for URI-only)", default="").strip()

    storage_name = typer.prompt("Storage profile name", default="local_fs").strip()
    storage_root = typer.prompt("Local backup directory", default="./backups").strip()

    backup_profile_name = typer.prompt(
        "Backup profile name", default="daily_backup"
    ).strip()
    backup_type = typer.prompt(
        "Backup type (full/incremental/differential)", default="full"
    ).strip()

    cfg_path = Path(config_file)
    if cfg_path.exists():
        cfg = load_config(str(cfg_path))
        raw = cfg.model_dump(mode="json")
    else:
        raw = {
            "databases": {},
            "storage": {},
            "backups": {},
        }

    dbs = raw.setdefault("databases", {})
    st = raw.setdefault("storage", {})
    bks = raw.setdefault("backups", {})

    dbs[db_name] = {
        "type": db_type,
        "host": host,
        "port": int(port),
        "username": username,
        "password": password or "${%s_PASSWORD}" % db_name.upper(),
        "database": database,
    }
    st[storage_name] = {
        "type": "local",
        "options": {
            "root_dir": storage_root,
        },
    }
    bks[backup_profile_name] = {
        "database": db_name,
        "storage": storage_name,
        "backup_type": backup_type,
        "compression": "gzip",
        "encryption": "none",
    }

    cfg_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    typer.echo(f"Configuration saved to {cfg_path}")

    if not execute:
        typer.echo("Skipping execution as requested.")
        return

    from backup_pilot.cli.main import backup as backup_command

    typer.echo("Running backup using the new profile...")
    backup_command(
        profile=backup_profile_name,
        config_file=str(cfg_path),
    )

