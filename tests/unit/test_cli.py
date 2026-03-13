from __future__ import annotations

from datetime import datetime
from pathlib import Path
import importlib
import sys

from typer.testing import CliRunner

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backup_pilot.core.models import BackupRecord, DatabaseType  # noqa: E402

cli_main = importlib.import_module("backup_pilot.cli.main")
app = cli_main.app
_history_file_for = cli_main._history_file_for


runner = CliRunner()


def test_history_file_for_config_path(tmp_path: Path) -> None:
    cfg = tmp_path / "backup_pilot.yaml"
    cfg.write_text("{}", encoding="utf-8")

    history_path = _history_file_for(str(cfg))

    assert history_path.name == "backup_pilot.history.jsonl"
    assert history_path.parent == cfg.parent


def test_list_backups_no_history(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "backup_pilot.yaml"
    cfg.write_text("{}", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "list-backups",
            "--config-file",
            str(cfg),
        ],
    )

    assert result.exit_code == 0
    assert "No backups have been recorded yet" in result.stdout


def test_list_backups_reads_history(tmp_path: Path) -> None:
    cfg = tmp_path / "backup_pilot.yaml"
    cfg.write_text("{}", encoding="utf-8")
    history_path = _history_file_for(str(cfg))

    record = BackupRecord(
        backup_id="20260312210710",
        profile_name="daily_mysql_full",
        db_type=DatabaseType.MYSQL,
        database_name="app_db",
        storage_location=str(tmp_path / "backups" / "20260312210710.bak"),
        created_at=datetime(2026, 3, 12),
        finished_at=None,
        size_bytes=1234,
    )
    history_path.write_text(record.model_dump_json() + "\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "list-backups",
            "--config-file",
            str(cfg),
        ],
    )

    assert result.exit_code == 0
    assert "Recorded backups:" in result.stdout
    assert "daily_mysql_full" in result.stdout
    assert "20260312210710" in result.stdout
