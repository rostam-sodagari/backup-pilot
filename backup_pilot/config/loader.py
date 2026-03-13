from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

from backup_pilot.config.models import AppConfig


def load_config(path: Optional[str]) -> AppConfig:
    config_path = Path(path or "backup_pilot.yaml")
    if not config_path.exists():  # pragma: no cover - trivial
        raise FileNotFoundError(f"Config file not found: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return AppConfig.model_validate(raw)
