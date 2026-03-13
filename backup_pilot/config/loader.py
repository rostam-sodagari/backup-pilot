from __future__ import annotations

from pathlib import Path
from typing import Optional
import os

import yaml
from dotenv import load_dotenv

from backup_pilot.config.models import AppConfig


def _expand_env_vars(value):
    """
    Recursively expand environment variables in a nested structure.
    Strings like "${VAR}" or "$VAR" will be replaced if present in the environment.
    """
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def load_config(path: Optional[str]) -> AppConfig:
    # Ensure .env (if present) is loaded so that ${VAR} placeholders
    # in the YAML can be expanded via os.path.expandvars even when
    # load_config is called outside the CLI entrypoint.
    load_dotenv(override=False)

    config_path = Path(path or "backup_pilot.yaml")
    if not config_path.exists():  # pragma: no cover - trivial
        raise FileNotFoundError(f"Config file not found: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw = _expand_env_vars(raw)
    return AppConfig.model_validate(raw)
