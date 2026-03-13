from __future__ import annotations

import logging
from typing import Optional


def configure_logger(
    name: str = "backup_pilot",
    level: int = logging.INFO,
    json: bool = False,
    file_path: Optional[str] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    if json:
        import json as _json

        class JsonFormatter(logging.Formatter):
            def format(
                self, record: logging.LogRecord
            ) -> str:  # pragma: no cover - formatting
                payload = {
                    "level": record.levelname,
                    "message": record.getMessage(),
                    "logger": record.name,
                }
                if hasattr(record, "extra"):
                    payload["extra"] = getattr(record, "extra")
                return _json.dumps(payload)

        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if file_path:
        try:
            file_handler = logging.FileHandler(file_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except OSError:  # pragma: no cover - filesystem specific
            logger.warning(
                "Could not open log file %s, logging to stream only", file_path
            )

    return logger


def configure_logger_from_config(logging_config: Optional[object]) -> logging.Logger:
    """Configure the default logger from AppConfig.logging. Clears existing handlers first."""
    from backup_pilot.config.models import LoggingConfig

    logger = logging.getLogger("backup_pilot")
    logger.handlers.clear()

    if isinstance(logging_config, LoggingConfig) and (
        logging_config.level
        or logging_config.file is not None
        or logging_config.json_format
    ):
        level_str = (logging_config.level or "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)
        return configure_logger(
            level=level,
            json=bool(logging_config.json_format),
            file_path=logging_config.file,
        )
    return configure_logger()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name or "backup_pilot")
