from __future__ import annotations

from backup_pilot.core.exceptions import ConnectionError
from backup_pilot.core.interfaces import DatabaseConnector, LoggerLike


class ConnectionService:
    """
    Provides a simple façade for testing database connectivity.
    """

    def __init__(self, connector: DatabaseConnector, logger: LoggerLike) -> None:
        self._connector = connector
        self._logger = logger

    def test_connection(self) -> None:
        try:
            self._logger.info("Testing database connection")
            self._connector.test_connection()
            self._logger.info("Database connection successful")
        except Exception as exc:  # pragma: no cover - simple pass-through
            self._logger.exception("Database connection failed")
            raise ConnectionError("Database connection failed") from exc

