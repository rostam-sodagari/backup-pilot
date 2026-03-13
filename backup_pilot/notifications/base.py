from __future__ import annotations

from abc import ABC, abstractmethod

from backup_pilot.core.models import BackupResult, RestoreResult


class NotifierBase(ABC):
    @abstractmethod
    def notify_success(self, result: BackupResult | RestoreResult) -> None:
        raise NotImplementedError

    @abstractmethod
    def notify_failure(self, result: BackupResult | RestoreResult, error: Exception) -> None:
        raise NotImplementedError

