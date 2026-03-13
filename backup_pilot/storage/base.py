from __future__ import annotations

from abc import ABC, abstractmethod
from typing import BinaryIO


class StorageBackendBase(ABC):
    """
    Base class helpers for storage backends.
    """

    @abstractmethod
    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        raise NotImplementedError

    @abstractmethod
    def download(self, backup_id: str) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def delete(self, backup_id: str) -> None:
        raise NotImplementedError
