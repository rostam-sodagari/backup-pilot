from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import BinaryIO, Protocol

from .models import BackupRequest, BackupResult, BackupType, RestoreRequest, RestoreResult


class DatabaseConnector(ABC):
    """
    Abstraction over a concrete database engine.
    """

    @abstractmethod
    def test_connection(self) -> None:
        """
        Validate connectivity and credentials.
        """

    @abstractmethod
    def create_backup_stream(self, request: BackupRequest) -> BinaryIO:
        """
        Produce a readable binary stream containing a logical backup of the database.
        """

    @abstractmethod
    def restore_from_stream(self, request: RestoreRequest, stream: BinaryIO) -> None:
        """
        Restore the database from the given stream.
        """


class BackupStrategy(ABC):
    """
    Encapsulates full backup behavior. Incremental and differential
    support are planned for a future release.
    """

    @abstractmethod
    def run(self, connector: DatabaseConnector, request: BackupRequest) -> BackupResult:
        """
        Execute a backup using the provided connector.
        """

    def record_success(
        self, backup_id: str, backup_type: BackupType, created_at: datetime
    ) -> None:
        """
        Called by the service after a backup is successfully uploaded.
        Override to persist metadata (e.g. for future incremental/differential).
        Default is no-op.
        """
        pass


class StorageBackend(ABC):
    """
    Abstract destination for backup artifacts (local or cloud).
    """

    @abstractmethod
    def upload(self, backup_id: str, stream: BinaryIO) -> str:
        """
        Store the backup stream and return a storage location/URI.
        """

    @abstractmethod
    def download(self, backup_id: str) -> BinaryIO:
        """
        Retrieve a readable binary stream for the given backup ID.
        """

    @abstractmethod
    def delete(self, backup_id: str) -> None:
        """
        Remove the backup artifact for the given backup ID.
        """


class Compressor(ABC):
    """
    Compression abstraction for streaming backup content.
    """

    @abstractmethod
    def compress(self, raw_stream: BinaryIO) -> BinaryIO:
        """
        Return a readable stream that yields compressed bytes.
        """

    @abstractmethod
    def decompress(self, compressed_stream: BinaryIO) -> BinaryIO:
        """
        Return a readable stream that yields decompressed bytes.
        """


class Encryptor(ABC):
    """
    Encryption abstraction for backup content at rest.
    """

    @abstractmethod
    def encrypt(self, stream: BinaryIO) -> BinaryIO:
        """
        Return a readable stream that yields encrypted bytes.
        """

    @abstractmethod
    def decrypt(self, encrypted_stream: BinaryIO) -> BinaryIO:
        """
        Return a readable stream that yields decrypted bytes.
        """


class Notifier(ABC):
    """
    Notification abstraction (Slack, email, etc.).
    """

    @abstractmethod
    def notify_success(self, result: BackupResult | RestoreResult) -> None:
        """
        Send a success notification for a completed operation.
        """

    @abstractmethod
    def notify_failure(
        self, result: BackupResult | RestoreResult, error: Exception
    ) -> None:
        """
        Send a failure notification for an operation.
        """


class LoggerLike(Protocol):
    """
    Minimal logger protocol used across services.
    """

    def info(self, msg: str, *args, **kwargs) -> None: ...

    def warning(self, msg: str, *args, **kwargs) -> None: ...

    def error(self, msg: str, *args, **kwargs) -> None: ...

    def exception(self, msg: str, *args, **kwargs) -> None: ...
