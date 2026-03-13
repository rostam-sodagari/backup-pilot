from __future__ import annotations

from io import BufferedReader
from typing import BinaryIO

from backup_pilot.encryption.base import EncryptorBase


class NoOpEncryptor(EncryptorBase):
    """
    No-op encryptor used when encryption is disabled.
    """

    def encrypt(self, stream: BinaryIO) -> BinaryIO:
        return BufferedReader(stream)

    def decrypt(self, encrypted_stream: BinaryIO) -> BinaryIO:
        return BufferedReader(encrypted_stream)
