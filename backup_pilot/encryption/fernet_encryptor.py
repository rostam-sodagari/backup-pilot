from __future__ import annotations

from io import BytesIO, BufferedReader
from typing import BinaryIO

from cryptography.fernet import Fernet

from backup_pilot.encryption.base import EncryptorBase


class FernetEncryptor(EncryptorBase):
    """
    Symmetric authenticated encryption using Fernet (AES-128-CBC + HMAC).
    Reads the full stream into memory, encrypts, and returns a BytesIO.
    """

    def __init__(self, key: str | bytes) -> None:
        if isinstance(key, str):
            key = key.encode("ascii")
        self._fernet = Fernet(key)

    def encrypt(self, stream: BinaryIO) -> BinaryIO:
        data = stream.read()
        encrypted = self._fernet.encrypt(data)
        buffer = BytesIO(encrypted)
        buffer.seek(0)
        return BufferedReader(buffer)

    def decrypt(self, encrypted_stream: BinaryIO) -> BinaryIO:
        data = encrypted_stream.read()
        decrypted = self._fernet.decrypt(data)
        buffer = BytesIO(decrypted)
        buffer.seek(0)
        return BufferedReader(buffer)
