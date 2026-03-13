from __future__ import annotations

from abc import ABC, abstractmethod
from typing import BinaryIO


class EncryptorBase(ABC):
    """
    Base encryptor abstraction for backup content at rest.
    """

    @abstractmethod
    def encrypt(self, stream: BinaryIO) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def decrypt(self, encrypted_stream: BinaryIO) -> BinaryIO:
        raise NotImplementedError
