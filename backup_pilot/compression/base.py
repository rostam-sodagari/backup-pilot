from __future__ import annotations

from abc import ABC, abstractmethod
from typing import BinaryIO


class CompressorBase(ABC):
    """
    Base compressor abstraction.
    """

    @abstractmethod
    def compress(self, raw_stream: BinaryIO) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def decompress(self, compressed_stream: BinaryIO) -> BinaryIO:
        raise NotImplementedError
