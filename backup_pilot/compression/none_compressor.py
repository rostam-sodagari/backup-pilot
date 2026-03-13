from __future__ import annotations

from io import BufferedReader
from typing import BinaryIO

from backup_pilot.compression.base import CompressorBase


class NoOpCompressor(CompressorBase):
    """
    No-op compressor used for testing or when compression is disabled.
    """

    def compress(self, raw_stream: BinaryIO) -> BinaryIO:
        return BufferedReader(raw_stream)

    def decompress(self, compressed_stream: BinaryIO) -> BinaryIO:
        return BufferedReader(compressed_stream)

