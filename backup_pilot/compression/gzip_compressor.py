from __future__ import annotations

import gzip
from io import BufferedReader, BytesIO
from typing import BinaryIO

from backup_pilot.compression.base import CompressorBase


class GzipCompressor(CompressorBase):
    """
    Gzip compressor using streaming APIs where possible.
    """

    def compress(self, raw_stream: BinaryIO) -> BinaryIO:
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            for chunk in iter(lambda: raw_stream.read(8192), b""):
                if not chunk:
                    break
                gz.write(chunk)
        buffer.seek(0)
        return BufferedReader(buffer)

    def decompress(self, compressed_stream: BinaryIO) -> BinaryIO:
        gz = gzip.GzipFile(fileobj=compressed_stream, mode="rb")
        return BufferedReader(gz)

