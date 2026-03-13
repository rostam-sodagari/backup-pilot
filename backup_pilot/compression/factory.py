from __future__ import annotations

from backup_pilot.compression.gzip_compressor import GzipCompressor
from backup_pilot.compression.none_compressor import NoOpCompressor


def create_compressor(name: str):
    if name == "gzip":
        return GzipCompressor()
    if name == "none":
        return NoOpCompressor()
    raise ValueError(f"Unsupported compression: {name}")

