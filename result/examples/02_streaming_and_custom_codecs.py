#!/usr/bin/env python3
"""Example 02: Constant-RAM Streaming I/O, ZstdTarFile, and Custom Codec Plugins."""

from __future__ import annotations

import io
import tarfile
import tempfile
from pathlib import Path

from arkit import (
    ZstdTarFile,
    compress_stream,
    decompress_stream,
    open_stream_reader,
    open_stream_writer,
    register_codec,
    register_format,
)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="arkit_ex02_") as tmp:
        ws = Path(tmp)

        # 1. Stream-write and stream-read framed Snappy & Brotli in constant memory
        snappy_log = ws / "events.jsonl.sz"
        with open_stream_writer(snappy_log, "snappy") as writer:
            for i in range(1000):
                writer.write(f'{{"id": {i}, "msg": "streamed event"}}\n'.encode())

        line_count = 0
        with open_stream_reader(snappy_log, "snappy") as reader:
            for _line in io.TextIOWrapper(reader, encoding="utf-8"):
                line_count += 1
        print(f"Streamed {line_count} lines from {snappy_log.name} ({snappy_log.stat().st_size:,} B)")

        # 2. Direct ZstdTarFile usage (seekable TarFile subclass over Zstandard)
        zst_tar = ws / "artifacts.tar.zst"
        with ZstdTarFile(zst_tar, "w", level=4, threads=2) as ztf:
            for idx in range(3):
                payload = f"artifact-{idx}\n".encode() * 100
                ti = tarfile.TarInfo(f"build/artifact_{idx}.txt")
                ti.size = len(payload)
                ztf.addfile(ti, io.BytesIO(payload))

        with ZstdTarFile(zst_tar, "r") as ztf_in:
            print("ZstdTarFile members:", ztf_in.getnames())
            ef = ztf_in.extractfile("build/artifact_1.txt")
            assert ef is not None
            print("Extracted first line:", ef.readline().decode().strip())

        # 3. Register a custom codec & format at runtime
        class _ReverseBitsWriter(io.RawIOBase):
            def __init__(self, dst: io.BytesIO) -> None:
                self._dst = dst

            def writable(self) -> bool:
                return True

            def write(self, b: bytes) -> int:  # type: ignore[override]
                self._dst.write(bytes((x ^ 0xAA) for x in bytes(b)))
                return len(b)

        register_codec(
            "xoraa",
            reader_factory=lambda src: io.BytesIO(bytes(x ^ 0xAA for x in src.read())),
            writer_factory=lambda dst, _lvl: _ReverseBitsWriter(dst),  # type: ignore[arg-type,return-value]
        )
        register_format("xoraa", suffixes=(".xaa",), kind="stream", codec="xoraa")

        encoded = io.BytesIO()
        compress_stream(io.BytesIO(b"Custom plugin roundtrip!"), encoded, "xoraa")
        encoded.seek(0)
        decoded = io.BytesIO()
        decompress_stream(encoded, decoded, "xoraa")
        print("Plugin codec output:", decoded.getvalue().decode())


if __name__ == "__main__":
    main()
