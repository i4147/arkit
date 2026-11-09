#!/usr/bin/env python3
"""Example 03: In-Place Stream Processing of Archives & Concurrent Batch Extraction."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import IO

from arkit import ArchiveProcessor, batch_extract, pack_file, process_archive, read_members


def redact_tokens(data: bytes, filename: str) -> bytes:
    _ = filename
    return data.replace(b"SECRET_TOKEN_123", b"[REDACTED]")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="arkit_ex03_") as tmp:
        ws = Path(tmp)

        # 1. Transform matching members inside .tar.gz atomically in place
        logs_archive = ws / "service_logs.tar.gz"
        pack_file(
            logs_archive,
            {
                "2026-11-09/auth.log": b"user=alice token=SECRET_TOKEN_123\n",
                "2026-11-09/metrics.bin": b"\x00\x01\x02\x03",
            },
        )
        modified = process_archive(
            logs_archive,
            redact_tokens,
            lambda name: name.endswith(".log"),
        )
        print(f"Redacted {modified} log member(s): {read_members(logs_archive)['2026-11-09/auth.log']!r}")

        # 2. Constant-RAM streaming member transform via ArchiveProcessor.process_streams()
        zst_archive = ws / "numbered.tar.zst"
        pack_file(zst_archive, {"events.txt": b"alpha\nbeta\ngamma\n"})

        def add_line_numbers(reader: IO[bytes], writer: IO[bytes], _name: str) -> None:
            for lineno, line in enumerate(reader, start=1):
                writer.write(f"{lineno:04d}: ".encode() + line)

        ArchiveProcessor(zst_archive).process_streams(add_line_numbers)
        print("Stream-numbered member:\n" + read_members(zst_archive)["events.txt"].decode())

        # 3. Concurrent batch extraction with nested archive expansion
        incoming = ws / "incoming"
        (incoming / "shard_a").mkdir(parents=True)
        (incoming / "shard_b").mkdir(parents=True)
        inner_tar = ws / "tmp_inner.tar.zst"
        pack_file(inner_tar, {"nested_report.txt": b"Q4 summary OK\n"})
        pack_file(incoming / "shard_a" / "bundle.zip", {"inner.tar.zst": inner_tar.read_bytes()})
        pack_file(incoming / "shard_b" / "data.tar.lz4", {"status.txt": b"ready\n"})
        inner_tar.unlink()

        results = batch_extract(incoming, workers=2, extract_nested=True, remove_source=False)
        print("Batch extract results:", {p.name: ok for p, ok in results.items()})
        print("Nested extracted file:", (incoming / "shard_a" / "nested_report.txt").read_text().strip())


if __name__ == "__main__":
    main()
