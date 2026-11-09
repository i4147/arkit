"""Tests for ``ParallelBrotliCompressor`` / ``PBCompressor`` (``PBRT0001`` format)."""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Literal

import brotli
import pytest

from arkit import (
    CorruptedArchiveError,
    Format,
    ParallelBrotliCompressor,
    PBCompressor,
    decompress,
    open_compressed_reader,
    validate,
)


@pytest.mark.parametrize("backend", ["thread", "process"])
def test_parallel_brotli_file_and_bytes_roundtrip(
    tmp_path: Path,
    backend: Literal["thread", "process"],
) -> None:
    assert PBCompressor is ParallelBrotliCompressor

    # Create ~495 KiB multi-chunk payload with 64 KiB chunk size -> 8 chunks
    payload = (b"ParallelBrotli-chunk-test-0123456789\n" * 12000) + bytes(range(256)) * 200
    src_file = tmp_path / "big.json"
    pbrt_file = tmp_path / "big.json.pbrt"
    out_file = tmp_path / "big.json.restored"
    src_file.write_bytes(payload)

    progress_calls: list[tuple[int, int | None]] = []
    compressor = PBCompressor(
        quality=4,
        chunk_size=64 * 1024,
        workers=2,
        mode=brotli.MODE_TEXT,
        progress_callback=lambda done, total: progress_calls.append((done, total)),
        backend=backend,
    )
    compressor.compress_file(src_file, pbrt_file)
    assert len(progress_calls) >= 7
    assert progress_calls[-1] == (len(payload), len(payload))

    # Verify PBRT0001 magic + validate() + streaming reader + parallel decompress_file!
    raw_pbrt = pbrt_file.read_bytes()
    assert raw_pbrt.startswith(b"PBRT0001")
    rep = validate(pbrt_file)
    assert rep.healthy is True
    assert rep.format == "pbrt"
    assert rep.checked_bytes == len(payload)

    with open_compressed_reader(pbrt_file, "pbrt") as stream:
        assert stream.read(100) + stream.read() == payload

    PBCompressor.decompress_file(pbrt_file, out_file, workers=2)
    assert out_file.read_bytes() == payload

    # Universal decompress() integration
    ext_dir = tmp_path / f"univ_{backend}"
    restored = decompress(pbrt_file, ext_dir, format=Format.PBRT)
    assert restored.read_bytes() == payload

    # Zero-temp-file in-memory bytes roundtrip
    mem_blob = compressor.compress_bytes(payload[:150_000])
    assert PBCompressor.decompress_bytes(mem_blob, workers=2) == payload[:150_000]


def test_parallel_brotli_validation_and_corrupt_detection(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="quality"):
        PBCompressor(quality=15)
    with pytest.raises(ValueError, match="chunk_size"):
        PBCompressor(chunk_size=1024)

    bad_magic = tmp_path / "bad.pbrt"
    bad_magic.write_bytes(b"NOT_PBRT0000")
    with pytest.raises(ValueError, match="PBRT0001"):
        PBCompressor.decompress_file(bad_magic, tmp_path / "out.bin")

    # Craft chunk with mismatched original length header
    comp_chunk = brotli.compress(b"hello world", quality=3)
    forged = (
        b"PBRT0001"
        + struct.pack("<I", 1)
        + struct.pack("<II", len(comp_chunk), 999)  # wrong orig_len!
        + comp_chunk
    )
    with pytest.raises(CorruptedArchiveError, match="mismatch"):
        PBCompressor.decompress_bytes(forged)
