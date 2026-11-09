"""Quantitative memory-efficiency regression tests using ``tracemalloc``.

Verifies that streaming archive creation, validation, extraction, framed Snappy,
parallel Brotli, and file split/merge operate in strictly bounded Python heap
memory well below the payload size.
"""

from __future__ import annotations

import os
import tracemalloc
from pathlib import Path

from arkit import (
    ParallelBrotliCompressor,
    compress,
    decompress,
    merge_files,
    split_file,
    validate,
)


def _generate_large_file(path: Path, size_mib: int = 16) -> int:
    """Write ``size_mib`` MiB of mixed compressible + pseudo-random blocks without big allocations."""
    pattern = (b"arkit-memory-efficiency-benchmark-line-0123456789\n" * 1024)[:50_000]
    noise = os.urandom(15_536)
    block = pattern + noise  # ~64 KiB
    target_bytes = size_mib * 1024 * 1024
    written = 0
    with path.open("wb") as fh:
        while written < target_bytes:
            step = block[: min(len(block), target_bytes - written)]
            fh.write(step)
            written += len(step)
    return written


def test_streaming_tar_zst_and_snappy_bounded_heap_memory(tmp_path: Path) -> None:
    src_dir = tmp_path / "large_src"
    src_dir.mkdir()
    total_bytes = _generate_large_file(src_dir / "big1.bin", size_mib=8)
    total_bytes += _generate_large_file(src_dir / "big2.bin", size_mib=8)
    assert total_bytes == 16 * 1024 * 1024

    tar_zst = tmp_path / "streamed.tar.zst"
    tar_snappy = tmp_path / "streamed.tar.snappy"
    out_dir = tmp_path / "extracted"

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        compress(src_dir, tar_zst, level=3)
        _, peak_compress_zst = tracemalloc.get_traced_memory()

        tracemalloc.reset_peak()
        rep = validate(tar_zst)
        _, peak_validate_zst = tracemalloc.get_traced_memory()
        assert rep.healthy is True

        tracemalloc.reset_peak()
        decompress(tar_zst, out_dir)
        _, peak_extract_zst = tracemalloc.get_traced_memory()

        # Framed Snappy compound tar (previously buffered 100% of file in bytearray()!)
        tracemalloc.reset_peak()
        compress(src_dir, tar_snappy)
        rep_sn = validate(tar_snappy)
        _, peak_snappy = tracemalloc.get_traced_memory()
        assert rep_sn.healthy is True
    finally:
        tracemalloc.stop()

    # Payload is 16 MiB; peak Python heap must stay strictly < 6 MiB across every operation!
    limit = 6 * 1024 * 1024
    assert peak_compress_zst < limit, f"tar.zst compress peak too high: {peak_compress_zst:,} B"
    assert peak_validate_zst < limit, f"tar.zst validate peak too high: {peak_validate_zst:,} B"
    assert peak_extract_zst < limit, f"tar.zst extract peak too high: {peak_extract_zst:,} B"
    assert peak_snappy < limit, f"tar.snappy stream peak too high: {peak_snappy:,} B"


def test_parallel_brotli_and_split_bounded_memory(tmp_path: Path) -> None:
    big_in = tmp_path / "input_12m.bin"
    _generate_large_file(big_in, size_mib=12)
    pbrt_out = tmp_path / "input_12m.pbrt"

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        pbc = ParallelBrotliCompressor(quality=2, chunk_size=256 * 1024, workers=2)
        pbc.compress_file(big_in, pbrt_out)
        _, peak_pbrt = tracemalloc.get_traced_memory()

        tracemalloc.reset_peak()
        # Split 12 MiB file into 6 MiB parts — must stream in 1 MiB chunks, NOT allocate 6 MiB!
        parts = split_file(big_in, part_size=6 * 1024 * 1024, out_dir=tmp_path / "parts")
        merged = merge_files(parts, tmp_path / "merged_12m.bin")
        _, peak_split_merge = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert merged.stat().st_size == 12 * 1024 * 1024
    # Bounded window of 2*2 * 256 KiB = ~1 MiB in flight, well below 6 MiB!
    assert peak_pbrt < 6 * 1024 * 1024, f"PBRT peak too high: {peak_pbrt:,} B"
    assert peak_split_merge < 4 * 1024 * 1024, f"split/merge peak too high: {peak_split_merge:,} B"
