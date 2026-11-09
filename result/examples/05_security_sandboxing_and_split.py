#!/usr/bin/env python3
"""Example 05: Security Sandboxing, Deterministic Builds, Parallel Brotli, Split/Merge & Atomic I/O."""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

from arkit import (
    ArchiveBombError,
    ParallelBrotliCompressor,
    SafetyPolicy,
    UnsafeArchivePathError,
    atomic_open,
    atomic_write,
    compress,
    decompress,
    extract,
    merge_files,
    split_file,
)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="arkit_ex05_") as tmp:
        ws = Path(tmp)

        # 1. Zip Slip & Decompression Bomb protection
        evil_zip = ws / "evil.zip"
        with zipfile.ZipFile(evil_zip, "w") as zf:
            zf.writestr("../../etc/cron.d/pwn", b"* * * * * root echo pwned\n")

        try:
            extract(evil_zip, ws / "sandbox")
        except UnsafeArchivePathError as exc:
            print(f"Blocked Zip Slip attack: {exc}")

        zeros = ws / "zeros.bin"
        zeros.write_bytes(b"\x00" * (2 * 1024 * 1024))
        bomb_zst = ws / "bomb.zst"
        compress(zeros, bomb_zst)
        try:
            decompress(bomb_zst, ws / "sandbox", policy=SafetyPolicy(max_ratio=25.0, ratio_min_bytes=65536))
        except ArchiveBombError as exc:
            print(f"Blocked decompression bomb mid-stream: {exc}")

        # 2. Deterministic byte-identical reproducible archive builds
        src_dir = ws / "pkg"
        src_dir.mkdir()
        (src_dir / "module.py").write_text("X = 42\n", encoding="utf-8")
        b1 = ws / "repro_1.tar.zst"
        b2 = ws / "repro_2.tar.zst"
        s1 = compress(src_dir, b1, deterministic=True, compute_sha256=True)
        s2 = compress(src_dir, b2, deterministic=True, compute_sha256=True)
        assert s1.sha256 == s2.sha256
        print(f"Deterministic SHA-256 match: {s1.sha256}")

        # 3. ParallelBrotliCompressor (PBRT0001) + Split/Merge with SHA-256 manifest verification
        pbrt_file = ws / "zeros.pbrt"
        pbc = ParallelBrotliCompressor(quality=4, chunk_size=256 * 1024, workers=2)
        pbc.compress_file(zeros, pbrt_file)

        parts = split_file(pbrt_file, part_size=256, out_dir=ws / "parts", manifest=True)
        merged_pbrt = ws / "merged.pbrt"
        merge_files(parts, merged_pbrt, sort=True, verify_manifest=True)
        restored_zeros = ws / "zeros.restored"
        ParallelBrotliCompressor.decompress_file(merged_pbrt, restored_zeros, workers=2)
        assert restored_zeros.stat().st_size == zeros.stat().st_size
        print(f"PBRT split ({len(parts)} parts) + SHA-256 verified merge + parallel restore OK!")

        # 4. Crash-safe atomic_write & streaming atomic_open
        state_json = ws / "state.json"
        atomic_write(state_json, {"status": "committed", "parts": len(parts)}, sort_keys=True)
        with atomic_open(ws / "streamed.txt", "w", encoding="utf-8") as fh:
            fh.write("Crash-safe streamed write\n")
        print("Atomic state:", state_json.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
