#!/usr/bin/env python3
"""Codec & container throughput, compression ratio, and peak heap memory benchmark.

Measures wall-clock compression/decompression throughput (MiB/s), compressed size
ratio, and ``tracemalloc`` peak Python heap usage across ``arkit``'s single-stream
codecs and multi-file container formats.

Usage:
    python benchmark/bench_codecs_and_containers.py [--size-mib 8] [--markdown]
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path

from arkit import Format, compress, decompress


@dataclass(frozen=True)
class BenchRow:
    label: str
    kind: str
    input_bytes: int
    output_bytes: int
    ratio: float
    comp_mib_s: float
    decomp_mib_s: float
    peak_comp_kib: float
    peak_decomp_kib: float


def _build_corpus(root: Path, total_mib: int) -> tuple[Path, Path]:
    """Create both a single synthetic log/JSON file and a multi-file directory tree."""
    total_bytes = total_mib * 1024 * 1024
    line = (
        b'{"ts":"2026-11-09T12:00:00Z","service":"arkit","level":"INFO",'
        b'"msg":"streaming archive benchmark record","seq":1024}\n'
    )
    # Mix structured text (85%) with deterministic pseudo-random binary blocks (15%)
    rng_seed = bytes((i * 73 + 19) & 0xFF for i in range(8192))
    unit = (line * 480) + rng_seed  # ~64 KiB

    single_path = root / "corpus.jsonl"
    written = 0
    with single_path.open("wb") as fh:
        while written < total_bytes:
            chunk = unit[: min(len(unit), total_bytes - written)]
            fh.write(chunk)
            written += len(chunk)

    tree_dir = root / "corpus_tree"
    (tree_dir / "logs").mkdir(parents=True)
    (tree_dir / "assets").mkdir(parents=True)
    per_file = max(65536, total_bytes // 8)
    for idx in range(8):
        sub = "logs" if idx < 6 else "assets"
        ext = ".jsonl" if idx < 6 else ".bin"
        fpath = tree_dir / sub / f"part_{idx:02d}{ext}"
        fwritten = 0
        with fpath.open("wb") as fh:
            while fwritten < per_file:
                chunk = unit[: min(len(unit), per_file - fwritten)]
                fh.write(chunk)
                fwritten += len(chunk)

    return single_path, tree_dir


def run_suite(size_mib: int = 8) -> list[BenchRow]:
    rows: list[BenchRow] = []
    workers = min(4, os.cpu_count() or 2)

    stream_specs: list[tuple[str, Format, int | None]] = [
        ("gzip (lvl 6)", Format.GZ, 6),
        ("bzip2 (lvl 6)", Format.BZ2, 6),
        ("xz (lvl 3)", Format.XZ, 3),
        ("zstd (lvl 3, MT)", Format.ZST, 3),
        ("brotli (q 4)", Format.BR, 4),
        ("pbrt (q 4, MT)", Format.PBRT, 4),
        ("lz4 (lvl 0)", Format.LZ4, 0),
        ("snappy (framed)", Format.SNAPPY, None),
    ]

    container_specs: list[tuple[str, Format, int | None]] = [
        ("tar (uncompressed)", Format.TAR, None),
        ("zip (deflate 6)", Format.ZIP, 6),
        ("tar.gz (lvl 6)", Format.TAR_GZ, 6),
        ("tar.zst (lvl 3, MT)", Format.TAR_ZST, 3),
        ("tar.lz4 (lvl 0)", Format.TAR_LZ4, 0),
        ("tar.snappy (framed)", Format.TAR_SNAPPY, None),
        ("tar.br (q 4)", Format.TAR_BR, 4),
        ("tar.xz (lvl 3)", Format.TAR_XZ, 3),
        ("7z (lzma2)", Format.SEVENZ, 3),
    ]

    with tempfile.TemporaryDirectory(prefix="arkit_bench_") as tmp:
        ws = Path(tmp)
        single_file, tree_dir = _build_corpus(ws, size_mib)

        for label, fmt, lvl in stream_specs:
            out_arch = ws / f"bench_stream{fmt.ext}"
            out_dir = ws / f"ext_stream_{fmt.name}"

            tracemalloc.start()
            tracemalloc.reset_peak()
            t0 = time.perf_counter()
            stats = compress(single_file, out_arch, format=fmt, level=lvl, threads=workers)
            dt_comp = max(1e-9, time.perf_counter() - t0)
            _, peak_comp = tracemalloc.get_traced_memory()

            tracemalloc.reset_peak()
            t1 = time.perf_counter()
            decompress(out_arch, out_dir, format=fmt)
            dt_decomp = max(1e-9, time.perf_counter() - t1)
            _, peak_decomp = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            mib = stats.source_bytes / (1024 * 1024)
            rows.append(
                BenchRow(
                    label=label,
                    kind="stream",
                    input_bytes=stats.source_bytes,
                    output_bytes=stats.result_bytes,
                    ratio=stats.ratio,
                    comp_mib_s=mib / dt_comp,
                    decomp_mib_s=mib / dt_decomp,
                    peak_comp_kib=peak_comp / 1024.0,
                    peak_decomp_kib=peak_decomp / 1024.0,
                )
            )
            out_arch.unlink(missing_ok=True)

        for label, fmt, lvl in container_specs:
            out_arch = ws / f"bench_cont{fmt.ext}"
            out_dir = ws / f"ext_cont_{fmt.name}"

            tracemalloc.start()
            tracemalloc.reset_peak()
            t0 = time.perf_counter()
            stats = compress(tree_dir, out_arch, format=fmt, level=lvl, threads=workers)
            dt_comp = max(1e-9, time.perf_counter() - t0)
            _, peak_comp = tracemalloc.get_traced_memory()

            tracemalloc.reset_peak()
            t1 = time.perf_counter()
            decompress(out_arch, out_dir, format=fmt)
            dt_decomp = max(1e-9, time.perf_counter() - t1)
            _, peak_decomp = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            mib = stats.source_bytes / (1024 * 1024)
            rows.append(
                BenchRow(
                    label=label,
                    kind="container",
                    input_bytes=stats.source_bytes,
                    output_bytes=stats.result_bytes,
                    ratio=stats.ratio,
                    comp_mib_s=mib / dt_comp,
                    decomp_mib_s=mib / dt_decomp,
                    peak_comp_kib=peak_comp / 1024.0,
                    peak_decomp_kib=peak_decomp / 1024.0,
                )
            )
            out_arch.unlink(missing_ok=True)

    return rows


def format_markdown(rows: list[BenchRow]) -> str:
    lines = [
        "| Format / Codec | Type | Ratio | Compress (MiB/s) | Decompress (MiB/s) | Peak Heap Compress (KiB) | Peak Heap Decompress (KiB) |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in rows:
        lines.append(
            f"| `{r.label}` | {r.kind} | {r.ratio:.3f} | {r.comp_mib_s:,.1f} | "
            f"{r.decomp_mib_s:,.1f} | {r.peak_comp_kib:,.1f} | {r.peak_decomp_kib:,.1f} |"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run arkit codec and container benchmarks.")
    ap.add_argument("--size-mib", type=int, default=8, help="Payload size in MiB (default: 8)")
    ap.add_argument("--markdown", action="store_true", help="Emit Markdown table")
    args = ap.parse_args()
    rows = run_suite(args.size_mib)
    print(format_markdown(rows))


if __name__ == "__main__":
    main()
