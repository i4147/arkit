#!/usr/bin/env python3
"""Parallel Brotli (``PBRT0001``) multi-worker scaling benchmark.

Measures compression and decompression throughput across worker counts using
``ParallelBrotliCompressor`` with bounded in-flight chunk windows.
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path

from arkit import ParallelBrotliCompressor


@dataclass(frozen=True)
class ScalingRow:
    workers: int
    backend: str
    comp_mib_s: float
    decomp_mib_s: float
    speedup: float
    peak_heap_mib: float


def run_scaling(size_mib: int = 16, quality: int = 5) -> list[ScalingRow]:
    max_cpus = os.cpu_count() or 2
    worker_counts = sorted({1, 2, min(4, max_cpus), max_cpus})
    rows: list[ScalingRow] = []

    with tempfile.TemporaryDirectory(prefix="arkit_pbrt_scale_") as tmp:
        ws = Path(tmp)
        src = ws / "input.jsonl"
        block = (b'{"event":"click","user":42,"path":"/docs/arkit","latency_ms":1.7}\n' * 980)[:65536]
        target = size_mib * 1024 * 1024
        written = 0
        with src.open("wb") as fh:
            while written < target:
                step = block[: min(len(block), target - written)]
                fh.write(step)
                written += len(step)

        baseline_comp_s: float | None = None
        for w in worker_counts:
            dst = ws / f"out_w{w}.pbrt"
            dec = ws / f"dec_w{w}.jsonl"
            pbc = ParallelBrotliCompressor(
                quality=quality,
                chunk_size=512 * 1024,
                workers=w,
                backend="thread",
            )
            tracemalloc.start()
            tracemalloc.reset_peak()
            t0 = time.perf_counter()
            pbc.compress_file(src, dst)
            dt_c = max(1e-9, time.perf_counter() - t0)
            _, peak_c = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            if baseline_comp_s is None:
                baseline_comp_s = dt_c

            t1 = time.perf_counter()
            ParallelBrotliCompressor.decompress_file(dst, dec, workers=w)
            dt_d = max(1e-9, time.perf_counter() - t1)

            mib = written / (1024 * 1024)
            rows.append(
                ScalingRow(
                    workers=w,
                    backend="thread",
                    comp_mib_s=mib / dt_c,
                    decomp_mib_s=mib / dt_d,
                    speedup=baseline_comp_s / dt_c,
                    peak_heap_mib=peak_c / (1024 * 1024),
                )
            )
            dst.unlink(missing_ok=True)
            dec.unlink(missing_ok=True)

    return rows


def format_markdown(rows: list[ScalingRow]) -> str:
    lines = [
        "| Workers | Backend | Compress (MiB/s) | Decompress (MiB/s) | Compress Speedup | Peak Heap (MiB) |",
        "| ---: | :--- | ---: | ---: | ---: | ---: |",
    ]
    for r in rows:
        lines.append(
            f"| {r.workers} | `{r.backend}` | {r.comp_mib_s:,.1f} | "
            f"{r.decomp_mib_s:,.1f} | {r.speedup:.2f}x | {r.peak_heap_mib:.2f} |"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark ParallelBrotliCompressor worker scaling.")
    ap.add_argument("--size-mib", type=int, default=16)
    ap.add_argument("--quality", type=int, default=5)
    args = ap.parse_args()
    print(format_markdown(run_scaling(args.size_mib, args.quality)))


if __name__ == "__main__":
    main()
