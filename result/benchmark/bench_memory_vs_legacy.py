#!/usr/bin/env python3
"""Memory-efficiency comparison: ``arkit`` streaming I/O vs. legacy full-buffer patterns.

Measures each workload in an isolated function scope with ``gc.collect()`` and a
fresh ``tracemalloc.start()`` / ``tracemalloc.stop()`` window so legacy allocations
never contaminate subsequent ``arkit`` measurements.
"""

from __future__ import annotations

import argparse
import gc
import io
import tarfile
import tempfile
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import brotli
import cramjam

from arkit import compress, convert_file, split_file


@dataclass(frozen=True)
class MemoryComparison:
    operation: str
    payload_mib: float
    legacy_peak_mib: float
    arkit_peak_mib: float

    @property
    def reduction_factor(self) -> float:
        return self.legacy_peak_mib / max(self.arkit_peak_mib, 0.01)


def _measure_peak_mib(fn: Callable[[], None]) -> float:
    gc.collect()
    tracemalloc.start()
    try:
        fn()
        _, peak = tracemalloc.get_traced_memory()
        return peak / (1024 * 1024)
    finally:
        tracemalloc.stop()
        gc.collect()


def _write_payload(path: Path, size_mib: int) -> int:
    block = (b"memory-comparison-record-0123456789abcdef\n" * 1500)[:65536]
    total = size_mib * 1024 * 1024
    written = 0
    with path.open("wb") as fh:
        while written < total:
            step = block[: min(len(block), total - written)]
            fh.write(step)
            written += len(step)
    return written


def run_memory_comparison(size_mib: int = 24) -> list[MemoryComparison]:
    results: list[MemoryComparison] = []
    with tempfile.TemporaryDirectory(prefix="arkit_mem_cmp_") as tmp:
        ws = Path(tmp)
        src_file = ws / "payload.log"
        total_bytes = _write_payload(src_file, size_mib)
        payload_mib = total_bytes / (1024 * 1024)

        # Warm up lazy codec imports before measuring heap allocations
        compress(b"warmup", "br")
        compress(b"warmup", "snappy")
        compress(b"warmup", "zst")

        # 1. Brotli compression: legacy full-buffer vs arkit streaming
        leg_br = _measure_peak_mib(
            lambda: (ws / "legacy.br").write_bytes(brotli.compress(src_file.read_bytes(), quality=4))
        )
        ark_br = _measure_peak_mib(
            lambda: compress(src_file, ws / "arkit.br", format="br", level=4)  # type: ignore[arg-type]
        )
        results.append(
            MemoryComparison("Brotli single-file compress (`.br`)", payload_mib, leg_br, ark_br)
        )

        # 2. Compound .tar.snappy: legacy BytesIO tar + full-buffer Snappy vs arkit framed streaming
        def _legacy_tar_snappy() -> None:
            tar_mem = io.BytesIO()
            with tarfile.open(fileobj=tar_mem, mode="w") as tf:
                tf.add(src_file, arcname="payload.log")
            sn_bytes = bytes(cramjam.snappy.compress(tar_mem.getvalue()))
            (ws / "legacy.tar.snappy").write_bytes(sn_bytes)

        leg_sn = _measure_peak_mib(_legacy_tar_snappy)
        ark_sn = _measure_peak_mib(
            lambda: compress(src_file, ws / "arkit.tar.snappy", format="tar.snappy")  # type: ignore[arg-type]
        )
        results.append(
            MemoryComparison("Compound `.tar.snappy` archive creation", payload_mib, leg_sn, ark_sn)
        )

        # 3. Cross-format conversion (.tar.gz -> .tar.zst): legacy read_members() dict vs arkit byte-pipe
        src_tgz = ws / "input.tar.gz"
        compress(src_file, src_tgz, format="tar.gz")

        def _legacy_convert() -> None:
            members_in_ram: dict[str, bytes] = {}
            with tarfile.open(src_tgz, "r:gz") as tf_in:
                for ti in tf_in.getmembers():
                    ef = tf_in.extractfile(ti)
                    if ef is not None:
                        members_in_ram[ti.name] = ef.read()
            tar_buf = io.BytesIO()
            with tarfile.open(fileobj=tar_buf, mode="w") as tf_out:
                for k, v in members_in_ram.items():
                    info_obj = tarfile.TarInfo(k)
                    info_obj.size = len(v)
                    tf_out.addfile(info_obj, io.BytesIO(v))

        leg_conv = _measure_peak_mib(_legacy_convert)
        ark_conv = _measure_peak_mib(
            lambda: convert_file(src_tgz, ws / "arkit.tar.zst")  # type: ignore[arg-type]
        )
        results.append(
            MemoryComparison("Cross-format `.tar.gz` -> `.tar.zst` conversion", payload_mib, leg_conv, ark_conv)
        )

        # 4. File split into large parts: legacy fh.read(part_size) vs arkit 1 MiB chunked split_file
        part_sz = (size_mib // 2) * 1024 * 1024

        def _legacy_split() -> None:
            with src_file.open("rb") as fh:
                idx = 1
                while blk := fh.read(part_sz):
                    (ws / f"leg.part{idx:03d}").write_bytes(blk)
                    idx += 1

        leg_split = _measure_peak_mib(_legacy_split)
        ark_split = _measure_peak_mib(
            lambda: split_file(src_file, part_size=part_sz, out_dir=ws / "arkit_parts")  # type: ignore[arg-type]
        )
        results.append(
            MemoryComparison(
                f"Splitting {size_mib} MiB file into {size_mib // 2} MiB parts",
                payload_mib,
                leg_split,
                ark_split,
            )
        )

    return results


def format_markdown(rows: list[MemoryComparison]) -> str:
    lines = [
        "| Operation | Payload Size (MiB) | Legacy Peak Heap (MiB) | `arkit` Peak Heap (MiB) | Heap Reduction |",
        "| :--- | ---: | ---: | ---: | ---: |",
    ]
    for r in rows:
        lines.append(
            f"| {r.operation} | {r.payload_mib:.1f} | {r.legacy_peak_mib:.2f} | "
            f"{r.arkit_peak_mib:.2f} | **{r.reduction_factor:.1f}x lower** |"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare legacy full-buffer vs arkit streaming peak RAM.")
    ap.add_argument("--size-mib", type=int, default=24, help="Test payload size in MiB (default: 24)")
    args = ap.parse_args()
    print(format_markdown(run_memory_comparison(args.size_mib)))


if __name__ == "__main__":
    main()
