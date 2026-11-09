# `arkit` Benchmark Suite

This directory contains reproducible performance and memory-efficiency benchmarks for `arkit`.

## Contents

| File | Description |
| :--- | :--- |
| [`bench_codecs_and_containers.py`](./bench_codecs_and_containers.py) | Measures compression/decompression throughput (`MiB/s`), compression ratio, and `tracemalloc` peak Python heap memory across all 8 single-stream codecs (`gz`, `bz2`, `xz`, `zst`, `br`, `pbrt`, `lz4`, `snappy`) and 9 multi-file container formats (`tar`, `zip`, `tar.gz`, `tar.zst`, `tar.lz4`, `tar.snappy`, `tar.br`, `tar.xz`, `7z`). |
| [`bench_memory_vs_legacy.py`](./bench_memory_vs_legacy.py) | Directly compares legacy full-buffer (`read_bytes()`, `bytearray()`, `read_members()`) peak heap consumption against `arkit`'s bounded `O(chunk_size)` streaming architecture in isolated `gc.collect()` + `tracemalloc` scopes. |
| [`bench_parallel_brotli.py`](./bench_parallel_brotli.py) | Evaluates multi-worker throughput scaling and bounded in-flight memory (`<= 2 * workers * chunk_size`) of `ParallelBrotliCompressor` (`PBRT0001`). |
| [`results.md`](./results.md) | Empirical benchmark report generated from running all three suites. |

## Running the Benchmarks

From the `result/` project root (with `arkit[all]` installed):

```bash
# 1. Full codec & container throughput + heap benchmark
python benchmark/bench_codecs_and_containers.py --size-mib 8 --markdown

# 2. Legacy full-buffer vs. arkit streaming peak RAM comparison
python benchmark/bench_memory_vs_legacy.py --size-mib 24

# 3. Parallel Brotli multi-worker scaling benchmark
python benchmark/bench_parallel_brotli.py --size-mib 16 --quality 5
```
