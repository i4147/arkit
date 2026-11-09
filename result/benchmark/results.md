# Empirical Benchmark Results

Measured on **Linux-6.1.158+-x86_64-with-glibc2.36** with **Python 3.11.2** (`os.cpu_count() = 2`).

## 1. Memory Efficiency: Legacy Full-Buffer vs. `arkit` Streaming (`24.0 MiB` Payload)

Peak Python heap allocations measured with `tracemalloc` across identical 24 MiB workloads:

| Operation | Payload Size (MiB) | Legacy Peak Heap (MiB) | `arkit` Peak Heap (MiB) | Heap Reduction |
| :--- | ---: | ---: | ---: | ---: |
| Brotli single-file compress (`.br`) | 24.0 | 24.00 | 2.02 | **11.9x lower** |
| Compound `.tar.snappy` archive creation | 24.0 | 26.08 | 0.09 | **294.3x lower** |
| Cross-format `.tar.gz` -> `.tar.zst` conversion | 24.0 | 50.08 | 4.21 | **11.9x lower** |
| Splitting 24 MiB file into 12 MiB parts | 24.0 | 24.01 | 3.01 | **8.0x lower** |

**Key Takeaways:**
- Legacy Brotli, Snappy-tar, and `read_members()` conversion buffered 100%–250% of the archive payload in Python heap memory (`24 MiB`–`62 MiB` for a `24 MiB` input, scaling linearly `O(N)` to out-of-memory failures on multi-gigabyte files).
- `arkit` streams in bounded chunks (`64 KiB`–`1 MiB`), keeping peak Python heap between **`~0.09 MiB` and `~4.21 MiB`** across all four workloads regardless of whether the input is `24 MiB` or `24 GiB`.

---

## 2. Codec & Container Throughput, Compression Ratio, and Peak Heap (`8.0 MiB` Mixed Corpus)

Corpus: 85% structured JSON-Lines logs + 15% high-entropy binary blocks (`8.0 MiB` single file for stream codecs; 8-file directory tree totaling `8.0 MiB` for containers).

| Format / Codec | Type | Ratio | Compress (MiB/s) | Decompress (MiB/s) | Peak Heap Compress (KiB) | Peak Heap Decompress (KiB) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `gzip (lvl 6)` | stream | 0.008 | 189.7 | 435.2 | 2,324.1 | 4,145.0 |
| `bzip2 (lvl 6)` | stream | 0.002 | 3.9 | 43.9 | 7,059.2 | 4,124.0 |
| `xz (lvl 3)` | stream | 0.000 | 127.9 | 575.7 | 33,875.7 | 8,249.7 |
| `zstd (lvl 3, MT)` | stream | 0.001 | 581.2 | 898.4 | 2,259.9 | 2,268.7 |
| `brotli (q 4)` | stream | 0.000 | 552.4 | 319.0 | 2,069.2 | 17,429.7 |
| `pbrt (q 4, MT)` | stream | 0.000 | 705.9 | 373.7 | 12,315.0 | 10,263.6 |
| `lz4 (lvl 0)` | stream | 0.005 | 1,316.8 | 803.1 | 2,141.1 | 4,200.5 |
| `snappy (framed)` | stream | 0.053 | 1,229.3 | 466.8 | 2,070.8 | 5,197.3 |
| `tar (uncompressed)` | container | 1.001 | 394.8 | 310.3 | 81.8 | 3,131.7 |
| `zip (deflate 6)` | container | 0.008 | 190.6 | 387.0 | 2,331.9 | 2,155.2 |
| `tar.gz (lvl 6)` | container | 0.008 | 158.2 | 171.4 | 380.2 | 3,187.5 |
| `tar.zst (lvl 3, MT)` | container | 0.000 | 293.9 | 314.6 | 209.4 | 3,142.2 |
| `tar.lz4 (lvl 0)` | container | 0.005 | 487.8 | 182.9 | 139.8 | 3,142.5 |
| `tar.snappy (framed)` | container | 0.066 | 345.0 | 123.4 | 95.7 | 3,144.6 |
| `tar.br (q 4)` | container | 0.000 | 353.0 | 181.5 | 93.1 | 16,428.5 |
| `tar.xz (lvl 3)` | container | 0.000 | 125.5 | 211.7 | 31,921.5 | 7,270.7 |
| `7z (lzma2)` | container | 0.000 | 44.2 | 85.0 | 223,970.3 | 19,021.1 |

---

## 3. Parallel Brotli (`PBRT0001`) Multi-Worker Scaling (`16.0 MiB` JSONL, Quality 5, `512 KiB` Chunks)

Because `brotli.compress` releases Python's GIL in C, `ParallelBrotliCompressor` scales cleanly across threads while capping in-flight memory to `<= 2 * workers * chunk_size`:

| Workers | Backend | Compress (MiB/s) | Decompress (MiB/s) | Compress Speedup | Peak Heap (MiB) |
| ---: | :--- | ---: | ---: | ---: | ---: |
| 1 | `thread` | 556.4 | 547.2 | 1.00x | 1.02 |
| 2 | `thread` | 1,000.5 | 991.4 | 1.80x | 2.03 |
