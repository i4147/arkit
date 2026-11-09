# `arkit` — Universal Streaming Archive Toolkit for Python

**`arkit`** is a production-ready, memory-bounded, and security-hardened Python archive library and CLI. It unifies **20 archive and compression formats**, streaming cross-format transcoding, safe-by-default extraction, in-place member processing, archive-aware virtual filesystem walkers, chunked parallel Brotli (`PBRT0001`), multi-part split/merge with SHA-256 manifests, and crash-safe atomic file I/O under one clean, strictly-typed (`mypy --strict`) API.

---

## Highlights

- **20 Unified Formats**:
  - **Containers**: `.zip`, `.whl`, `.tar`, `.tar.gz` (`.tgz`), `.tar.bz2` (`.tbz2`), `.tar.xz` (`.txz`), `.tar.zst` (`.tzst`), `.tar.br`, `.tar.lz4`, `.tar.snappy`, `.7z`, `.tar.7z`.
  - **Single-Stream Codecs**: `.gz`, `.bz2`, `.xz`, `.zst`, `.br`, `.pbrt` (Parallel Brotli), `.lz4`, `.snappy` (`.sz`).
  - **Extensible**: Register custom stream codecs and extensions at runtime via `register_codec()` and `register_format()`.
- **Constant `O(chunk_size)` Streaming Memory**:
  - Single-pass `tar.*` streaming (`mode="w|"` / `"r|"`) and seekable `ZstdTarFile` without intermediate `.tar` buffers.
  - True `O(64 KiB)` framed Snappy (`SnappyStreamReader`/`SnappyStreamWriter`) and `O(1 MiB)` Brotli (`BrotliStreamReader`/`BrotliStreamWriter`) — **11.9x to 294x lower peak Python heap** than full-buffer implementations.
  - Zero-staging cross-format `tar.* -> tar.*` byte-pipe recompression and streaming `zip <-> tar.*` transcoding.
  - O(1)-RAM magic-byte & incremental 512-byte deep `ustar` tar header detection (`detect_file`).
- **Safe by Default**:
  - Blocks Zip/Tar Slip path traversal (`../`), absolute/drive paths, NUL bytes, symlink/hardlink escapes, and device/FIFO nodes out of the box (`UnsafeArchivePathError`).
  - Mid-stream decompression bomb enforcement (`SafetyPolicy`: `max_file_size`, `max_total_size`, `max_files`, `max_ratio` -> `ArchiveBombError`).
  - Crash-safe `atomic_open()` / `atomic_write()` (`fsync` + POSIX `os.replace`) so interrupted operations never corrupt or truncate existing files.
- **Advanced Tooling Included**:
  - **Virtual Archive Walkers**: Read-only lazy `walk()` (`VirtualPath`) to inspect/grep nested archives without disk extraction, plus `AdvancedWalker` (`VirtualFile`) for two-way nested archive editing with atomic deepest-first rebuilds.
  - **In-Place Member Processing**: `process_archive()` and `ArchiveProcessor.process_streams()` to transform matching files inside archives in bounded memory.
  - **Parallel Brotli (`PBRT0001`)**: `ParallelBrotliCompressor` (`PBCompressor`) with `<= 2 * workers` bounded in-flight chunks across GIL-free threads or processes.
  - **Reproducible Builds & Diffing**: Deterministic archive creation (`deterministic=True`), content-hash `diff_archives()` across disparate formats, and multi-part `split_file`/`merge_files` with `.sha256` manifests.

---

## Installation

`arkit` requires **Python 3.10+** and has **zero mandatory runtime dependencies** for standard library formats (`zip`, `whl`, `tar`, `gz`, `bz2`, `xz`). Install optional C-accelerated codecs via extras:

```bash
# Core package (stdlib formats only)
pip install ./result

# Install with all optional codecs (zstandard, brotli, lz4, cramjam/snappy, py7zr)
pip install "./result[all]"

# Development & test suite dependencies
pip install -e "./result[dev]"
```

---

## Python Quickstart

### 1. Compress, Validate, Inspect, Convert & Extract

```python
from pathlib import Path
from arkit import compress, convert, decompress, diff_archives, info, validate

# Multi-threaded .tar.zst compression with SHA-256 digest
stats = compress(
    Path("my_project"),
    Path("my_project.tar.zst"),
    format="tar.zst",
    level=9,
    threads=4,
    compute_sha256=True,
)
print(f"Compressed {stats.source_bytes:,} B -> {stats.result_bytes:,} B ({stats.space_change_percent:.1f}% saved)")

# Constant-RAM integrity check & metadata inspection
report = validate("my_project.tar.zst")
assert report.healthy

meta = info("my_project.tar.zst")
print(f"{meta.member_count} members, payload={meta.payload_size:,} B")

# Stream-convert .tar.zst -> .zip and verify content equivalence
convert("my_project.tar.zst", "my_project.zip", output_format="zip", level=6)
assert diff_archives("my_project.tar.zst", "my_project.zip").identical

# Safe extraction with bomb & path-traversal protection
decompress("my_project.tar.zst", Path("restored_project"))
```

### 2. Walk & Edit Nested Archives Without Manual Extraction

```python
from arkit import AdvancedWalker, walk

# Lazily search inside nested .tar.zst -> .zip -> .gz files without extracting to disk
for vpath in walk("./releases", max_depth=3):
    if vpath.match("*.json") and b"deprecated" in vpath.read_bytes():
        print(f"Found in {vpath} (depth={vpath.depth})")

# Modify files inside nested archives and atomically rebuild deepest-first on exit
with AdvancedWalker("./releases") as editor:
    for vf in editor:
        if vf.path.endswith("settings.env"):
            vf.write_text(vf.read_text().replace("DEBUG=1", "DEBUG=0"))
```

### 3. In-Place Stream Processing & Parallel Brotli (`PBRT0001`)

```python
from arkit import PBCompressor, process_archive

# Strip blank lines from every file inside data.tar.gz -> cleaned.tar.gz
process_archive(
    "data.tar.gz",
    lambda data: b"\n".join(line for line in data.splitlines() if line.strip()),
    "cleaned.tar.gz",
)

# Multi-threaded chunked Brotli compression with bounded memory
pbc = PBCompressor(quality=4, chunk_size=1024 * 1024, workers=4)
pbc.compress_file("events.jsonl", "events.jsonl.pbrt")
PBCompressor.decompress_file("events.jsonl.pbrt", "events.restored.jsonl")
```

---

## Command-Line Interface (`arkit`)

```bash
# Compress with deterministic reproducible metadata & SHA-256 checksum
arkit compress ./project project.tar.zst -l 6 -t 4 --deterministic --checksum

# Inspect, list, validate, and diff archives (supports --json on all commands)
arkit info project.tar.zst --json
arkit list project.tar.zst -p "*.py"
arkit validate project.tar.zst
arkit convert project.tar.zst project.zip
arkit diff project.tar.zst project.zip

# Search inside nested archives without extracting to disk
arkit walk ./releases --grep "TODO" --json

# Split & merge large archives with SHA-256 part verification
arkit split project.tar.zst -s 100MiB -o ./parts --manifest
arkit merge ./parts/project.tar.zst.part* -o merged.tar.zst --sort --verify-manifest
```

---

## Memory Efficiency Summary (`24.0 MiB` Workload, `tracemalloc`)

| Operation | Legacy Full-Buffer Peak Heap | `arkit` Streaming Peak Heap | Reduction |
| :--- | ---: | ---: | ---: |
| Brotli single-file compress (`.br`) | `24.00 MiB` | **`2.02 MiB`** | **11.9x lower** |
| Compound `.tar.snappy` creation | `26.08 MiB` | **`0.09 MiB`** | **294.3x lower** |
| Cross-format `.tar.gz -> .tar.zst` | `50.08 MiB` | **`4.21 MiB`** | **11.9x lower** |
| Splitting `24 MiB` into `12 MiB` parts | `24.01 MiB` | **`3.01 MiB`** | **8.0x lower** |

See [`benchmark/results.md`](./benchmark/results.md) for the full throughput, compression ratio, and scaling tables.

---

## Project Layout

```text
result/
├── pyproject.toml         # PEP 621 package metadata, optional extras, strict mypy & ruff config
├── README.md              # Project overview & quickstart
├── LICENSE                # MIT License
├── src/arkit/             # 17 strictly-typed modules + PEP 561 py.typed marker
├── tests/                 # 111 pytest unit, security, CLI, compatibility, & tracemalloc tests
├── docs/                  # Architecture, format matrix, security model, API & CLI manuals
├── examples/              # 5 self-contained runnable example scripts
└── benchmark/             # Throughput, heap-memory, & parallel-scaling benchmarks + results.md
```

## Documentation Links

- [Documentation Index](./docs/index.md)
- [Architecture & Memory Efficiency](./docs/architecture_and_memory.md)
- [Supported Formats & Codec Plugins](./docs/formats_and_codecs.md)
- [Security Model & Crash Safety](./docs/security.md)
- [Python API Reference](./docs/api_reference.md)
- [CLI Reference](./docs/cli_reference.md)
- [Runnable Examples](./examples/README.md)
- [Empirical Benchmarks](./benchmark/results.md)
