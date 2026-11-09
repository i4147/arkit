# Python API Reference

All public functions, classes, dataclasses, and exceptions are importable directly from the top-level `arkit` package (`from arkit import ...`) and carry full PEP 561 static type annotations (`mypy --strict` verified).

---

## 1. Universal Operations (`arkit.operations`)

### `compress(source, output, *, format=None, level=None, threads=None, include_root=False, follow_symlinks=False, include=None, exclude=None, filter_func=None, deterministic=False, compute_sha256=False, progress_callback=None)`
- **Path mode** (`source: StrPath, output: StrPath -> CompressionStats`): Atomically compresses a file or packs a directory into `output`, returning `CompressionStats`.
- **Bytes mode** (`source: bytes, output: Format | str -> bytes`): Compresses `source` bytes in memory using single-stream codec `output`.

### `decompress(archive_path, output_dir=..., *, format=None, safe=True, policy=None, overwrite=True, preserve_permissions=False, preserve_timestamps=True, strip_components=0, output_name=None, members=None, include=None, exclude=None)`
- **Path mode** (`-> Path`): Safely extracts container archives into `output_dir` or decompresses single-stream files into `output_dir / derived_name`.
- **Bytes mode** (`decompress(data: bytes, format=...) -> bytes`): Decompresses a byte buffer in memory.

### `pack(source, fmt, level=None, ...) -> bytes` / `pack_file(path, source, fmt=None, ...) -> Path` / `pack_stream(dst, source, fmt, ...) -> int`
Pack a directory path, a `Mapping[str, bytes]`, or an `Iterable[Member]` into any container format (`zip`, `whl`, `tar.*`, `7z`).

### `extract(source, dest, fmt=None, members=None, *, safe=True, policy=None, strip_components=0, ...) -> list[Path]`
Safely stream-extract an archive file or `bytes` buffer into `dest`, returning the list of written file paths.

### `convert(source, output, *, output_format=None, level=None, threads=None, deterministic=False, compute_sha256=False)`
Stream-transcode archives or compressed streams across any compatible formats (`zip <-> tar.zst`, `tar.gz -> tar.xz`, `gz -> br`, `7z -> tar.zst`), returning `CompressionStats` for paths or `bytes` for byte buffers. Also available as `convert_file`, `convert_bytes`, and `convert_stream`.

### Inspection, Mutation & Integrity
- `list_members(source, fmt=None) -> list[MemberInfo]` — Stream member headers without loading member bodies.
- `iter_members(source, fmt=None) -> Iterator[Member]` — Lazily yield `Member(info, data)` one entry at a time.
- `read_member(source, name, fmt=None) -> bytes` — Extract a single named member into `bytes`.
- `read_members(source, fmt=None) -> dict[str, bytes]` — Read all members into `{name: bytes}`.
- `add_member(path, name, data, fmt=None, level=None)` / `remove_member(path, name, fmt=None, level=None)` — Atomically add or remove a member from a container archive.
- `info(source, fmt=None, *, compute_sha256=False) -> ArchiveInfo` — Compute archive size, uncompressed payload size, member count, expansion ratio, and optional SHA-256.
- `diff_archives(source_a, source_b, *, fmt_a=None, fmt_b=None) -> DiffReport` — Compare two archives of any format by member names, sizes, and streaming SHA-256 digests (`added`, `removed`, `modified`, `unchanged`, `identical`).
- `validate(source, fmt=None) -> HealthReport` — Stream-verify headers, framing, and CRCs/checksums without writing to disk (`bool(report) == report.healthy`). Also `validate_file`, `validate_bytes`, `validate_stream`, `check_integrity`, and `verify_archive` (raises `CorruptedArchiveError`).
- `checksum(source, algorithm="sha256") -> str` & `compression_stats(source, result, *, compute_sha256=False) -> CompressionStats`.

---

## 2. Object-Oriented `Archive` (`arkit.archive`)

```python
from arkit import Archive, open_archive

# Create & inspect a bound archive handle
arch = Archive.pack("bundle.tar.zst", {"hello.txt": b"world"})
assert "hello.txt" in arch
assert arch.validate() is True
data = arch.read("hello.txt")
arch.add("extra.txt", b"more")
arch.extract("./out")

# Or configure a reusable engine
engine = Archive(compression_level=6, threads=4)
engine.compress("./src_dir", "release.tar.zst")
engine.decompress("release.tar.zst", "./unpacked", validate_first=True)
```

---

## 3. Streaming Processors & Batch Extraction (`arkit.processor`, `arkit.batch`)

- `process_archive(archive_path, processor_func, dst_or_filter=None, ...)`:
  - Accepts `fn(data: bytes) -> bytes` or `fn(data: bytes, filename: str) -> bytes`.
  - When 3rd argument (or `dst_path=`) is a destination path, writes a new archive and returns `Path`.
  - When `dst_path` is omitted, atomically rewrites `archive_path` in place and returns `int` (processed member count).
- `ArchiveProcessor(archive_path).process_streams(stream_func, filter_func=..., dst_path=...)`:
  - Calls `stream_func(reader: IO[bytes], writer: IO[bytes], member_name: str)` to transform arbitrarily large archive members in constant memory!
- `scan_archives(root)` / `batch_extract(root, *, workers=None, extract_nested=True, remove_source=False, policy=None)`:
  - Discovers and concurrently extracts archives across a directory tree (non-destructive by default).

---

## 4. Virtual Filesystem & Archive Walkers (`arkit.walker`)

### Read-Only Lazy `walk(root, *, max_depth=3, follow_symlinks=False, include_dirs=True, skip_dirs=None, pattern=None) -> Iterator[VirtualPath]`
Lazily traverses filesystem directories and nested archives (`zip`, `whl`, `tar.*`, `7z`, and single-stream `.gz`/`.zst`/`.xz`/`.bz2`/`.lz4`/`.br`/`.snappy`) without extracting to disk.
- `VirtualPath` provides `.name`, `.stem`, `.suffix`, `.suffixes`, `.parts`, `.parent`, `.parents`, `/`, `.match()`, `.relative_to()`, `.in_archive`, `.depth`, `.real`, `.size`, `.mtime`, `.open()`, `.read_bytes()`, and `.read_text()`, while intentionally omitting `__fspath__` so virtual archive paths can never be accidentally passed to `os.remove()` or `open()`.

### Two-Way Read/Write `AdvancedWalker` (`ArchiveWalker` alias)
```python
from arkit import AdvancedWalker

with AdvancedWalker("./deployments", max_archive_depth=3) as walker:
    for vf in walker:
        if vf.path.endswith("config.yaml"):
            vf.write_text(vf.read_text().replace("staging", "production"))
# Modified archives (including nested inner.zip inside outer.tar.zst!) are rebuilt
# deepest-first via atomic_open() upon clean context exit.
```

---

## 5. Parallel Brotli (`arkit.parallel_brotli`)

`ParallelBrotliCompressor` (aliased as `PBCompressor`) implements the chunked `PBRT0001` format:
- `compress_file(input_path, output_path)` / `decompress_file(input_path, output_path, workers=None)`
- `compress_stream(src, dst)` / `decompress_stream(src, dst, workers=None)`
- `compress_bytes(data) -> bytes` / `decompress_bytes(data, workers=None) -> bytes`
- Bounded memory: caps in-flight tasks to `<= 2 * workers` chunks and supports `backend="thread"` (default, GIL-free C Brotli) or `backend="process"`.

---

## 6. Split/Merge & Crash-Safe Atomic I/O (`arkit.split`, `arkit.atomicio`)

- `split_file(path, part_size, out_dir=None, *, manifest=False) -> list[Path]`
- `merge_files(parts, output, *, sort=False, verify_manifest=False) -> Path`
- `atomic_open(path, mode="wb", *, fsync=True, chmod_mode=None)` — Streaming context manager committing via `os.replace` on clean exit and unlinking `.tmp` on error.
- `atomic_write(path, content, *, backup=False, skip_unchanged=False, indent=2, sort_keys=False, newline=None)` — Supports `bytes`, `str`, and JSON-serializable objects (including `dataclass`, `Path`, `set`, `datetime`, `UUID`).

---

## 7. Exception Hierarchy (`arkit.exceptions`)

```text
ArchiveError
├── UnknownFormatError        # Unrecognized extension or headerless bytes
├── UnsupportedFormatError    # Operation unsupported for format
├── MissingCodecError         # Optional codec package not installed (subclass of ImportError)
├── CorruptedArchiveError     # Bad magic, truncated stream, or CRC mismatch (alias: CorruptedContent)
├── PasswordProtectedError    # Encrypted archive requiring password
├── ArchiveInPlaceError       # Callback failure during in-place ArchiveProcessor run
├── UnsafeArchivePathError    # Zip/Tar Slip traversal, symlink escape, or special node
└── ArchiveBombError          # Exceeded max_file_size, max_total_size, max_files, or max_ratio
```
