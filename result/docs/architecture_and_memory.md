# Architecture & Memory Efficiency

`arkit` is engineered around a strict architectural principle: **archive and stream operations should scale with `O(chunk_size)` memory (`64 KiB`–`1 MiB`), never `O(archive_size)`**.

---

## 1. Layered Package Architecture

```text
arkit/
├── exceptions.py       # Unified exception hierarchy rooted at ArchiveError
├── models.py           # Immutable slots dataclasses (CompressionStats, HealthReport, SafetyPolicy, ...)
├── atomicio.py         # Crash-safe atomic_open() stream & atomic_write() with fsync + os.replace
├── safety.py           # Path sanitization, symlink/hardlink guards, mid-stream ExtractionGuard
├── codecs.py           # True streaming readers/writers (gz, bz2, xz, zst, br, lz4, snappy, pbrt, 7z)
├── formats.py          # Unified Format enum, O(1)-RAM magic header & deep tar sniffing, plugin registry
├── tar_stream.py       # Single-pass streaming TarFile readers/writers + seekable ZstdTarFile subclass
├── parallel_brotli.py  # Bounded-window PBRT0001 multi-worker Brotli compressor/decompressor
├── operations.py       # Universal compress, decompress, extract, pack, convert, validate, info, diff
├── archive.py          # Dual-mode OO Archive handle (bound file/bytes wrapper + configured engine)
├── processor.py        # Streaming member transformer (ArchiveProcessor, process_archive)
├── batch.py            # Concurrent archive scanner & recursive batch extractor
├── split.py            # 1 MiB chunked file split/merge with optional .sha256 manifests
├── walker.py           # Lazy read-only walk() (VirtualPath) & two-way AdvancedWalker (VirtualFile)
└── cli.py              # 13-subcommand CLI with --dry-run, --deterministic, and --json output
```

---

## 2. Key Memory Optimizations Over Legacy Prototypes

During the redesign of the 20 prototype scripts into `arkit`, every major memory and correctness bottleneck was systematically eliminated:

| Subsystem | Legacy Prototype Bottleneck | `arkit` Streaming Architecture |
| :--- | :--- | :--- |
| **Brotli (`.br`, `.tar.br`)** | `brotli.compress(src.read_bytes())` and `_BufferWriter(bytearray())` buffered 100% of uncompressed + compressed files in RAM. | `BrotliStreamReader` (`brotli.Decompressor().process(chunk)` + `is_finished()` truncation check) and `BrotliStreamWriter` (`brotli.Compressor().process/flush/finish`) stream in `1 MiB` chunks (`~2 MiB` heap). |
| **Snappy (`.snappy`, `.tar.snappy`)** | Accumulated entire payload in `bytearray()` before calling `snappy.compress()`, plus `cramjam.Compressor.compress()` return-type bug (`int` vs `Buffer`). | `SnappyStreamWriter` & `SnappyStreamReader` parse and emit 64 KiB Snappy framed chunks (`0xFF sNaPpY` stream header + CRC32C frames) in `< 200 KiB` Python heap! |
| **Compound Tar (`tar.zst`, `tar.lz4`, `tar.br`, `tar.snappy`)** | Built entire `.tar` in `io.BytesIO()` in RAM before compressing, or decompressed entire `.tar` into `BytesIO` before iterating. | `open_tar_stream_writer` (`tarfile.open(fileobj=codec_writer, mode="w|")`) and `open_tar_stream_reader` (`mode="r|"`) pipe tar blocks directly through streaming codecs in a single pass with zero intermediate `.tar` buffer. |
| **Format Detection (`detect_file`)** | `detect(path.read_bytes())` read the entire multi-GB archive into RAM just to check 6 magic bytes! | `detect_file()` reads only the first `512` bytes from disk, and when `deep=True` decompresses at most `512` bytes incrementally to check the POSIX `ustar` tar signature at offset `257`. |
| **Validation (`validate`)** | `validate_file(p)` called `validate_bytes(p.read_bytes())`, loading the entire file into RAM and skipping payload CRCs on tar/streams. | Streams every member / codec frame through `_CountingSink` (`0 B` storage), verifying Zip CRC32, Zstd/LZ4/Snappy/XZ/Gzip/Bzip2 checksums, Brotli EOF markers, and 7z CRCs in constant RAM. |
| **Cross-Format Conversion (`convert_file`)** | Extracted all members into a `dict[str, bytes]` in RAM (`read_members`) and repacked in RAM (`pack`). | Uses four streaming fast paths: (1) `1 MiB` stream-to-stream pipe (`gz <-> zst`), (2) raw tar byte-stream recompression (`tar.gz -> tar.zst` without unpacking tar headers!), (3) member-by-member streaming `zip <-> tar.*` copy without temp extraction, and (4) temp-dir fallback only when `7z` random-access container requires it. |
| **Parallel Brotli (`PBRT0001`)** | Submitted all chunks into an unbounded list of `Future`s (`O(file_size)` RAM) and wrote temporary files inside `compress_bytes()`. | Caps in-flight work to `<= workers * 2` chunks using `concurrent.futures.wait(..., FIRST_COMPLETED)`, defaults to zero-copy `ThreadPoolExecutor` (since C `brotli` releases the GIL), writes via `atomic_open`, and supports true zero-disk `compress_bytes`/`decompress_bytes`. |
| **Split & Merge (`split_file`, `merge_files`)** | `fh.read(part_size)` allocated an entire `part_size` buffer (e.g. `4 GiB` per part!) in RAM at once. | Copies each part in `min(1 MiB, remaining)` slices while incrementally computing SHA-256 digests (`~3 MiB` heap regardless of part size). |
| **Directory `CompressionStats`** | Used `source_path.stat().st_size` (`4096` bytes directory inode!) when compressing directories, reporting bogus negative compression ratios. | Sums actual regular file byte sizes across the packed tree while tracking wall-clock `elapsed_seconds` and optional output `sha256`. |
| **Nested Walkers (`walk`, `AdvancedWalker`)** | Loaded outer archives eagerly; `VirtualFile` lacked streaming helpers and risked partial overwrites on exceptions. | `walk()` streams sequential `tarfile` pipes (`r|*`) for nested archives and caps recursion via `max_depth`; `AdvancedWalker` stages inner archive bytes via `SpooledTemporaryFile` and commits modified archives deepest-first via `atomic_open` only when exiting cleanly without exceptions. |
