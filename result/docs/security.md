# Security & Crash Safety

Untrusted archives are one of the most common attack vectors in backend services, build pipelines, and data ingestion systems. `arkit` enforces **safe-by-default extraction and atomic filesystem writes** across every container and stream format.

---

## 1. Threat Model & Default Defenses

| Threat | Attack Mechanism | `arkit` Default Defense | Exception Raised |
| :--- | :--- | :--- | :--- |
| **Zip Slip / Tar Slip** | Member names containing `../../etc/cron.d/evil` or Windows `..\` separators. | `sanitize_member_name()` & `safe_destination()` normalize separators, reject `..` segments, and verify `(dest / member).resolve().is_relative_to(dest.resolve())`. | `UnsafeArchivePathError` |
| **Absolute & Drive Paths** | `/etc/passwd`, `\\server\share\x`, or `C:\Windows\...` member names. | Rejected outright before touching the filesystem. | `UnsafeArchivePathError` |
| **NUL-Byte Path Truncation** | Embedded `\x00` characters inside archive member filenames. | Rejected immediately during path sanitization. | `UnsafeArchivePathError` |
| **Symlink & Hardlink Escapes** | Creating a symlink `sub -> /etc` and extracting `sub/shadow` through it. | Symlinks and hardlinks are **rejected by default** (`allow_symlinks=False`, `allow_hardlinks=False`). Even when explicitly enabled, `arkit` resolves link targets lexically and rejects any target escaping the extraction root, plus checks existing parent components for symlinks (`ensure_parent`). | `UnsafeArchivePathError` |
| **Special Device / FIFO Nodes** | Tar `CHRTYPE`, `BLKTYPE`, or `FIFOTYPE` entries creating device files or blocking readers on named pipes. | Rejected by default (`allow_special=False`). | `UnsafeArchivePathError` |
| **Zip / Zstd / 7z Bombs** | Tiny compressed archives expanding into tens or hundreds of gigabytes or millions of tiny files. | `ExtractionGuard` monitors **every streamed chunk** and aborts **mid-stream** before disk exhaustion if `max_file_size`, `max_total_size`, `max_files`, or `max_ratio` is exceeded. | `ArchiveBombError` |
| **Partial / Torn Writes on Crash** | Power loss, `SIGINT`, or mid-stream `ArchiveBombError` leaving half-written archives on disk. | Every file creation, conversion, split, merge, and in-place transform writes to a sibling `.tmp` file via `atomic_open()`, flushes/`fsync`s, and commits via atomic POSIX `os.replace()` — unlinking the temp file on any exception. | Original file remains 100% intact |

---

## 2. Customizing `SafetyPolicy`

All extraction functions (`extract`, `decompress`, `batch_extract`, `Archive.extract`) accept a `SafetyPolicy` object:

```python
from arkit import SafetyPolicy, decompress

strict_policy = SafetyPolicy(
    max_file_size=100 * 1024 * 1024,    # 100 MiB per member
    max_total_size=500 * 1024 * 1024,   # 500 MiB total extracted bytes
    max_files=5_000,                    # Maximum 5,000 members
    max_ratio=100.0,                    # Max 100:1 expansion ratio
    ratio_min_bytes=1024 * 1024,        # Start checking ratio after 1 MiB
    allow_symlinks=False,               # Block all symlinks
    allow_hardlinks=False,              # Block all hardlinks
    allow_special=False,                # Block FIFOs / block / char devices
    expected_top_dir="my-package-1.0",  # Require all entries under this prefix
)

decompress("untrusted_upload.tar.zst", "./sandbox", policy=strict_policy)
```

---

## 3. Cryptographic Integrity & Reproducible Builds

- **End-to-End Checksums**: Pass `compute_sha256=True` to `compress()`, `convert()`, or `info()` (or `--checksum` on the CLI) to compute SHA-256 digests in bounded `1 MiB` chunks.
- **Multi-Part Split Manifests**: `split_file(..., manifest=True)` emits `<filename>.sha256` recording the digest of every part; `merge_files(..., verify_manifest=True)` verifies every part before committing the merged output.
- **Deterministic Reproducible Archives**: Pass `deterministic=True` (or `--deterministic` on the CLI) to sort archive entries lexicographically, normalize UID/GID/permissions (`0644`/`0755`), clamp modification times (`SOURCE_DATE_EPOCH` if set, else `0` UTC / `1980-01-01` DOS epoch for ZIP), and zero Gzip header timestamps — producing byte-for-byte identical archives across machines.
