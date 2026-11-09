# CLI Reference

Install `arkit` to expose the `arkit` command (also runnable via `python -m arkit`):

```bash
arkit --help
arkit --version
```

Every subcommand supports `--json` for structured machine-readable output, `-v/--verbose` for diagnostic logging, and `-q/--quiet` to suppress non-error text output.

---

## Subcommands Overview

### 1. `arkit compress` (alias: `pack`)
Create an archive or compress a single file atomically.

```bash
# Multi-threaded Zstandard tar archive with SHA-256 digest
arkit compress ./my_project release.tar.zst -l 9 -t 4 --checksum

# Deterministic reproducible archive filtering out build artifacts
arkit compress ./my_project dist.tar.gz --deterministic -x "*.pyc" -x "__pycache__/*"

# Preview files that would be packed without writing to disk
arkit compress ./my_project preview.zip --dry-run
```

### 2. `arkit extract` (alias: `decompress`)
Safely extract an archive or single-stream file with traversal and bomb guards.

```bash
# Extract into ./out stripping top-level directory prefix
arkit extract release.tar.zst -o ./out --strip-components 1

# Enforce custom bomb thresholds and extract only Python files
arkit extract untrusted.zip -o ./sandbox -i "*.py" --max-total-size 100MiB --max-ratio 50
```

### 3. `arkit batch-extract`
Scan a directory tree and extract all archives concurrently in parallel threads.

```bash
arkit batch-extract ./incoming_dumps -w 4 --json
```

### 4. `arkit list` (alias: `ls`) & `arkit info`
Inspect archive contents and compression ratios without extracting to disk.

```bash
arkit list release.tar.zst -p "*.py"
arkit info release.tar.zst --checksum --json
```

### 5. `arkit validate` (alias: `test`)
Stream-verify archive headers, framing, and member CRCs/checksums (exit code `0` if healthy, `2` if corrupt).

```bash
arkit validate release.tar.zst
arkit validate release.tar.zst --json
```

### 6. `arkit convert`
Stream-transcode between any two formats (`tar.gz -> tar.zst`, `zip -> tar.xz`, `gz -> zst`, etc.).

```bash
arkit convert legacy.tar.gz modern.tar.zst -l 6 -t 4 --checksum
```

### 7. `arkit diff`
Compare two archives (even across different formats, e.g. `.zip` vs `.tar.zst`!) by member content digests (exit code `0` if identical, `1` if differences exist).

```bash
arkit diff build_v1.zip build_v2.tar.zst
```

### 8. `arkit walk`
Lazily list or search (`--grep`) inside directories and nested archives without extracting to disk!

```bash
# Walk up to 3 levels of nested archives
arkit walk ./releases --max-depth 3 --files-only

# Search for a string inside nested .tar.zst / .zip / .gz files in place
arkit walk ./releases --grep "DATABASE_URL" --json
```

### 9. `arkit split` & `arkit merge`
Split large archives into fixed-size parts (`KiB`, `MiB`, `GiB`) with optional `.sha256` manifests, and reassemble them safely.

```bash
arkit split backup.tar.zst -s 500MiB -o ./parts --manifest
arkit merge ./parts/backup.tar.zst.part* -o restored.tar.zst --sort --verify-manifest
```

### 10. `arkit pbrt`
Chunked multi-worker Parallel Brotli (`PBRT0001`) compression and decompression.

```bash
arkit pbrt huge.jsonl huge.jsonl.pbrt -q 5 -c 1MiB -w 4 --text
arkit pbrt -d huge.jsonl.pbrt restored.jsonl -w 4
```

### 11. `arkit codecs`
Display installed codec backends, version status, and valid compression level ranges.

```bash
arkit codecs
arkit codecs --json
```
