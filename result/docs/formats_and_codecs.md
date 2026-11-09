# Formats, Codecs & Plugin System

`arkit` natively supports **20 built-in formats** spanning **8 single-stream compression codecs** and **12 multi-file archive containers**, with zero mandatory third-party dependencies for stdlib formats (`zip`, `whl`, `tar`, `gz`, `bz2`, `xz`) and lazy on-demand loading for optional C-accelerated codecs.

---

## 1. Built-In Format Matrix

| `Format` Enum | Canonical ID | Recognized Suffixes / Aliases | Kind | Level Range (Default) | Multi-Thread | Dependency |
| :--- | :--- | :--- | :--- | :--- | :---: | :--- |
| `Format.GZ` | `gz` | `.gz`, `.gzip` | Stream | `1..9` (`6`) | No | stdlib `gzip` |
| `Format.BZ2` | `bz2` | `.bz2`, `.bzip2` | Stream | `1..9` (`9`) | No | stdlib `bz2` |
| `Format.XZ` | `xz` | `.xz`, `.lzma` | Stream | `0..9` (`6`) | No | stdlib `lzma` |
| `Format.ZST` | `zst` | `.zst`, `.zstd`, `zstandard` | Stream | `1..22` (`3`) | **Yes** | `zstandard` / Python 3.14 `compression.zstd` |
| `Format.BR` | `br` | `.br`, `.brotli` | Stream | `0..11` (`6`) | No | `brotli` / `brotlicffi` |
| `Format.PBRT` | `pbrt` | `.pbrt` | Stream | `0..11` (`4`) | **Yes** | `brotli` / `brotlicffi` |
| `Format.LZ4` | `lz4` | `.lz4` | Stream | `0..16` (`0`) | No | `lz4` |
| `Format.SNAPPY` | `snappy` | `.snappy`, `.sz` | Stream | — | No | `cramjam` / `python-snappy` |
| `Format.ZIP` | `zip` | `.zip` | Container | `0..9` (`6`) | No | stdlib `zipfile` (ZIP64 enabled) |
| `Format.WHL` | `whl` | `.whl`, `wheel` | Container | `0..9` (`6`) | No | stdlib `zipfile` |
| `Format.TAR` | `tar` | `.tar` | Container | — | No | stdlib `tarfile` (PAX format) |
| `Format.TAR_GZ` | `tar.gz` | `.tar.gz`, `.tgz` | Container | `1..9` (`6`) | No | stdlib `tarfile` + `gzip` |
| `Format.TAR_BZ2` | `tar.bz2` | `.tar.bz2`, `.tbz2`, `.tbz` | Container | `1..9` (`9`) | No | stdlib `tarfile` + `bz2` |
| `Format.TAR_XZ` | `tar.xz` | `.tar.xz`, `.txz`, `.tlz` | Container | `0..9` (`6`) | No | stdlib `tarfile` + `lzma` |
| `Format.TAR_ZST` | `tar.zst` | `.tar.zst`, `.tar.zstd`, `.tzst` | Container | `1..22` (`3`) | **Yes** | `zstandard` |
| `Format.TAR_BR` | `tar.br` | `.tar.br`, `.tar.brotli`, `.tbr` | Container | `0..11` (`6`) | No | `brotli` |
| `Format.TAR_LZ4` | `tar.lz4` | `.tar.lz4`, `.tlz4` | Container | `0..16` (`0`) | No | `lz4` |
| `Format.TAR_SNAPPY` | `tar.snappy` | `.tar.snappy`, `.tar.sz`, `.tsz` | Container | — | No | `cramjam` |
| `Format.SEVENZ` | `7z` | `.7z`, `7zip`, `sevenz` | Container | `0..9` (`5`) | No | `py7zr` |
| `Format.TAR_SEVENZ` | `tar.7z` | `.tar.7z`, `.t7z` | Container | `0..9` (`5`) | No | `py7zr` |

---

## 2. Format Detection & Deep Tar Sniffing

`arkit` resolves formats using a fast two-tier strategy:

1. **Longest-Suffix Filename Matching (`guess_format`)**:
   Compound extensions like `.tar.zst` or `.tar.gz` always take priority over `.zst` or `.gz`.
2. **Constant-RAM Magic Byte & Deep Tar Sniffing (`detect` / `detect_file`)**:
   When a file has no extension (or an ambiguous name), `detect_file(path, deep=True)` reads only the first `512` bytes to match magic signatures (`PK\x03\x04` for ZIP, `\x28\xb5\x2f\xfd` for Zstd, `7z\xbc\xaf\x27\x1c` for 7z, `PBRT0001` for Parallel Brotli, `\xff\x06\x00\x00sNaPpY` for Snappy, `\x04\x22\x4d\x18` for LZ4, etc.).
   When a stream codec header is matched and `deep=True`, `arkit` incrementally decompresses **only the first 512 bytes** of the payload to check for the POSIX `ustar` signature at byte offset `257` — accurately distinguishing `.tar.zst` from `.zst` or `.tar.snappy` from `.snappy` on multi-gigabyte files in `< 64 KiB` of RAM!

> **Note on Raw Brotli (`.br`)**: Per RFC 7932, raw Brotli streams intentionally have no magic header bytes. When decompressing raw Brotli from an extensionless `bytes` buffer, pass `format="br"` explicitly. Chunked Parallel Brotli (`.pbrt`) includes the 8-byte `PBRT0001` magic header and is auto-detected from both files and byte buffers.

---

## 3. Runtime Codec & Format Plugins

Applications can register custom stream codecs and custom extensions at runtime:

```python
from arkit import register_codec, register_format, open_stream_writer, open_stream_reader

register_codec(
    "mycodec",
    reader_factory=my_stream_reader,
    writer_factory=my_stream_writer,
    level_range=(1, 9, 5),
)
register_format(
    "mycodec",
    suffixes=(".myc",),
    kind="stream",
    codec="mycodec",
)
```
