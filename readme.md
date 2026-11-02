## Package structure

```
pyarchive/
├── __init__.py          # Public API
├── compression.py       # Raw compression/decompression (streaming where possible)
├── tar_archive.py       # Tar + compression handling
├── zip_archive.py       # Zip and .whl
├── seven_zip.py         # 7z via py7zr
└── utils.py             # Format detection, helpers
```

## Key design decisions

- **Streaming**: For gz, xz, bz2, zstd, lz4, brotli we use the corresponding module’s streaming reader/writer. This avoids loading entire files into memory.
- **Tar**: We wrap the raw compressed stream with a `tarfile.TarFile` using `fileobj` parameter. For writing, we create a temporary output stream and compress on the fly.
- **process_archive**: Opens the archive, iterates over members, for each regular file reads its content (streamed), calls `process_func(content: bytes) -> bytes`, writes the processed content to a new archive. Only tar.*, zip, and whl are supported for this operation.
- **Format detection**: By file extension (e.g., `.tar.gz`, `.zip`, `.7z`). Falls back to magic bytes if needed.

## Core implementation snippets

### `compression.py` – streaming compress/decompress

For streaming we provide context managers:

