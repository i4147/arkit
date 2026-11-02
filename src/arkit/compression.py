import gzip, bz2, lzma, lz4.frame, zstandard, brotli


def compress(data: bytes, fmt: str) -> bytes:
    """Compress raw bytes using the given format (gz, xz, bz2, lz4, zstd, br)."""
    if fmt == "gz":
        return gzip.compress(data)
    elif fmt == "xz":
        return lzma.compress(data)
    elif fmt == "bz2":
        return bz2.compress(data)
    elif fmt == "lz4":
        return lz4.frame.compress(data)
    elif fmt == "zstd":
        cctx = zstandard.ZstdCompressor()
        return cctx.compress(data)
    elif fmt == "br":
        return brotli.compress(data)
    else:
        raise ValueError(f"Unsupported compression: {fmt}")


def decompress(data: bytes, fmt: str) -> bytes:
    """Decompress bytes."""
    if fmt == "gz":
        return gzip.decompress(data)
    elif fmt == "xz":
        return lzma.decompress(data)
    elif fmt == "bz2":
        return bz2.decompress(data)
    elif fmt == "lz4":
        return lz4.frame.decompress(data)
    elif fmt == "zstd":
        dctx = zstandard.ZstdDecompressor()
        return dctx.decompress(data)
    elif fmt == "br":
        return brotli.decompress(data)
    else:
        raise ValueError(f"Unsupported compression: {fmt}")


def open_stream_reader(path: str, fmt: str):
    """Return a file-like object that decompresses on the fly."""
    raw = open(path, "rb")
    if fmt == "gz":
        return gzip.GzipFile(fileobj=raw)
    elif fmt == "xz":
        return lzma.open(raw, "rb")
    elif fmt == "bz2":
        return bz2.BZ2File(raw, "rb")
    elif fmt == "lz4":
        return lz4.frame.open(raw, "rb")
    elif fmt == "zstd":
        dctx = zstandard.ZstdDecompressor()
        return dctx.stream_reader(raw)
    elif fmt == "br":
        return brotli.Decompressor(fileobj=raw)
    else:
        raw.close()
        raise ValueError(...)
