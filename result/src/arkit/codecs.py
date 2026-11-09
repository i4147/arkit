"""Streaming and in-memory compression/decompression codecs.

Every built-in stream codec (``gz``, ``bz2``, ``xz``, ``zst``, ``br``, ``lz4``,
``snappy``, ``pbrt``) operates in bounded ``O(chunk_size)`` memory without
loading full files into RAM.
"""

from __future__ import annotations

import bz2
import gzip
import importlib
import importlib.util
import io
import lzma
import os
import struct
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import ModuleType
from typing import IO, Any, Protocol, TypeAlias, cast

from .exceptions import CorruptedArchiveError, MissingCodecError, UnsupportedFormatError
from .models import ProgressCallback, StrPath

BinaryIO: TypeAlias = IO[bytes]

__all__ = [
    "BinaryIO",
    "BrotliStreamReader",
    "BrotliStreamWriter",
    "CHUNK_SIZE",
    "CodecReaderFactory",
    "CodecWriterFactory",
    "SnappyStreamReader",
    "SnappyStreamWriter",
    "codec_available",
    "compress_bytes_raw",
    "compress_stream",
    "copy_stream",
    "decompress_bytes_raw",
    "decompress_stream",
    "level_bounds",
    "missing_codecs",
    "open_compressed_reader",
    "open_compressed_writer",
    "open_stream_reader",
    "open_stream_writer",
    "register_codec",
    "resolve_level",
    "zstd_reader",
]

CHUNK_SIZE: int = 1024 * 1024  # 1 MiB streaming buffer
_SNAPPY_STREAM_HEADER: bytes = b"\xff\x06\x00\x00sNaPpY"
_SNAPPY_BLOCK_SIZE: int = 65536  # RFC Snappy framing max uncompressed chunk size
_PBRT_MAGIC: bytes = b"PBRT0001"

CodecReaderFactory: TypeAlias = Callable[[BinaryIO], BinaryIO]
CodecWriterFactory: TypeAlias = Callable[[BinaryIO, int | None], BinaryIO]

_CODEC_ALIASES: dict[str, str] = {
    "gz": "gz",
    "gzip": "gz",
    "bz2": "bz2",
    "bzip2": "bz2",
    "xz": "xz",
    "lzma": "xz",
    "zst": "zst",
    "zstd": "zst",
    "zstandard": "zst",
    "br": "br",
    "brotli": "br",
    "lz4": "lz4",
    "snappy": "snappy",
    "pbrt": "pbrt",
    "7z": "7z",
    "7zip": "7z",
    "sevenz": "7z",
}

#: (min_level, max_level, default_level) per canonical codec id.
_LEVEL_TABLE: dict[str, tuple[int, int, int]] = {
    "gz": (0, 9, 6),
    "bz2": (1, 9, 9),
    "xz": (0, 9, 6),
    "zst": (1, 22, 3),
    "br": (0, 11, 6),
    "lz4": (0, 16, 0),
    "snappy": (0, 0, 0),
    "pbrt": (0, 11, 4),
    "7z": (0, 9, 5),
}

_CUSTOM_READERS: dict[str, CodecReaderFactory] = {}
_CUSTOM_WRITERS: dict[str, CodecWriterFactory] = {}
_MODULE_CACHE: dict[str, ModuleType | None] = {}


def normalize_codec(codec: str) -> str:
    """Normalize a codec identifier or alias (e.g. ``'gzip'`` -> ``'gz'``)."""
    clean = codec.strip().lstrip(".").lower()
    if clean in _CUSTOM_READERS:
        return clean
    if clean in _CODEC_ALIASES:
        return _CODEC_ALIASES[clean]
    raise UnsupportedFormatError(f"Unsupported compression codec: {codec!r}")


def level_bounds(codec: str) -> tuple[int, int, int]:
    """Return ``(min_level, max_level, default_level)`` for ``codec``."""
    norm = normalize_codec(codec)
    return _LEVEL_TABLE.get(norm, (0, 9, 6))


def resolve_level(codec: str, level: int | None) -> int:
    """Validate and resolve compression ``level`` for ``codec``."""
    lo, hi, default = level_bounds(codec)
    if level is None:
        return default
    if lo == hi == 0:
        # Format does not expose tunable levels (e.g. Snappy); accept 0 or ignore gracefully if 0
        if level == 0:
            return 0
        raise ValueError(f"Compression level {level} is not supported for {codec!r} (valid: 0)")
    if not (lo <= level <= hi):
        raise ValueError(f"Compression level {level} outside valid range [{lo}, {hi}] for {codec!r}")
    return level


def _try_import(module_name: str) -> ModuleType | None:
    if module_name not in _MODULE_CACHE:
        try:
            _MODULE_CACHE[module_name] = importlib.import_module(module_name)
        except ImportError:
            _MODULE_CACHE[module_name] = None
    return _MODULE_CACHE[module_name]


def _require_zstd() -> ModuleType:
    mod = _try_import("zstandard")
    if mod is not None:
        return mod
    raise MissingCodecError("Zstandard support requires: pip install arkit[zstd] (or pip install zstandard)")


def _require_brotli() -> ModuleType:
    mod = _try_import("brotli") or _try_import("brotlicffi")
    if mod is not None:
        return mod
    raise MissingCodecError("Brotli support requires: pip install arkit[brotli] (or pip install brotli)")


def _require_lz4() -> ModuleType:
    mod = _try_import("lz4.frame")
    if mod is not None:
        return mod
    raise MissingCodecError("LZ4 support requires: pip install arkit[lz4] (or pip install lz4)")


def _require_snappy() -> Any:
    cram = _try_import("cramjam")
    if cram is not None and hasattr(cram, "snappy"):
        return cram.snappy
    raise MissingCodecError("Snappy support requires: pip install arkit[snappy] (or pip install cramjam)")


def _require_py7zr() -> ModuleType:
    mod = _try_import("py7zr")
    if mod is not None:
        return mod
    raise MissingCodecError("7z support requires: pip install arkit[sevenzip] (or pip install py7zr)")


def codec_available(codec: str) -> bool:
    """Return ``True`` if ``codec`` is available (stdlib, installed extra, or user-registered)."""
    if codec in _CUSTOM_READERS:
        return True
    try:
        norm = normalize_codec(codec)
    except UnsupportedFormatError:
        return False

    if norm in {"gz", "bz2", "xz"}:
        return True
    if norm == "zst":
        return importlib.util.find_spec("zstandard") is not None
    if norm in {"br", "pbrt"}:
        return (
            importlib.util.find_spec("brotli") is not None
            or importlib.util.find_spec("brotlicffi") is not None
        )
    if norm == "lz4":
        return importlib.util.find_spec("lz4.frame") is not None
    if norm == "snappy":
        return importlib.util.find_spec("cramjam") is not None
    if norm == "7z":
        return importlib.util.find_spec("py7zr") is not None
    return False


def missing_codecs() -> list[str]:
    """Return a list of optional built-in codecs not currently installed."""
    return [c for c in ("lz4", "zst", "br", "snappy", "7z") if not codec_available(c)]


def register_codec(
    codec: str,
    reader_factory: CodecReaderFactory,
    writer_factory: CodecWriterFactory | None = None,
) -> None:
    """Register a custom decompression (and optional compression) stream codec."""
    key = codec.strip().lstrip(".").lower()
    if not key:
        raise ValueError("Codec name must be non-empty")
    _CUSTOM_READERS[key] = reader_factory
    if writer_factory is not None:
        _CUSTOM_WRITERS[key] = writer_factory


def copy_stream(
    source: BinaryIO,
    destination: BinaryIO,
    chunk_size: int = CHUNK_SIZE,
    *,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    """Copy ``source`` to ``destination`` in bounded chunks and return bytes copied."""
    total = 0
    while chunk := source.read(chunk_size):
        destination.write(chunk)
        total += len(chunk)
        if progress_callback is not None:
            progress_callback(total, total_hint)
    return total


# ---------------------------------------------------------------------------
# Streaming Brotli Reader & Writer
# ---------------------------------------------------------------------------


class BrotliStreamReader(io.RawIOBase):
    """True ``O(chunk_size)``-memory streaming Brotli reader verifying stream completion."""

    def __init__(self, source: BinaryIO, *, closefd: bool = False, chunk_size: int = CHUNK_SIZE) -> None:
        super().__init__()
        brotli = _require_brotli()
        self._brotli = brotli
        self._source = source
        self._closefd = closefd
        self._chunk_size = chunk_size
        self._decompressor = brotli.Decompressor()
        self._buffer = bytearray()
        self._eof = False

    def readable(self) -> bool:
        return True

    def _fill(self) -> None:
        if self._eof:
            return
        start_len = len(self._buffer)
        while len(self._buffer) == start_len and not self._eof:
            chunk = self._source.read(self._chunk_size)
            if not chunk:
                if not self._decompressor.is_finished():
                    raise CorruptedArchiveError("Truncated or incomplete Brotli stream")
                self._eof = True
                return
            try:
                out = self._decompressor.process(chunk)
            except Exception as exc:
                raise CorruptedArchiveError(f"Corrupt Brotli stream: {exc}") from exc
            if out:
                self._buffer.extend(out)

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        if size is None or size < 0:
            chunks: list[bytes] = []
            if self._buffer:
                chunks.append(bytes(self._buffer))
                self._buffer.clear()
            while not self._eof:
                self._fill()
                if self._buffer:
                    chunks.append(bytes(self._buffer))
                    self._buffer.clear()
            return b"".join(chunks)

        while len(self._buffer) < size and not self._eof:
            self._fill()
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        data = self.read(len(view))
        n = len(data)
        view[:n] = data
        return n

    def close(self) -> None:
        if not self.closed:
            try:
                if self._closefd:
                    self._source.close()
            finally:
                super().close()


class BrotliStreamWriter(io.RawIOBase):
    """True ``O(chunk_size)``-memory streaming Brotli writer."""

    def __init__(
        self,
        destination: BinaryIO,
        *,
        quality: int = 6,
        mode: int | None = None,
        closefd: bool = False,
    ) -> None:
        super().__init__()
        brotli = _require_brotli()
        self._destination = destination
        self._closefd = closefd
        resolved_mode = mode if mode is not None else int(brotli.MODE_GENERIC)
        self._compressor = brotli.Compressor(quality=quality, mode=resolved_mode)
        self._finished = False

    def writable(self) -> bool:
        return True

    def write(self, b: Any) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        raw = bytes(b)
        if not raw:
            return 0
        out = self._compressor.process(raw)
        if out:
            self._destination.write(out)
        return len(raw)

    def flush(self) -> None:
        if not self.closed and not self._finished:
            out = self._compressor.flush()
            if out:
                self._destination.write(out)
            self._destination.flush()

    def close(self) -> None:
        if not self.closed:
            try:
                if not self._finished:
                    self._finished = True
                    tail = self._compressor.finish()
                    if tail:
                        self._destination.write(tail)
                    self._destination.flush()
            finally:
                try:
                    super().close()
                finally:
                    if self._closefd:
                        self._destination.close()


# ---------------------------------------------------------------------------
# True O(64 KiB) Framed Snappy Streaming Reader & Writer
# ---------------------------------------------------------------------------


class SnappyStreamReader(io.RawIOBase):
    """Framed Snappy stream reader operating in bounded ~64 KiB memory with CRC32C verification."""

    def __init__(self, source: BinaryIO, *, closefd: bool = False) -> None:
        super().__init__()
        self._snappy = _require_snappy()
        self._source = source
        self._closefd = closefd
        self._buffer = bytearray()
        self._eof = False
        self._saw_header = False

    def readable(self) -> bool:
        return True

    def _read_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            part = self._source.read(n - len(buf))
            if not part:
                break
            buf.extend(part)
        return bytes(buf)

    def _pull_frame(self) -> None:
        start_len = len(self._buffer)
        while len(self._buffer) == start_len and not self._eof:
            hdr = self._read_exact(4)
            if not hdr:
                if not self._saw_header:
                    raise CorruptedArchiveError("Empty Snappy framed stream (missing stream identifier)")
                self._eof = True
                return
            if len(hdr) < 4:
                raise CorruptedArchiveError("Truncated Snappy frame header")

            chunk_type = hdr[0]
            chunk_len = int.from_bytes(hdr[1:4], "little")
            body = self._read_exact(chunk_len)
            if len(body) != chunk_len:
                raise CorruptedArchiveError("Truncated Snappy frame body")

            if chunk_type == 0xFF:
                if body != b"sNaPpY":
                    raise CorruptedArchiveError("Invalid Snappy stream identifier payload")
                self._saw_header = True
            elif not self._saw_header:
                raise CorruptedArchiveError("Snappy frame encountered before stream identifier header")
            elif chunk_type in (0x00, 0x01):
                try:
                    decoded = bytes(self._snappy.decompress(_SNAPPY_STREAM_HEADER + hdr + body))
                except Exception as exc:
                    raise CorruptedArchiveError(f"Corrupt Snappy frame or CRC mismatch: {exc}") from exc
                if decoded:
                    self._buffer.extend(decoded)
            elif 0x02 <= chunk_type <= 0x7F:
                raise CorruptedArchiveError(f"Unsupported unskippable Snappy chunk type: 0x{chunk_type:02x}")
            # 0x80..0xFD and 0xFE (padding) are skippable per Snappy framing spec.

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        if size is None or size < 0:
            out_parts: list[bytes] = []
            if self._buffer:
                out_parts.append(bytes(self._buffer))
                self._buffer.clear()
            while not self._eof:
                self._pull_frame()
                if self._buffer:
                    out_parts.append(bytes(self._buffer))
                    self._buffer.clear()
            return b"".join(out_parts)

        while len(self._buffer) < size and not self._eof:
            self._pull_frame()
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        data = self.read(len(view))
        n = len(data)
        view[:n] = data
        return n

    def close(self) -> None:
        if not self.closed:
            try:
                if self._closefd:
                    self._source.close()
            finally:
                super().close()


class SnappyStreamWriter(io.RawIOBase):
    """Framed Snappy stream writer operating in bounded 64 KiB blocks."""

    def __init__(self, destination: BinaryIO, *, closefd: bool = False) -> None:
        super().__init__()
        snappy = _require_snappy()
        self._destination = destination
        self._closefd = closefd
        self._compressor = snappy.Compressor()
        self._wrote_any = False
        self._finished = False

    def writable(self) -> bool:
        return True

    def write(self, b: Any) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        view = memoryview(b).cast("B")
        total = len(view)
        offset = 0
        while offset < total:
            step = view[offset : offset + _SNAPPY_BLOCK_SIZE]
            self._compressor.compress(bytes(step))
            flushed = bytes(self._compressor.flush())
            if flushed:
                self._destination.write(flushed)
            self._wrote_any = True
            offset += len(step)
        return total

    def flush(self) -> None:
        if not self.closed and not self._finished:
            flushed = bytes(self._compressor.flush())
            if flushed:
                self._destination.write(flushed)
            self._destination.flush()

    def close(self) -> None:
        if not self.closed:
            try:
                if not self._finished:
                    self._finished = True
                    if not self._wrote_any:
                        # Emit valid empty Snappy framed stream header
                        self._compressor.compress(b"")
                    tail = bytes(self._compressor.finish())
                    if tail:
                        self._destination.write(tail)
                    elif not self._wrote_any:
                        self._destination.write(_SNAPPY_STREAM_HEADER)
                    self._destination.flush()
            finally:
                try:
                    super().close()
                finally:
                    if self._closefd:
                        self._destination.close()


# ---------------------------------------------------------------------------
# Parallel Brotli (PBRT0001) Streaming Reader
# ---------------------------------------------------------------------------


class _PbrtStreamReader(io.RawIOBase):
    """Streaming reader for ``PBRT0001`` chunked parallel Brotli containers."""

    def __init__(self, source: BinaryIO, *, closefd: bool = False) -> None:
        super().__init__()
        self._brotli = _require_brotli()
        self._source = source
        self._closefd = closefd
        magic = self._read_exact(len(_PBRT_MAGIC))
        if magic != _PBRT_MAGIC:
            raise CorruptedArchiveError("Invalid PBRT0001 magic header")
        hdr = self._read_exact(4)
        if len(hdr) != 4:
            raise CorruptedArchiveError("Truncated PBRT0001 chunk count")
        (self._chunk_count,) = struct.unpack("<I", hdr)
        self._chunks_read = 0
        self._buffer = bytearray()
        self._eof = self._chunk_count == 0

    def readable(self) -> bool:
        return True

    def _read_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            part = self._source.read(n - len(buf))
            if not part:
                break
            buf.extend(part)
        return bytes(buf)

    def _pull_chunk(self) -> None:
        start_len = len(self._buffer)
        while len(self._buffer) == start_len and not self._eof:
            if self._chunks_read >= self._chunk_count:
                self._eof = True
                return
            meta = self._read_exact(8)
            if len(meta) != 8:
                raise CorruptedArchiveError("Truncated PBRT0001 chunk header")
            comp_len, orig_len = struct.unpack("<II", meta)
            payload = self._read_exact(comp_len)
            if len(payload) != comp_len:
                raise CorruptedArchiveError("Truncated PBRT0001 chunk payload")
            try:
                decoded = self._brotli.decompress(payload)
            except Exception as exc:
                raise CorruptedArchiveError(f"Corrupt PBRT0001 chunk payload: {exc}") from exc
            if len(decoded) != orig_len:
                raise CorruptedArchiveError(
                    f"PBRT0001 chunk length mismatch: expected {orig_len}, got {len(decoded)}"
                )
            self._chunks_read += 1
            if decoded:
                self._buffer.extend(decoded)

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        if size is None or size < 0:
            parts: list[bytes] = []
            if self._buffer:
                parts.append(bytes(self._buffer))
                self._buffer.clear()
            while not self._eof:
                self._pull_chunk()
                if self._buffer:
                    parts.append(bytes(self._buffer))
                    self._buffer.clear()
            return b"".join(parts)

        while len(self._buffer) < size and not self._eof:
            self._pull_chunk()
        out = bytes(self._buffer[:size])
        del self._buffer[:size]
        return out

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        data = self.read(len(view))
        n = len(data)
        view[:n] = data
        return n

    def close(self) -> None:
        if not self.closed:
            try:
                if self._closefd:
                    self._source.close()
            finally:
                super().close()


# ---------------------------------------------------------------------------
# Incremental Sniffing Decompressors (for O(1)-memory deep format detection)
# ---------------------------------------------------------------------------


class IncrementalDecoder(Protocol):
    """Protocol for small-buffer incremental stream decoding (e.g. header sniffing)."""

    def feed(self, data: bytes) -> bytes: ...
    def finish(self) -> bytes: ...


class _StdIncremental:
    def __init__(self, obj: Any) -> None:
        self._obj = obj

    def feed(self, data: bytes) -> bytes:
        if getattr(self._obj, "eof", False):
            return b""
        return bytes(self._obj.decompress(data))

    def finish(self) -> bytes:
        return b""


class _BrotliIncremental:
    def __init__(self) -> None:
        brotli = _require_brotli()
        self._dec = brotli.Decompressor()

    def feed(self, data: bytes) -> bytes:
        if self._dec.is_finished():
            return b""
        return bytes(self._dec.process(data))

    def finish(self) -> bytes:
        return b""


class _ZstdIncremental:
    def __init__(self) -> None:
        zstd = _require_zstd()
        self._dec = zstd.ZstdDecompressor().decompressobj()

    def feed(self, data: bytes) -> bytes:
        return bytes(self._dec.decompress(data))

    def finish(self) -> bytes:
        flush = getattr(self._dec, "flush", None)
        return bytes(flush()) if callable(flush) else b""


class _Lz4Incremental:
    def __init__(self) -> None:
        lz4f = _require_lz4()
        self._ctx = lz4f.create_decompression_context()
        self._lz4f = lz4f

    def feed(self, data: bytes) -> bytes:
        chunk, _bytes_read, _eof = self._lz4f.decompress_chunk(self._ctx, data)
        return bytes(chunk)

    def finish(self) -> bytes:
        return b""


class _SnappyIncremental:
    def __init__(self) -> None:
        self._buf = bytearray()
        self._snappy = _require_snappy()
        self._saw_hdr = False

    def feed(self, data: bytes) -> bytes:
        self._buf.extend(data)
        out = bytearray()
        while len(self._buf) >= 4:
            ctype = self._buf[0]
            clen = int.from_bytes(self._buf[1:4], "little")
            if len(self._buf) < 4 + clen:
                break
            hdr = bytes(self._buf[:4])
            body = bytes(self._buf[4 : 4 + clen])
            del self._buf[: 4 + clen]
            if ctype == 0xFF and body == b"sNaPpY":
                self._saw_hdr = True
            elif self._saw_hdr and ctype in (0x00, 0x01):
                out.extend(bytes(self._snappy.decompress(_SNAPPY_STREAM_HEADER + hdr + body)))
        return bytes(out)

    def finish(self) -> bytes:
        return b""


def make_incremental_decoder(codec: str) -> IncrementalDecoder:
    """Create an incremental decompressor for ``codec`` (used for O(1)-RAM deep tar sniffing)."""
    norm = normalize_codec(codec)
    if norm == "gz":
        return _StdIncremental(zlib.decompressobj(47))
    if norm == "bz2":
        return _StdIncremental(bz2.BZ2Decompressor())
    if norm == "xz":
        return _StdIncremental(lzma.LZMADecompressor())
    if norm == "zst":
        return _ZstdIncremental()
    if norm == "br":
        return _BrotliIncremental()
    if norm == "lz4":
        return _Lz4Incremental()
    if norm == "snappy":
        return _SnappyIncremental()
    raise UnsupportedFormatError(f"Incremental decoding not supported for codec: {codec!r}")


# ---------------------------------------------------------------------------
# Stream Context Managers & Openers
# ---------------------------------------------------------------------------


def zstd_reader(source: BinaryIO, *, closefd: bool = False) -> BinaryIO:
    """Wrap ``source`` in a streaming Zstandard reader supporting multi-frame streams."""
    zstd = _require_zstd()
    dctx = zstd.ZstdDecompressor()
    try:
        raw = dctx.stream_reader(source, read_across_frames=True, closefd=closefd)
    except TypeError:
        raw = dctx.stream_reader(source)
    return io.BufferedReader(raw)


@contextmanager
def open_compressed_reader(source: StrPath | BinaryIO, codec: str) -> Iterator[BinaryIO]:
    """Context manager yielding a streaming decompressed binary reader for ``codec``."""
    raw_key = codec.strip().lstrip(".").lower()
    owns_file = isinstance(source, (str, os.PathLike))
    fh: BinaryIO = open(source, "rb") if isinstance(source, (str, os.PathLike)) else source
    try:
        if raw_key in _CUSTOM_READERS:
            custom_reader = _CUSTOM_READERS[raw_key](fh)
            try:
                yield custom_reader
            finally:
                custom_reader.close()
            return

        norm = normalize_codec(codec)
        if norm == "gz":
            with gzip.GzipFile(fileobj=fh, mode="rb") as reader:
                yield cast(BinaryIO, reader)
        elif norm == "bz2":
            with bz2.BZ2File(fh, mode="rb") as reader:
                yield reader
        elif norm == "xz":
            with lzma.LZMAFile(fh, mode="rb") as reader:
                yield reader
        elif norm == "zst":
            reader_zst = zstd_reader(fh, closefd=False)
            try:
                yield reader_zst
            finally:
                reader_zst.close()
        elif norm == "br":
            reader_br = io.BufferedReader(BrotliStreamReader(fh, closefd=False))
            try:
                yield reader_br
            finally:
                reader_br.close()
        elif norm == "lz4":
            lz4f = _require_lz4()
            with lz4f.open(fh, mode="rb") as reader_lz4:
                yield cast(BinaryIO, reader_lz4)
        elif norm == "snappy":
            reader_sn = io.BufferedReader(SnappyStreamReader(fh, closefd=False))
            try:
                yield reader_sn
            finally:
                reader_sn.close()
        elif norm == "pbrt":
            reader_pb = io.BufferedReader(_PbrtStreamReader(fh, closefd=False))
            try:
                yield reader_pb
            finally:
                reader_pb.close()
        else:
            raise UnsupportedFormatError(f"Unsupported stream reader codec: {codec!r}")
    finally:
        if owns_file:
            fh.close()


@contextmanager
def open_compressed_writer(
    destination: StrPath | BinaryIO,
    codec: str,
    *,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> Iterator[BinaryIO]:
    """Context manager yielding a streaming compressed binary writer for ``codec``."""
    raw_key = codec.strip().lstrip(".").lower()
    owns_file = isinstance(destination, (str, os.PathLike))
    fh: BinaryIO = open(destination, "wb") if isinstance(destination, (str, os.PathLike)) else destination
    try:
        if raw_key in _CUSTOM_WRITERS:
            custom_writer = _CUSTOM_WRITERS[raw_key](fh, level)
            try:
                yield custom_writer
            finally:
                custom_writer.close()
            return

        norm = normalize_codec(codec)
        lvl = resolve_level(norm, level)
        if norm == "gz":
            mtime: float | None = 0 if deterministic else None
            with gzip.GzipFile(fileobj=fh, mode="wb", compresslevel=lvl, mtime=mtime) as writer:
                yield cast(BinaryIO, writer)
        elif norm == "bz2":
            with bz2.BZ2File(fh, mode="wb", compresslevel=lvl) as writer:
                yield writer
        elif norm == "xz":
            with lzma.LZMAFile(fh, mode="wb", format=lzma.FORMAT_XZ, preset=lvl) as writer:
                yield writer
        elif norm == "zst":
            zstd = _require_zstd()
            z_threads = threads if threads is not None else 0
            cctx = zstd.ZstdCompressor(level=lvl, threads=z_threads, write_checksum=True)
            with cctx.stream_writer(fh, closefd=False) as writer_zst:
                yield cast(BinaryIO, writer_zst)
        elif norm == "br":
            writer_br = io.BufferedWriter(BrotliStreamWriter(fh, quality=lvl, closefd=False))
            try:
                yield writer_br
            finally:
                writer_br.close()
        elif norm == "lz4":
            lz4f = _require_lz4()
            with lz4f.open(fh, mode="wb", compression_level=lvl) as writer_lz4:
                yield cast(BinaryIO, writer_lz4)
        elif norm == "snappy":
            writer_sn = io.BufferedWriter(SnappyStreamWriter(fh, closefd=False))
            try:
                yield writer_sn
            finally:
                writer_sn.close()
        else:
            raise UnsupportedFormatError(f"Unsupported stream writer codec: {codec!r}")
    finally:
        if owns_file:
            fh.close()


def open_stream_reader(path: StrPath, fmt: str) -> BinaryIO:
    """Open ``path`` and return a standalone decompressing binary file-like object."""
    raw = open(path, "rb")
    try:
        norm = normalize_codec(fmt)
        if norm == "gz":
            return cast(BinaryIO, gzip.GzipFile(fileobj=raw, mode="rb"))
        if norm == "bz2":
            return bz2.BZ2File(raw, mode="rb")
        if norm == "xz":
            return lzma.LZMAFile(raw, mode="rb")
        if norm == "zst":
            return zstd_reader(raw, closefd=True)
        if norm == "br":
            return io.BufferedReader(BrotliStreamReader(raw, closefd=True))
        if norm == "lz4":
            lz4f = _require_lz4()
            return cast(BinaryIO, lz4f.open(raw, mode="rb"))
        if norm == "snappy":
            return io.BufferedReader(SnappyStreamReader(raw, closefd=True))
        if norm == "pbrt":
            return io.BufferedReader(_PbrtStreamReader(raw, closefd=True))
        raise UnsupportedFormatError(f"Unsupported compression format: {fmt!r}")
    except Exception:
        raw.close()
        raise


def open_stream_writer(
    path: StrPath,
    fmt: str,
    *,
    level: int | None = None,
    threads: int | None = None,
) -> BinaryIO:
    """Open ``path`` and return a standalone compressing binary file-like object."""
    raw = open(path, "wb")
    try:
        norm = normalize_codec(fmt)
        lvl = resolve_level(norm, level)
        if norm == "gz":
            return cast(BinaryIO, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=lvl))
        if norm == "bz2":
            return bz2.BZ2File(raw, mode="wb", compresslevel=lvl)
        if norm == "xz":
            return lzma.LZMAFile(raw, mode="wb", format=lzma.FORMAT_XZ, preset=lvl)
        if norm == "zst":
            zstd = _require_zstd()
            cctx = zstd.ZstdCompressor(level=lvl, threads=threads if threads is not None else 0)
            return cast(BinaryIO, cctx.stream_writer(raw, closefd=True))
        if norm == "br":
            return io.BufferedWriter(BrotliStreamWriter(raw, quality=lvl, closefd=True))
        if norm == "lz4":
            lz4f = _require_lz4()
            return cast(BinaryIO, lz4f.open(raw, mode="wb", compression_level=lvl))
        if norm == "snappy":
            return io.BufferedWriter(SnappyStreamWriter(raw, closefd=True))
        raise UnsupportedFormatError(f"Unsupported compression format: {fmt!r}")
    except Exception:
        raw.close()
        raise


def compress_stream(
    source: BinaryIO,
    destination: BinaryIO,
    codec: str,
    *,
    level: int | None = None,
    threads: int | None = None,
    chunk_size: int = CHUNK_SIZE,
    deterministic: bool = False,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    """Stream-compress ``source`` into ``destination`` using ``codec``.

    Returns the number of uncompressed input bytes processed.
    """
    norm = codec.strip().lstrip(".").lower()
    if norm not in _CUSTOM_WRITERS:
        norm = normalize_codec(codec)
    if norm == "7z":
        py7zr = _require_py7zr()
        lvl = resolve_level("7z", level)
        payload = source.read()
        buf = io.BytesIO()
        with py7zr.SevenZipFile(buf, "w", filters=[{"id": py7zr.FILTER_LZMA2, "preset": lvl}]) as sz:
            sz.writestr(payload, "data")
        destination.write(buf.getvalue())
        if progress_callback is not None:
            progress_callback(len(payload), total_hint)
        return len(payload)

    with open_compressed_writer(
        destination,
        codec,
        level=level,
        threads=threads,
        deterministic=deterministic,
    ) as writer:
        return copy_stream(
            source,
            writer,
            chunk_size=chunk_size,
            progress_callback=progress_callback,
            total_hint=total_hint,
        )


def decompress_stream(
    source: BinaryIO,
    destination: BinaryIO,
    codec: str,
    *,
    chunk_size: int = CHUNK_SIZE,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    """Stream-decompress ``source`` into ``destination`` using ``codec``.

    Returns the number of uncompressed output bytes written.
    """
    norm = codec.strip().lstrip(".").lower()
    if norm not in _CUSTOM_READERS:
        norm = normalize_codec(codec)
    if norm == "7z":
        payload = decompress_bytes_raw(source.read(), "7z")
        destination.write(payload)
        if progress_callback is not None:
            progress_callback(len(payload), total_hint)
        return len(payload)

    with open_compressed_reader(source, codec) as reader:
        return copy_stream(
            reader,
            destination,
            chunk_size=chunk_size,
            progress_callback=progress_callback,
            total_hint=total_hint,
        )


def compress_bytes_raw(
    data: bytes,
    codec: str,
    *,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes:
    """Compress raw ``bytes`` in memory using ``codec``."""
    norm = normalize_codec(codec)
    lvl = resolve_level(norm, level)
    if norm == "gz":
        return gzip.compress(data, compresslevel=lvl, mtime=0 if deterministic else None)
    if norm == "bz2":
        return bz2.compress(data, compresslevel=lvl)
    if norm == "xz":
        return lzma.compress(data, format=lzma.FORMAT_XZ, preset=lvl)
    if norm == "zst":
        zstd = _require_zstd()
        cctx = zstd.ZstdCompressor(
            level=lvl,
            threads=threads if threads is not None else 0,
            write_checksum=True,
        )
        return bytes(cctx.compress(data))
    if norm == "br":
        brotli = _require_brotli()
        return bytes(brotli.compress(data, quality=lvl))
    if norm == "lz4":
        lz4f = _require_lz4()
        return bytes(lz4f.compress(data, compression_level=lvl))
    if norm == "snappy":
        snappy = _require_snappy()
        return bytes(snappy.compress(data))
    if norm == "7z":
        buf = io.BytesIO()
        compress_stream(io.BytesIO(data), buf, "7z", level=lvl)
        return buf.getvalue()
    if norm == "pbrt":
        from .parallel_brotli import ParallelBrotliCompressor

        pbc = ParallelBrotliCompressor(quality=lvl, workers=threads)
        return pbc.compress_bytes(data)
    raise UnsupportedFormatError(f"Unsupported compression codec: {codec!r}")


def decompress_bytes_raw(data: bytes, codec: str) -> bytes:
    """Decompress raw ``bytes`` in memory using ``codec``."""
    norm = normalize_codec(codec)
    try:
        if norm == "gz":
            return gzip.decompress(data)
        if norm == "bz2":
            return bz2.decompress(data)
        if norm == "xz":
            return lzma.decompress(data)
        if norm == "zst":
            out = io.BytesIO()
            decompress_stream(io.BytesIO(data), out, "zst")
            return out.getvalue()
        if norm == "br":
            brotli = _require_brotli()
            return bytes(brotli.decompress(data))
        if norm == "lz4":
            lz4f = _require_lz4()
            return bytes(lz4f.decompress(data))
        if norm == "snappy":
            snappy = _require_snappy()
            return bytes(snappy.decompress(data))
        if norm == "pbrt":
            from .parallel_brotli import ParallelBrotliCompressor

            return ParallelBrotliCompressor.decompress_bytes(data)
        if norm == "7z":
            py7zr = _require_py7zr()
            pio = importlib.import_module("py7zr.io")
            with py7zr.SevenZipFile(io.BytesIO(data), "r") as sz:
                names = [f.filename for f in sz.list() if not f.is_directory]
                if len(names) != 1:
                    raise CorruptedArchiveError(
                        f"Expected 1 member in single-stream 7z payload, found {len(names)}"
                    )
                factory = pio.BytesIOFactory(limit=1 << 30)
                sz.extract(targets=names, factory=factory)
                bio = factory.products.get(names[0])
                return bio.read() if bio is not None else b""
    except CorruptedArchiveError:
        raise
    except Exception as exc:
        raise CorruptedArchiveError(f"Failed to decompress {norm} payload: {exc}") from exc
    raise UnsupportedFormatError(f"Unsupported compression codec: {codec!r}")
