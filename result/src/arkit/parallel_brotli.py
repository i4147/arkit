"""Memory-bounded parallel Brotli container (``PBRT0001``) compressor & decompressor.

Improves upon the legacy ``ParallelBrotliCompressor`` in four major ways:
1. **Bounded backpressure**: Submits at most ``workers * 2`` chunks in flight instead of
   feeding an unbounded ``multiprocessing.Pool.imap`` queue that buffered entire files in RAM.
2. **GIL-releasing thread pool default** (with optional ``backend='process'``): Avoids IPC
   serialization copies of multi-megabyte chunks while scaling across CPU cores.
3. **Zero-temp-file byte & stream APIs**: ``compress_bytes`` and ``decompress_bytes``
   operate directly on streams without round-tripping through temporary files on disk.
4. **Atomic output writes & parallel decompression**: Uses :func:`~arkit.atomicio.atomic_open`
   and validates uncompressed chunk lengths on decode.
"""

from __future__ import annotations

import io
import os
import struct
from collections import deque
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import IO, Any, Literal

from .atomicio import atomic_open
from .exceptions import CorruptedArchiveError, MissingCodecError
from .models import ProgressCallback, StrPath

__all__ = [
    "PBCompressor",
    "ParallelBrotliCompressor",
]


def _get_brotli() -> Any:
    try:
        import brotli

        return brotli
    except ImportError:
        try:
            import brotlicffi

            return brotlicffi
        except ImportError as exc:
            raise MissingCodecError(
                "Parallel Brotli support requires: pip install arkit[brotli] (or pip install brotli)"
            ) from exc


def _compress_chunk_worker(args: tuple[int, bytes, int, int]) -> tuple[int, bytes, int]:
    index, chunk, quality, mode = args
    brotli = _get_brotli()
    compressed: bytes = brotli.compress(chunk, quality=quality, mode=mode)
    return index, compressed, len(chunk)


def _decompress_chunk_worker(args: tuple[int, bytes, int]) -> tuple[int, bytes]:
    index, payload, expected_len = args
    brotli = _get_brotli()
    try:
        decoded: bytes = brotli.decompress(payload)
    except Exception as exc:
        raise CorruptedArchiveError(f"Corrupt Brotli payload in chunk {index}: {exc}") from exc
    if len(decoded) != expected_len:
        raise CorruptedArchiveError(
            f"Chunk {index} uncompressed length mismatch: expected {expected_len}, got {len(decoded)}"
        )
    return index, decoded


class ParallelBrotliCompressor:
    """Parallel chunked Brotli compressor/decompressor using the ``PBRT0001`` frame container.

    Container binary layout::

        8B magic: b"PBRT0001"
        4B uint32 LE: chunk_count
        Repeated chunk_count times:
            4B uint32 LE: compressed_chunk_len
            4B uint32 LE: original_chunk_len
            <compressed_chunk_len> bytes: independent Brotli frame
    """

    MAGIC: bytes = b"PBRT0001"

    def __init__(
        self,
        quality: int = 4,
        chunk_size: int = 4 * 1024 * 1024,
        workers: int | None = None,
        mode: int | None = None,
        progress_callback: ProgressCallback | None = None,
        *,
        backend: Literal["thread", "process"] = "thread",
    ) -> None:
        if not 0 <= quality <= 11:
            raise ValueError("quality must be between 0 and 11")
        if chunk_size < 64 * 1024:
            raise ValueError("chunk_size must be at least 64 KiB (65536 bytes)")
        brotli = _get_brotli()
        self.quality: int = quality
        self.chunk_size: int = chunk_size
        self.workers: int = max(1, workers or (os.cpu_count() or 1))
        self.mode: int = mode if mode is not None else int(getattr(brotli, "MODE_GENERIC", 0))
        self.progress_callback: ProgressCallback | None = progress_callback
        self.backend: Literal["thread", "process"] = backend

    def _make_executor(self, workers: int | None = None) -> Executor:
        n = max(1, workers or self.workers)
        if self.backend == "process":
            return ProcessPoolExecutor(max_workers=n)
        return ThreadPoolExecutor(max_workers=n)

    def compress_stream(
        self,
        source: IO[bytes],
        destination: IO[bytes],
        *,
        total_size: int | None = None,
    ) -> int:
        """Compress ``source`` into ``destination`` (must be seekable to patch ``chunk_count``).

        Maintains at most ``workers * 2`` chunks in flight so peak memory stays strictly
        bounded to ``O(workers * chunk_size)`` regardless of input size.
        """
        if not destination.seekable():
            raise ValueError("ParallelBrotliCompressor.compress_stream requires a seekable destination")

        destination.write(self.MAGIC)
        count_pos = destination.tell()
        destination.write(struct.pack("<I", 0))

        max_in_flight = max(2, self.workers * 2)
        pending: deque[Future[tuple[int, bytes, int]]] = deque()
        chunk_count = 0
        bytes_done = 0
        next_index = 0
        eof = False

        with self._make_executor() as executor:
            while not eof or pending:
                while not eof and len(pending) < max_in_flight:
                    chunk = source.read(self.chunk_size)
                    if not chunk:
                        eof = True
                        break
                    fut = executor.submit(
                        _compress_chunk_worker,
                        (next_index, chunk, self.quality, self.mode),
                    )
                    pending.append(fut)
                    next_index += 1

                if pending:
                    _idx, compressed, orig_len = pending.popleft().result()
                    destination.write(struct.pack("<II", len(compressed), orig_len))
                    destination.write(compressed)
                    chunk_count += 1
                    bytes_done += orig_len
                    if self.progress_callback is not None:
                        self.progress_callback(bytes_done, total_size)

        end_pos = destination.tell()
        destination.seek(count_pos)
        destination.write(struct.pack("<I", chunk_count))
        destination.seek(end_pos)
        return bytes_done

    def compress_file(self, input_path: StrPath, output_path: StrPath) -> Path:
        """Compress ``input_path`` into ``output_path`` atomically with bounded memory."""
        src_path = Path(input_path)
        dst_path = Path(output_path)
        total_size = src_path.stat().st_size
        with src_path.open("rb") as f_in, atomic_open(dst_path, "wb") as f_out:
            self.compress_stream(f_in, f_out, total_size=total_size)
        return dst_path

    def compress_bytes(self, data: bytes) -> bytes:
        """Compress ``data`` in memory without temporary files on disk."""
        buf_out = io.BytesIO()
        self.compress_stream(io.BytesIO(data), buf_out, total_size=len(data))
        return buf_out.getvalue()

    @classmethod
    def decompress_stream(
        cls,
        source: IO[bytes],
        destination: IO[bytes],
        *,
        workers: int = 1,
        progress_callback: ProgressCallback | None = None,
    ) -> int:
        """Decompress a ``PBRT0001`` stream into ``destination`` with bounded memory."""
        magic = source.read(len(cls.MAGIC))
        if magic != cls.MAGIC:
            raise ValueError("Not a ParallelBrotli (PBRT0001) container")
        raw_count = source.read(4)
        if len(raw_count) != 4:
            raise CorruptedArchiveError("Truncated PBRT0001 chunk count header")
        (chunk_count,) = struct.unpack("<I", raw_count)

        bytes_written = 0
        if workers <= 1 or chunk_count <= 1:
            for idx in range(chunk_count):
                hdr = source.read(8)
                if len(hdr) != 8:
                    raise CorruptedArchiveError("Truncated PBRT0001 chunk metadata")
                comp_len, orig_len = struct.unpack("<II", hdr)
                payload = source.read(comp_len)
                if len(payload) != comp_len:
                    raise CorruptedArchiveError("Truncated PBRT0001 chunk payload")
                _, decoded = _decompress_chunk_worker((idx, payload, orig_len))
                destination.write(decoded)
                bytes_written += len(decoded)
                if progress_callback is not None:
                    progress_callback(bytes_written, None)
            return bytes_written

        max_in_flight = max(2, workers * 2)
        pending: deque[Future[tuple[int, bytes]]] = deque()
        submitted = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            while submitted < chunk_count or pending:
                while submitted < chunk_count and len(pending) < max_in_flight:
                    hdr = source.read(8)
                    if len(hdr) != 8:
                        raise CorruptedArchiveError("Truncated PBRT0001 chunk metadata")
                    comp_len, orig_len = struct.unpack("<II", hdr)
                    payload = source.read(comp_len)
                    if len(payload) != comp_len:
                        raise CorruptedArchiveError("Truncated PBRT0001 chunk payload")
                    pending.append(executor.submit(_decompress_chunk_worker, (submitted, payload, orig_len)))
                    submitted += 1

                if pending:
                    _idx, decoded = pending.popleft().result()
                    destination.write(decoded)
                    bytes_written += len(decoded)
                    if progress_callback is not None:
                        progress_callback(bytes_written, None)

        return bytes_written

    @classmethod
    def decompress_file(
        cls,
        input_path: StrPath,
        output_path: StrPath,
        *,
        workers: int = 1,
        progress_callback: ProgressCallback | None = None,
    ) -> Path:
        """Decompress a ``PBRT0001`` file into ``output_path`` atomically."""
        src_path = Path(input_path)
        dst_path = Path(output_path)
        with src_path.open("rb") as f_in, atomic_open(dst_path, "wb") as f_out:
            cls.decompress_stream(
                f_in,
                f_out,
                workers=workers,
                progress_callback=progress_callback,
            )
        return dst_path

    @classmethod
    def decompress_bytes(cls, data: bytes, *, workers: int = 1) -> bytes:
        """Decompress ``PBRT0001`` container bytes in memory."""
        out = io.BytesIO()
        cls.decompress_stream(io.BytesIO(data), out, workers=workers)
        return out.getvalue()


#: Alias matching ``usage_br.py`` (``PBCompressor``).
PBCompressor = ParallelBrotliCompressor
