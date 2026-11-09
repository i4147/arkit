"""Single-pass streaming TAR helpers and ``ZstdTarFile``.

Enables reading and writing TAR archives compressed with any supported codec
(``gz``, ``bz2``, ``xz``, ``zst``, ``br``, ``lz4``, ``snappy``, ``7z``) in a
single streaming pass without writing uncompressed intermediate ``.tar`` files
to disk or holding archives in RAM.
"""

from __future__ import annotations

import io
import os
import tarfile
import tempfile
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import IO, Any

from .codecs import (
    CHUNK_SIZE,
    _require_py7zr,
    _require_zstd,
    open_compressed_reader,
    open_compressed_writer,
    resolve_level,
    zstd_reader,
)
from .exceptions import CorruptedArchiveError, UnsupportedFormatError
from .formats import Compression, Container, Format
from .models import StrPath

__all__ = [
    "ZstdTarFile",
    "open_tar_stream_reader",
    "open_tar_stream_writer",
]

_SPOOL_TAR_7Z: int = 32 * 1024 * 1024  # 32 MiB before spilling to disk for .tar.7z


class _SeekableZstdReader(io.RawIOBase):
    """Streaming Zstandard reader supporting forward C-level seeks and rewind on seekable files."""

    def __init__(self, raw: IO[bytes], dctx: Any) -> None:
        super().__init__()
        self._raw = raw
        self._dctx = dctx
        self._raw_start = raw.tell() if raw.seekable() else 0
        self._reader: Any = self._open_reader()

    def _open_reader(self) -> Any:
        try:
            return self._dctx.stream_reader(self._raw, read_across_frames=True, closefd=False)
        except TypeError:
            return self._dctx.stream_reader(self._raw)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return self._raw.seekable()

    def tell(self) -> int:
        return int(self._reader.tell())

    def read(self, size: int = -1) -> bytes:
        data: bytes = self._reader.read(size)
        return data

    def readinto(self, b: Any) -> int:
        n: int = self._reader.readinto(b)
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            target = self.tell() + offset
        elif whence == io.SEEK_SET:
            target = offset
        else:
            raise io.UnsupportedOperation("SEEK_END not supported on compressed Zstd stream")
        if target < 0:
            raise ValueError("Negative seek position")
        cur = self.tell()
        if target < cur:
            if not self._raw.seekable():
                raise io.UnsupportedOperation("Underlying stream is not seekable for backward seek")
            self._reader.close()
            self._raw.seek(self._raw_start)
            self._reader = self._open_reader()
        if target > self.tell():
            self._reader.seek(target)
        return self.tell()

    def close(self) -> None:
        if not self.closed:
            try:
                self._reader.close()
            finally:
                super().close()


class ZstdTarFile(tarfile.TarFile):
    """``tarfile.TarFile`` subclass backed by a multi-threaded Zstandard stream.

    Compatible with ``archive_utils.ZstdTarFile`` while using ``zstandard``
    (or ``pyzstd`` fallback if ``zstandard`` is unavailable) so callers can use
    random/streaming TarFile methods directly on ``.tar.zst`` files.
    """

    def __init__(
        self,
        name: StrPath | None = None,
        mode: str = "r",
        *,
        fileobj: IO[bytes] | None = None,
        level: int = 3,
        threads: int | None = None,
        zstd_dict: Any = None,
        **kwargs: Any,
    ) -> None:
        if name is None and fileobj is None:
            raise ValueError("Either name or fileobj must be provided")

        self._owns_raw = fileobj is None
        if fileobj is None:
            assert name is not None
            raw: IO[bytes] = open(name, "rb" if "r" in mode else "wb")
        else:
            raw = fileobj
        self._raw_stream: IO[bytes] = raw
        self._zstd_stream: Any = None

        try:
            zstd = _require_zstd()
            if "r" in mode:
                dctx = (
                    zstd.ZstdDecompressor(dict_data=zstd_dict)
                    if zstd_dict is not None
                    else zstd.ZstdDecompressor()
                )
                self._zstd_stream = _SeekableZstdReader(raw, dctx)
                super().__init__(fileobj=self._zstd_stream, mode="r", **kwargs)
            elif "w" in mode:
                workers = threads if threads is not None else (os.cpu_count() or 1)
                cctx = (
                    zstd.ZstdCompressor(level=level, threads=workers, write_checksum=True, dict_data=zstd_dict)
                    if zstd_dict is not None
                    else zstd.ZstdCompressor(level=level, threads=workers, write_checksum=True)
                )
                self._zstd_stream = cctx.stream_writer(raw, closefd=False)
                super().__init__(fileobj=self._zstd_stream, mode="w", **kwargs)
            else:
                raise ValueError(f"Unsupported ZstdTarFile mode: {mode!r}")
        except Exception:
            if self._zstd_stream is not None:
                self._zstd_stream.close()
            if self._owns_raw:
                raw.close()
            raise

    def close(self) -> None:
        try:
            super().close()
        finally:
            try:
                if self._zstd_stream is not None:
                    self._zstd_stream.close()
            finally:
                if self._owns_raw:
                    self._raw_stream.close()


@contextmanager
def open_tar_stream_reader(source: StrPath | IO[bytes], fmt: Format) -> Iterator[tarfile.TarFile]:
    """Open any TAR variant (plain or compressed) for single-pass streaming reads (``mode='r|'``)."""
    if fmt.container is not Container.TAR:
        raise UnsupportedFormatError(f"Format {fmt} is not a TAR container")

    with ExitStack() as stack:
        owns_file = isinstance(source, (str, os.PathLike))
        raw: IO[bytes] = (
            stack.enter_context(open(source, "rb"))
            if isinstance(source, (str, os.PathLike))
            else source
        )

        comp = fmt.compression
        if comp is None:
            tf = stack.enter_context(tarfile.open(fileobj=raw, mode="r|"))
            yield tf
            return

        if comp is Compression.SEVENZ:
            # 7z requires seekable archive access; extract single tar stream into spooled temp file
            py7zr = _require_py7zr()
            spool: Any = stack.enter_context(tempfile.SpooledTemporaryFile(max_size=_SPOOL_TAR_7Z, mode="w+b"))
            if owns_file:
                sz_src: Any = Path(source)  # type: ignore[arg-type]
            else:
                sz_src = raw if raw.seekable() else io.BytesIO(raw.read())
            with py7zr.SevenZipFile(sz_src, "r") as sz:
                files = [f for f in sz.list() if not f.is_directory]
                if len(files) != 1:
                    raise CorruptedArchiveError(
                        f"Expected exactly 1 tar member inside .tar.7z, found {len(files)}"
                    )
                with tempfile.TemporaryDirectory(prefix="arkit_tar7z_") as tmp_dir:
                    sz.extract(path=tmp_dir, targets=[files[0].filename])
                    extracted_tar = Path(tmp_dir) / files[0].filename
                    with extracted_tar.open("rb") as tar_fh:
                        while chunk := tar_fh.read(CHUNK_SIZE):
                            spool.write(chunk)
            spool.seek(0)
            tf = stack.enter_context(tarfile.open(fileobj=spool, mode="r|"))
            yield tf
            return

        if comp is Compression.ZST:
            zreader = stack.enter_context(open_compressed_reader(raw, "zst"))
            tf = stack.enter_context(tarfile.open(fileobj=zreader, mode="r|"))
            yield tf
            return

        creader = stack.enter_context(open_compressed_reader(raw, comp.value))
        tf = stack.enter_context(tarfile.open(fileobj=creader, mode="r|"))
        yield tf


@contextmanager
def open_tar_stream_writer(
    destination: StrPath | IO[bytes],
    fmt: Format,
    *,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
    tar_format: int = tarfile.PAX_FORMAT,
) -> Iterator[tarfile.TarFile]:
    """Open any TAR variant (plain or compressed) for single-pass streaming writes (``mode='w|'``)."""
    if fmt.container is not Container.TAR:
        raise UnsupportedFormatError(f"Format {fmt} is not a TAR container")

    with ExitStack() as stack:
        raw: IO[bytes] = (
            stack.enter_context(open(destination, "wb"))
            if isinstance(destination, (str, os.PathLike))
            else destination
        )

        comp = fmt.compression
        if comp is None:
            tf = stack.enter_context(tarfile.open(fileobj=raw, mode="w|", format=tar_format))
            yield tf
            return

        if comp is Compression.SEVENZ:
            py7zr = _require_py7zr()
            lvl = resolve_level("7z", level)
            with tempfile.TemporaryDirectory(prefix="arkit_tar7z_out_") as tmp_dir:
                tar_tmp = Path(tmp_dir) / "payload.tar"
                with tarfile.open(tar_tmp, mode="w", format=tar_format) as tf_inner:
                    yield tf_inner
                if raw.seekable():
                    with py7zr.SevenZipFile(
                        raw,
                        "w",
                        filters=[{"id": py7zr.FILTER_LZMA2, "preset": lvl}],
                    ) as sz:
                        sz.write(tar_tmp, arcname="payload.tar")
                else:
                    buf = io.BytesIO()
                    with py7zr.SevenZipFile(
                        buf,
                        "w",
                        filters=[{"id": py7zr.FILTER_LZMA2, "preset": lvl}],
                    ) as sz:
                        sz.write(tar_tmp, arcname="payload.tar")
                    raw.write(buf.getvalue())
            return

        cwriter = stack.enter_context(
            open_compressed_writer(
                raw,
                comp.value,
                level=level,
                threads=threads,
                deterministic=deterministic,
            )
        )
        tf = stack.enter_context(tarfile.open(fileobj=cwriter, mode="w|", format=tar_format))
        yield tf


# Expose zstd_reader re-export for legacy compatibility
_ = zstd_reader
