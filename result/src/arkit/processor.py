"""Streaming and atomic in-place / copy-on-write archive member processing.

Unifies ``archiveutils.ArchiveProcessor``, ``process_archive``,
``process_tar_archive``, and ``process_zip_archive`` while eliminating
unnecessary full-member allocations for skipped entries and supporting all
``arkit`` codecs (``tar.*``, ``zip``, ``whl``, ``7z``, and single-stream files).
"""

from __future__ import annotations

import inspect
import logging
import os
import shutil
import stat
import tarfile
import tempfile
import time
import zipfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypeAlias, overload

from .atomicio import atomic_open
from .codecs import (
    CHUNK_SIZE,
    BinaryIO,
    _require_py7zr,
    open_compressed_reader,
    open_compressed_writer,
    resolve_level,
)
from .exceptions import ArchiveInPlaceError, UnknownFormatError, UnsupportedFormatError
from .formats import Container, Format
from .models import FilterFunc, PathLike, StrPath
from .operations import (
    _derive_single_output_name,
    _resolve_container_level,
    _resolve_fmt,
    _zip_date_time,
    list_members,
)
from .tar_stream import open_tar_stream_reader, open_tar_stream_writer

logger = logging.getLogger("arkit.processor")

ProcessorFunc1: TypeAlias = Callable[[bytes], bytes]
ProcessorFunc2: TypeAlias = Callable[[bytes, str], bytes]
ProcessorFunc: TypeAlias = ProcessorFunc1 | ProcessorFunc2
StreamProcessorFunc: TypeAlias = Callable[[BinaryIO, BinaryIO, str], None]

__all__ = [
    "ArchiveProcessor",
    "FilterFunc",
    "PathLike",
    "ProcessorFunc",
    "StreamProcessorFunc",
    "process_archive",
    "process_tar_archive",
    "process_zip_archive",
]


def _accepts_two_args(func: ProcessorFunc) -> bool:
    """Inspect ``func`` to determine whether it accepts ``(data, filename)`` or ``(data,)``."""
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    return has_varargs or len(positional) >= 2


def _invoke_processor(func: ProcessorFunc, two_arg: bool, data: bytes, name: str) -> bytes:
    if two_arg:
        return func(data, name)  # type: ignore[call-arg]
    return func(data)  # type: ignore[call-arg]


class ArchiveProcessor:
    """Memory-efficient processor for transforming files inside archives atomically.

    Supports ``.zip``, ``.whl``, ``.tar``, ``.tar.gz``, ``.tar.bz2``, ``.tar.xz``,
    ``.tar.zst``, ``.tar.br``, ``.tar.lz4``, ``.tar.snappy``, ``.tar.7z``, ``.7z``,
    and standalone single-stream compressed files (``.gz``, ``.bz2``, ``.xz``,
    ``.zst``, ``.br``, ``.lz4``, ``.snappy``).
    """

    def __init__(self, archive_path: PathLike, *, fmt: Format | str | None = None) -> None:
        self.archive_path: Path = Path(archive_path)
        if not self.archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        try:
            self.format: Format = _resolve_fmt(self.archive_path, fmt)
        except Exception as exc:
            raise UnsupportedFormatError(f"Unsupported archive format: {self.archive_path.name}") from exc
        self.format_type: str = (
            "single" if self.format.container is None else self.format.container.value
        )
        self.compression: str | None = (
            self.format.compression.value if self.format.compression is not None else None
        )

    def list_files(self) -> list[str]:
        """List regular file paths inside the archive without loading member payloads."""
        if self.format.container is None:
            return [_derive_single_output_name(self.archive_path, self.format)]
        return [mi.name for mi in list_members(self.archive_path, self.format)]

    def process_files(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None = None,
        preserve_timestamps: bool = True,
        *,
        dst_path: StrPath | None = None,
        dst_format: Format | str | None = None,
        level: int | None = None,
        threads: int | None = None,
        on_error: Literal["warn", "raise", "skip"] = "warn",
    ) -> int:
        """Transform matching archive members and atomically write to ``dst_path`` (or in-place).

        Members excluded by ``filter_func`` are streamed directly in 1 MiB chunks
        without loading their full contents into memory.

        Returns the count of processed members.
        """
        two_arg = _accepts_two_args(processor_func)

        def _stream_adapter(reader: BinaryIO, writer: BinaryIO, name: str) -> None:
            raw = reader.read()
            transformed = _invoke_processor(processor_func, two_arg, raw, name)
            writer.write(transformed)

        return self.process_streams(
            _stream_adapter,
            filter_func=filter_func,
            preserve_timestamps=preserve_timestamps,
            dst_path=dst_path,
            dst_format=dst_format,
            level=level,
            threads=threads,
            on_error=on_error,
        )

    def process_streams(
        self,
        stream_func: StreamProcessorFunc,
        *,
        filter_func: FilterFunc | None = None,
        preserve_timestamps: bool = True,
        dst_path: StrPath | None = None,
        dst_format: Format | str | None = None,
        level: int | None = None,
        threads: int | None = None,
        on_error: Literal["warn", "raise", "skip"] = "warn",
    ) -> int:
        """Stream-transform matching archive members in bounded memory.

        ``stream_func(reader, writer, member_name)`` reads from ``reader`` and writes
        transformed bytes to ``writer``, enabling constant-memory transformations even
        on multi-gigabyte archive members.
        """
        target_path = Path(dst_path) if dst_path is not None else self.archive_path
        if dst_format is not None:
            out_fmt = _resolve_fmt(target_path, dst_format, for_output=True)
        elif dst_path is not None:
            try:
                out_fmt = _resolve_fmt(target_path, None, for_output=True)
            except UnknownFormatError:
                out_fmt = self.format
        else:
            out_fmt = self.format
        lvl = _resolve_container_level(out_fmt, level)

        if self.format.container is None:
            if out_fmt.container is not None:
                raise UnsupportedFormatError("Cannot stream-process single-file format directly into container")
            return self._process_single(
                stream_func,
                filter_func=filter_func,
                target_path=target_path,
                out_fmt=out_fmt,
                level=lvl,
                threads=threads,
                on_error=on_error,
            )

        if self.format.container is Container.ZIP and out_fmt.container is Container.ZIP:
            return self._process_zip(
                stream_func,
                filter_func=filter_func,
                preserve_timestamps=preserve_timestamps,
                target_path=target_path,
                level=lvl,
                on_error=on_error,
            )

        if self.format.container is Container.TAR and out_fmt.container is Container.TAR:
            return self._process_tar(
                stream_func,
                filter_func=filter_func,
                preserve_timestamps=preserve_timestamps,
                target_path=target_path,
                out_fmt=out_fmt,
                level=lvl,
                threads=threads,
                on_error=on_error,
            )

        if self.format.container is Container.SEVENZ and out_fmt.container is Container.SEVENZ:
            return self._process_7z(
                stream_func,
                filter_func=filter_func,
                target_path=target_path,
                level=lvl,
                on_error=on_error,
            )

        raise UnsupportedFormatError(
            f"Incompatible source/destination container formats: {self.format.format_id} -> {out_fmt.format_id}"
        )

    def _handle_member_error(
        self,
        name: str,
        exc: Exception,
        on_error: Literal["warn", "raise", "skip"],
    ) -> None:
        if on_error == "raise":
            raise ArchiveInPlaceError(f"Failed to process member {name!r}: {exc}") from exc
        if on_error == "warn":
            logger.warning("Failed to process %s: %s", name, exc)

    def _process_single(
        self,
        stream_func: StreamProcessorFunc,
        *,
        filter_func: FilterFunc | None,
        target_path: Path,
        out_fmt: Format,
        level: int,
        threads: int | None,
        on_error: Literal["warn", "raise", "skip"],
    ) -> int:
        assert self.format.compression is not None and out_fmt.compression is not None
        logical_name = _derive_single_output_name(self.archive_path, self.format)
        if filter_func is not None and not filter_func(logical_name):
            if target_path.resolve() != self.archive_path.resolve():
                with self.archive_path.open("rb") as fi, atomic_open(target_path, "wb") as fo:
                    shutil.copyfileobj(fi, fo, CHUNK_SIZE)
            return 0

        try:
            with self.archive_path.open("rb") as raw_in, open_compressed_reader(
                raw_in, self.format.compression.value
            ) as reader, atomic_open(target_path, "wb") as raw_out, open_compressed_writer(
                raw_out,
                out_fmt.compression.value,
                level=level,
                threads=threads,
            ) as writer:
                stream_func(reader, writer, logical_name)
            return 1
        except Exception as exc:
            self._handle_member_error(logical_name, exc, on_error)
            if target_path.resolve() != self.archive_path.resolve():
                with self.archive_path.open("rb") as fi, atomic_open(target_path, "wb") as fo:
                    shutil.copyfileobj(fi, fo, CHUNK_SIZE)
            return 0

    def _process_zip(
        self,
        stream_func: StreamProcessorFunc,
        *,
        filter_func: FilterFunc | None,
        preserve_timestamps: bool,
        target_path: Path,
        level: int,
        on_error: Literal["warn", "raise", "skip"],
    ) -> int:
        comp = zipfile.ZIP_STORED if level <= 0 else zipfile.ZIP_DEFLATED
        cl = level if comp == zipfile.ZIP_DEFLATED else None
        processed_count = 0

        with zipfile.ZipFile(self.archive_path, "r") as zin, atomic_open(target_path, "wb") as raw_out:
            with zipfile.ZipFile(raw_out, "w", compression=comp, compresslevel=cl, allowZip64=True) as zout:
                for item in zin.infolist():
                    if item.is_dir():
                        dir_zi = zipfile.ZipInfo(
                            filename=item.filename,
                            date_time=item.date_time if preserve_timestamps else _zip_date_time(None),
                        )
                        dir_zi.external_attr = item.external_attr
                        zout.writestr(dir_zi, b"")
                        continue

                    filename = item.filename
                    dt = item.date_time if preserve_timestamps else _zip_date_time(datetime.now(tz=timezone.utc))
                    new_zi = zipfile.ZipInfo(filename=filename, date_time=dt)
                    new_zi.compress_type = comp
                    new_zi.external_attr = item.external_attr or ((stat.S_IFREG | 0o644) << 16)

                    if filter_func is not None and not filter_func(filename):
                        # Stream-copy untouched member without buffering whole file in RAM!
                        with zin.open(item, "r") as s_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                            shutil.copyfileobj(s_fh, d_fh, CHUNK_SIZE)
                        continue

                    spool: Any = tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024, mode="w+b")
                    try:
                        try:
                            with zin.open(item, "r") as s_fh:
                                stream_func(s_fh, spool, filename)
                            spool.seek(0)
                            with zout.open(new_zi, "w", force_zip64=True) as d_fh:
                                shutil.copyfileobj(spool, d_fh, CHUNK_SIZE)
                            processed_count += 1
                        except Exception as exc:
                            self._handle_member_error(filename, exc, on_error)
                            with zin.open(item, "r") as s_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                                shutil.copyfileobj(s_fh, d_fh, CHUNK_SIZE)
                    finally:
                        spool.close()
        return processed_count

    def _process_tar(
        self,
        stream_func: StreamProcessorFunc,
        *,
        filter_func: FilterFunc | None,
        preserve_timestamps: bool,
        target_path: Path,
        out_fmt: Format,
        level: int,
        threads: int | None,
        on_error: Literal["warn", "raise", "skip"],
    ) -> int:
        processed_count = 0
        with open_tar_stream_reader(self.archive_path, self.format) as tin, atomic_open(
            target_path, "wb"
        ) as raw_out:
            with open_tar_stream_writer(raw_out, out_fmt, level=level, threads=threads) as tout:
                for member in tin:
                    if not member.isfile():
                        tout.addfile(member)
                        continue

                    ef = tin.extractfile(member)
                    if ef is None:
                        tout.addfile(member)
                        continue

                    if filter_func is not None and not filter_func(member.name):
                        # Stream untouched member straight through in chunks!
                        with ef:
                            copied = tarfile.TarInfo(name=member.name)
                            copied.size = member.size
                            copied.mode = member.mode
                            copied.mtime = member.mtime if preserve_timestamps else int(time.time())
                            copied.uid = member.uid
                            copied.gid = member.gid
                            copied.uname = member.uname
                            copied.gname = member.gname
                            tout.addfile(copied, ef)
                        continue

                    # Buffer original to spool only if fallback needed on error; tar requires size header up front
                    orig_spool: Any = tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024, mode="w+b")
                    out_spool: Any = tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024, mode="w+b")
                    try:
                        with ef:
                            shutil.copyfileobj(ef, orig_spool, CHUNK_SIZE)
                        orig_spool.seek(0)
                        try:
                            stream_func(orig_spool, out_spool, member.name)
                            new_size = out_spool.tell()
                            out_spool.seek(0)
                            payload_reader: Any = out_spool
                            processed_count += 1
                        except Exception as exc:
                            self._handle_member_error(member.name, exc, on_error)
                            orig_spool.seek(0, os.SEEK_END)
                            new_size = orig_spool.tell()
                            orig_spool.seek(0)
                            payload_reader = orig_spool

                        new_ti = tarfile.TarInfo(name=member.name)
                        new_ti.size = new_size
                        new_ti.mode = member.mode
                        new_ti.mtime = member.mtime if preserve_timestamps else int(time.time())
                        new_ti.uid = member.uid
                        new_ti.gid = member.gid
                        new_ti.uname = member.uname
                        new_ti.gname = member.gname
                        tout.addfile(new_ti, payload_reader)
                    finally:
                        orig_spool.close()
                        out_spool.close()
        return processed_count

    def _process_7z(
        self,
        stream_func: StreamProcessorFunc,
        *,
        filter_func: FilterFunc | None,
        target_path: Path,
        level: int,
        on_error: Literal["warn", "raise", "skip"],
    ) -> int:
        py7zr = _require_py7zr()
        processed_count = 0
        lvl = resolve_level("7z", level)
        filters = [{"id": py7zr.FILTER_LZMA2, "preset": lvl}]

        with tempfile.TemporaryDirectory(prefix="arkit_proc7z_") as tmp_dir:
            staging = Path(tmp_dir)
            with py7zr.SevenZipFile(self.archive_path, "r") as src_7z:
                names = [fi.filename for fi in src_7z.list() if not fi.is_directory]
                src_7z.extractall(path=staging)

            for rel_name in names:
                fpath = staging / rel_name
                if not fpath.is_file():
                    continue
                if filter_func is not None and not filter_func(rel_name):
                    continue
                tmp_out = fpath.with_suffix(fpath.suffix + ".arkit_proc")
                try:
                    with fpath.open("rb") as r_fh, tmp_out.open("wb") as w_fh:
                        stream_func(r_fh, w_fh, rel_name)
                    os.replace(tmp_out, fpath)
                    processed_count += 1
                except Exception as exc:
                    tmp_out.unlink(missing_ok=True)
                    self._handle_member_error(rel_name, exc, on_error)

            with atomic_open(target_path, "wb") as raw_out:
                with py7zr.SevenZipFile(raw_out, "w", filters=filters) as dst_7z:
                    for rel_name in names:
                        fpath = staging / rel_name
                        if fpath.is_file():
                            dst_7z.write(fpath, arcname=rel_name)
        return processed_count


def process_tar_archive(
    src_path: StrPath,
    process_func: ProcessorFunc,
    dst_path: StrPath | None = None,
    *,
    filter_func: FilterFunc | None = None,
    preserve_timestamps: bool = True,
) -> Path:
    """Process all matching members of a TAR (or ``.tar.*``) archive into ``dst_path``."""
    out = Path(dst_path) if dst_path is not None else Path(f"{os.fspath(src_path)}.processed")
    proc = ArchiveProcessor(src_path)
    if proc.format.container is not Container.TAR:
        raise UnsupportedFormatError(f"Expected TAR archive, got {proc.format.format_id}")
    proc.process_files(
        process_func,
        filter_func=filter_func,
        preserve_timestamps=preserve_timestamps,
        dst_path=out,
        on_error="raise",
    )
    return out


def process_zip_archive(
    src_path: StrPath,
    process_func: ProcessorFunc,
    dst_path: StrPath | None = None,
    *,
    filter_func: FilterFunc | None = None,
    preserve_timestamps: bool = True,
) -> Path:
    """Process all matching members of a ZIP / WHL archive into ``dst_path``."""
    out = Path(dst_path) if dst_path is not None else Path(f"{os.fspath(src_path)}.processed")
    proc = ArchiveProcessor(src_path)
    if proc.format.container is not Container.ZIP:
        raise UnsupportedFormatError(f"Expected ZIP/WHL archive, got {proc.format.format_id}")
    proc.process_files(
        process_func,
        filter_func=filter_func,
        preserve_timestamps=preserve_timestamps,
        dst_path=out,
        on_error="raise",
    )
    return out


@overload
def process_archive(
    archive_path: StrPath,
    processor_func: ProcessorFunc,
    dst_or_filter: StrPath,
    preserve_timestamps: bool = True,
    *,
    filter_func: FilterFunc | None = None,
    on_error: Literal["warn", "raise", "skip"] = "warn",
) -> Path: ...


@overload
def process_archive(
    archive_path: StrPath,
    processor_func: ProcessorFunc,
    dst_or_filter: FilterFunc | None = None,
    preserve_timestamps: bool = True,
    *,
    dst_path: None = None,
    on_error: Literal["warn", "raise", "skip"] = "warn",
) -> int: ...


@overload
def process_archive(
    archive_path: StrPath,
    processor_func: ProcessorFunc,
    dst_or_filter: FilterFunc | None = None,
    preserve_timestamps: bool = True,
    *,
    dst_path: StrPath,
    on_error: Literal["warn", "raise", "skip"] = "warn",
) -> Path: ...


def process_archive(
    archive_path: StrPath,
    processor_func: ProcessorFunc,
    dst_or_filter: StrPath | FilterFunc | None = None,
    preserve_timestamps: bool = True,
    *,
    dst_path: StrPath | None = None,
    filter_func: FilterFunc | None = None,
    on_error: Literal["warn", "raise", "skip"] = "warn",
) -> Path | int:
    """Universal archive member processor supporting both copy-to-destination and in-place modes.

    - **Destination mode**: ``process_archive("in.tar.gz", fn, "out.tar.gz")`` or
      ``process_archive("in.tar.gz", fn, dst_path="out.tar.gz")`` writes a new
      archive and returns ``Path(dst_path)``.
    - **In-place atomic mode**: ``process_archive("in.tar.gz", fn, filter_func)``
      atomically updates ``archive_path`` in-place and returns the ``int`` count
      of processed members.

    ``processor_func`` may take either ``(data: bytes) -> bytes`` or
    ``(data: bytes, member_name: str) -> bytes``.
    """
    resolved_dst: StrPath | None = dst_path
    resolved_filter: FilterFunc | None = filter_func

    if dst_or_filter is not None:
        if isinstance(dst_or_filter, (str, os.PathLike)):
            resolved_dst = dst_or_filter
        elif callable(dst_or_filter):
            resolved_filter = dst_or_filter
        else:
            raise TypeError("Third argument must be either a destination path or a filter_func callable")

    proc = ArchiveProcessor(archive_path)
    count = proc.process_files(
        processor_func,
        filter_func=resolved_filter,
        preserve_timestamps=preserve_timestamps,
        dst_path=resolved_dst,
        on_error=on_error,
    )
    if resolved_dst is not None:
        return Path(resolved_dst)
    return count
