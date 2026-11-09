"""Universal streaming archive and compression operations.

Provides functional APIs covering compression, decompression, safe extraction,
archive packing, member inspection, streaming validation, cross-format conversion,
in-place member addition/removal, archive diffing, and cryptographic checksums.
"""

from __future__ import annotations

import contextlib
import fnmatch
import hashlib
import importlib
import io
import os
import shutil
import stat
import tarfile
import tempfile
import time
import zipfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, overload

from .atomicio import atomic_open
from .codecs import (
    CHUNK_SIZE,
    BinaryIO,
    _require_py7zr,
    compress_bytes_raw,
    copy_stream,
    decompress_bytes_raw,
    open_compressed_reader,
    open_compressed_writer,
    resolve_level,
)
from .codecs import compress_stream as _codec_compress_stream
from .codecs import decompress_stream as _codec_decompress_stream
from .exceptions import (
    ArchiveError,
    CorruptedArchiveError,
    PasswordProtectedError,
    UnknownFormatError,
    UnsupportedFormatError,
)
from .formats import (
    Compression,
    Container,
    Format,
    detect,
    detect_file,
    detect_format,
    guess_format,
    magic_ok,
)
from .models import (
    ArchiveInfo,
    CompressionStats,
    DiffReport,
    FilterFunc,
    HealthReport,
    Member,
    MemberInfo,
    MembersInput,
    ProgressCallback,
    SafetyPolicy,
    StrPath,
)
from .parallel_brotli import ParallelBrotliCompressor
from .safety import DEFAULT_SAFETY_POLICY, ExtractionGuard, ensure_parent, safe_destination
from .tar_stream import open_tar_stream_reader, open_tar_stream_writer

__all__ = [
    "add_member",
    "check_integrity",
    "checksum",
    "compress",
    "compress_bytes",
    "compress_file",
    "compress_stream_format",
    "compression_stats",
    "convert",
    "convert_bytes",
    "convert_file",
    "convert_stream",
    "decompress",
    "decompress_bytes",
    "decompress_file",
    "decompress_stream_format",
    "diff_archives",
    "extract",
    "extract_stream",
    "info",
    "iter_members",
    "list_members",
    "pack",
    "pack_file",
    "pack_stream",
    "read_member",
    "read_members",
    "remove_member",
    "validate",
    "validate_bytes",
    "validate_file",
    "validate_stream",
    "verify_archive",
]

_PAYLOAD_NAME: str = "data"
_SPOOL_LIMIT: int = 32 * 1024 * 1024  # 32 MiB before spilling to temp disk
_REPRODUCIBLE_DOS_TIME: tuple[int, int, int, int, int, int] = (1980, 1, 1, 0, 0, 0)


class _CountingSink(io.RawIOBase):
    """Zero-memory write sink that counts bytes written (used for validation & sizing)."""

    def __init__(self) -> None:
        super().__init__()
        self.size: int = 0

    def writable(self) -> bool:
        return True

    def write(self, b: Any) -> int:
        n = len(b)
        self.size += n
        return n


# ---------------------------------------------------------------------------
# Helpers: Format Resolution, Path Filtering, Timestamps, Sizing
# ---------------------------------------------------------------------------


def _resolve_fmt(
    source: StrPath | bytes | None,
    fmt: Format | str | None,
    *,
    for_output: bool = False,
) -> Format:
    if fmt is not None:
        return Format.parse(fmt)
    if source is None:
        raise UnknownFormatError("Unable to determine format; pass format explicitly")
    if isinstance(source, bytes):
        detected = detect(source, deep=True)
        if detected is None:
            raise UnknownFormatError("Unable to determine format from byte buffer")
        return detected

    path = Path(source)
    if for_output:
        guessed = guess_format(path)
        if guessed is not None:
            return guessed
        af = detect_format(path, allow_magic=False)
        if af.format_enum is not None:
            return af.format_enum
        raise UnknownFormatError(f"Unsupported output format for path: {path}")

    guessed = guess_format(path)
    if guessed is not None:
        if guessed.container is None and path.is_file():
            deeper = detect_file(path, deep=True)
            if deeper is not None and deeper.container is not None:
                return deeper
        return guessed

    if path.is_file():
        detected_file = detect_file(path, deep=True)
        if detected_file is not None:
            return detected_file

    af = detect_format(path, allow_magic=True)
    if af.format_enum is not None:
        return af.format_enum
    raise UnknownFormatError(f"Unsupported archive format: {path}")


def _resolve_container_level(fmt: Format, level: int | None) -> int:
    if fmt.compression is not None:
        return resolve_level(fmt.compression.value, level)
    if fmt.container is Container.ZIP:
        if level is None:
            return 6
        if not 0 <= level <= 9:
            raise ValueError(f"ZIP compression level {level} outside [0, 9]")
        return level
    if fmt.container is Container.SEVENZ:
        return resolve_level("7z", level)
    if level is not None and level != 0:
        raise ValueError(f"Uncompressed TAR does not accept compression level {level}")
    return 0


def _matches_filters(
    rel_posix: str,
    *,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
) -> bool:
    name_only = PurePosixName(rel_posix)
    if exclude:
        for pat in exclude:
            if (
                fnmatch.fnmatch(rel_posix, pat)
                or fnmatch.fnmatch(name_only, pat)
                or any(fnmatch.fnmatch(part, pat) for part in rel_posix.split("/"))
            ):
                return False
    if include:
        matched = any(
            fnmatch.fnmatch(rel_posix, pat) or fnmatch.fnmatch(name_only, pat)
            for pat in include
        )
        if not matched:
            return False
    if filter_func is not None and not filter_func(rel_posix):
        return False
    return True


def PurePosixName(rel_posix: str) -> str:
    return rel_posix.rsplit("/", 1)[-1]


def _iter_source_files(
    source: Path,
    *,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
) -> Iterator[tuple[Path, str]]:
    """Yield ``(real_file_path, archive_posix_name)`` in deterministic sorted order."""
    if source.is_file() or (source.is_symlink() and follow_symlinks and source.resolve().is_file()):
        arcname = source.name
        if _matches_filters(arcname, include=include, exclude=exclude, filter_func=filter_func):
            yield source, arcname
        return

    if not source.is_dir():
        raise FileNotFoundError(f"Source path does not exist or is not a regular file/directory: {source}")

    base = source.parent if include_root else source
    for root_str, dirs, files in os.walk(source, followlinks=follow_symlinks):
        dirs.sort()
        files.sort()
        root_path = Path(root_str)
        if not follow_symlinks:
            dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
        if exclude:
            dirs[:] = [
                d
                for d in dirs
                if not any(fnmatch.fnmatch(d, pat) for pat in exclude)
            ]
        for fname in files:
            fpath = root_path / fname
            if not follow_symlinks and fpath.is_symlink():
                continue
            if not fpath.is_file():
                continue
            rel = fpath.relative_to(base).as_posix()
            if _matches_filters(rel, include=include, exclude=exclude, filter_func=filter_func):
                yield fpath, rel


def _compute_source_bytes(
    source: Path,
    *,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
) -> int:
    if source.is_file():
        return source.stat().st_size
    total = 0
    for fpath, _arc in _iter_source_files(
        source,
        follow_symlinks=follow_symlinks,
        include=include,
        exclude=exclude,
        filter_func=filter_func,
    ):
        with contextlib.suppress(OSError):
            total += fpath.stat().st_size
    return total


def _deterministic_epoch() -> int:
    env_epoch = os.environ.get("SOURCE_DATE_EPOCH")
    if env_epoch is not None:
        with contextlib.suppress(ValueError):
            return max(0, int(env_epoch))
    return 0


def _zip_date_time(mtime: datetime | float | None, *, deterministic: bool = False) -> tuple[int, int, int, int, int, int]:
    if deterministic:
        epoch = _deterministic_epoch()
        if epoch == 0:
            return _REPRODUCIBLE_DOS_TIME
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    elif isinstance(mtime, datetime):
        dt = mtime
    elif isinstance(mtime, (int, float)):
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    else:
        dt = datetime.now(tz=timezone.utc)
    tt = dt.timetuple()
    return (max(tt.tm_year, 1980), tt.tm_mon, tt.tm_mday, tt.tm_hour, tt.tm_min, tt.tm_sec)


def _tar_mtime_dt(ti: tarfile.TarInfo) -> datetime:
    try:
        return datetime.fromtimestamp(int(ti.mtime), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return datetime.fromtimestamp(0, tz=timezone.utc)


def _derive_single_output_name(archive_path: Path, fmt: Format) -> str:
    lower = archive_path.name.lower()
    for suf in fmt.suffixes:
        if lower.endswith(suf) and len(archive_path.name) > len(suf):
            return archive_path.name[: -len(suf)]
    if archive_path.suffix:
        stripped = archive_path.name[: -len(archive_path.suffix)]
        if stripped:
            return stripped
    return f"{archive_path.name}.out"


# ---------------------------------------------------------------------------
# Checksum & CompressionStats
# ---------------------------------------------------------------------------


def checksum(source: StrPath | bytes, algorithm: str = "sha256") -> str:
    """Compute hex cryptographic digest of a file or byte buffer in bounded 1 MiB chunks."""
    hasher = hashlib.new(algorithm)
    if isinstance(source, bytes):
        hasher.update(source)
        return hasher.hexdigest()
    with Path(source).open("rb") as fh:
        while chunk := fh.read(CHUNK_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def compression_stats(
    source: StrPath | int,
    result: StrPath | int,
    *,
    elapsed_seconds: float = 0.0,
    sha256: str | None = None,
    compute_sha256: bool = False,
) -> CompressionStats:
    """Calculate :class:`~arkit.models.CompressionStats` between ``source`` and ``result``.

    Correctly sums all regular file sizes when ``source`` or ``result`` is a directory
    (fixing the directory inode size pitfall).
    """
    source_bytes = source if isinstance(source, int) else _compute_source_bytes(Path(source))
    result_bytes = result if isinstance(result, int) else _compute_source_bytes(Path(result))
    if sha256 is None and compute_sha256 and not isinstance(result, int):
        res_path = Path(result)
        if res_path.is_file():
            sha256 = checksum(res_path, "sha256")
    ratio = (result_bytes / source_bytes) if source_bytes > 0 else 0.0
    space_freed = source_bytes - result_bytes
    space_change_percent = (1.0 - ratio) * 100.0 if source_bytes > 0 else 0.0
    return CompressionStats(
        source_bytes=source_bytes,
        result_bytes=result_bytes,
        ratio=ratio,
        space_freed=space_freed,
        space_change_percent=space_change_percent,
        elapsed_seconds=elapsed_seconds,
        sha256=sha256,
    )


# ---------------------------------------------------------------------------
# Byte & Stream Operations (arutils + codecs unified)
# ---------------------------------------------------------------------------


def compress_bytes(
    data: bytes,
    fmt: Format | str,
    *,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes:
    """Compress raw ``data`` in memory using ``fmt`` (stream codec or single-entry container)."""
    resolved = Format.parse(fmt)
    lvl = _resolve_container_level(resolved, level)
    if resolved.container is None:
        assert resolved.compression is not None
        return compress_bytes_raw(
            data,
            resolved.compression.value,
            level=lvl,
            threads=threads,
            deterministic=deterministic,
        )
    if resolved.container is Container.TAR:
        # Note: matches arutils.compress(tar_bytes, Format.TAR_ZST) where `data` is raw payload
        # encoded by outer compression if already tar, OR packs single payload if not tar!
        if len(data) >= 262 and data[257:262] == b"ustar":
            if resolved.compression is None:
                return data
            return compress_bytes_raw(
                data,
                resolved.compression.value,
                level=lvl,
                threads=threads,
                deterministic=deterministic,
            )
        return pack({_PAYLOAD_NAME: data}, resolved, level=lvl, threads=threads, deterministic=deterministic)
    return pack({_PAYLOAD_NAME: data}, resolved, level=lvl, threads=threads, deterministic=deterministic)


def decompress_bytes(data: bytes, fmt: Format | str | None = None) -> bytes:
    """Decompress ``data`` in memory (auto-detecting format from magic bytes when ``fmt`` is ``None``)."""
    resolved = _resolve_fmt(data, fmt)
    if resolved.container is None:
        assert resolved.compression is not None
        return decompress_bytes_raw(data, resolved.compression.value)
    if resolved.container is Container.TAR:
        raw_tar = (
            data
            if resolved.compression is None
            else decompress_bytes_raw(data, resolved.compression.value)
        )
        # If caller expects raw tar stream (arutils compatibility) vs single member:
        with tarfile.open(fileobj=io.BytesIO(raw_tar), mode="r:") as tf:
            files = [m for m in tf.getmembers() if m.isfile()]
            if len(files) == 1 and files[0].name == _PAYLOAD_NAME:
                ef = tf.extractfile(files[0])
                return ef.read() if ef is not None else b""
        return raw_tar
    members = _read_members_bytes(resolved, data)
    if len(members) != 1:
        raise ArchiveError(f"Expected single-member archive for byte decompression, found {len(members)}")
    return members[0].data


def compress_stream_format(
    src: BinaryIO,
    dst: BinaryIO,
    fmt: Format | str,
    level: int | None = None,
    chunk: int = CHUNK_SIZE,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> int:
    """Stream-compress ``src`` into ``dst`` according to :class:`Format` or codec string."""
    resolved = Format.parse(fmt)
    lvl = _resolve_container_level(resolved, level)
    if resolved.container is None:
        assert resolved.compression is not None
        if resolved.compression is Compression.PBRT:
            pbc = ParallelBrotliCompressor(quality=lvl, workers=threads)
            if dst.seekable():
                return pbc.compress_stream(src, dst)
            spool: Any = tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b")
            try:
                total = pbc.compress_stream(src, spool)
                spool.seek(0)
                shutil.copyfileobj(spool, dst, chunk)
                return total
            finally:
                spool.close()
        return _codec_compress_stream(
            src,
            dst,
            resolved.compression.value,
            level=lvl,
            threads=threads,
            chunk_size=chunk,
            deterministic=deterministic,
        )
    if resolved.container is Container.TAR:
        if resolved.compression is None:
            return copy_stream(src, dst, chunk)
        return _codec_compress_stream(
            src,
            dst,
            resolved.compression.value,
            level=lvl,
            threads=threads,
            chunk_size=chunk,
            deterministic=deterministic,
        )
    if resolved.container is Container.ZIP:
        return _zip_stream_single_compress(src, dst, chunk, lvl, deterministic=deterministic)
    if resolved.container is Container.SEVENZ:
        return _codec_compress_stream(src, dst, "7z", level=lvl, chunk_size=chunk)
    raise UnsupportedFormatError(f"Unsupported format: {resolved}")


def decompress_stream_format(
    src: BinaryIO,
    dst: BinaryIO,
    fmt: Format | str | None = None,
    chunk: int = CHUNK_SIZE,
) -> int:
    """Stream-decompress ``src`` into ``dst`` (auto-sniffing magic bytes when ``fmt`` is ``None``)."""
    owned_spool: Any = None
    try:
        if fmt is None:
            if not src.seekable():
                owned_spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b")
                shutil.copyfileobj(src, owned_spool, chunk)
                owned_spool.seek(0)
                src = owned_spool
            pos = src.tell()
            head = src.read(65536)
            src.seek(pos)
            detected = detect(head, deep=False)
            if detected is None:
                raise UnknownFormatError("Unable to determine format from stream")
            resolved = detected
        else:
            resolved = Format.parse(fmt)

        if resolved.container is None:
            assert resolved.compression is not None
            return _codec_decompress_stream(src, dst, resolved.compression.value, chunk_size=chunk)
        if resolved.container is Container.TAR:
            if resolved.compression is None:
                return copy_stream(src, dst, chunk)
            return _codec_decompress_stream(src, dst, resolved.compression.value, chunk_size=chunk)
        if resolved.container is Container.ZIP:
            if not src.seekable():
                owned_spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b")
                shutil.copyfileobj(src, owned_spool, chunk)
                owned_spool.seek(0)
                src = owned_spool
            with zipfile.ZipFile(src, "r") as zf:
                files = [zi for zi in zf.infolist() if not zi.is_dir()]
                if len(files) != 1:
                    raise ArchiveError(f"Expected 1 member in streamed ZIP, found {len(files)}")
                with zf.open(files[0], "r") as member_fh:
                    return copy_stream(member_fh, dst, chunk)
        if resolved.container is Container.SEVENZ:
            return _codec_decompress_stream(src, dst, "7z", chunk_size=chunk)
        raise UnsupportedFormatError(f"Unsupported format: {resolved}")
    finally:
        if owned_spool is not None:
            owned_spool.close()


def _zip_stream_single_compress(
    src: BinaryIO,
    dst: BinaryIO,
    chunk: int,
    level: int,
    *,
    deterministic: bool = False,
) -> int:
    def _write_into(seekable_out: BinaryIO) -> int:
        comp = zipfile.ZIP_STORED if level <= 0 else zipfile.ZIP_DEFLATED
        cl = level if comp == zipfile.ZIP_DEFLATED else None
        total = 0
        with zipfile.ZipFile(seekable_out, "w", compression=comp, compresslevel=cl, allowZip64=True) as zf:
            zi = zipfile.ZipInfo(_PAYLOAD_NAME, date_time=_zip_date_time(None, deterministic=deterministic))
            zi.external_attr = 0o644 << 16
            with zf.open(zi, "w", force_zip64=True) as fh:
                while block := src.read(chunk):
                    fh.write(block)
                    total += len(block)
        return total

    if dst.seekable():
        return _write_into(dst)
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b") as spool:
        total = _write_into(cast(BinaryIO, spool))
        spool.seek(0)
        shutil.copyfileobj(spool, dst, chunk)
        return total


# ---------------------------------------------------------------------------
# Packing & High-Level Compress
# ---------------------------------------------------------------------------


def _write_tar_from_input(
    tf: tarfile.TarFile,
    source: MembersInput,
    *,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    written_bytes = 0
    seen_names: set[str] = set()

    if isinstance(source, (str, os.PathLike)):
        src_path = Path(source)
        for fpath, arcname in _iter_source_files(
            src_path,
            include_root=include_root,
            follow_symlinks=follow_symlinks,
            include=include,
            exclude=exclude,
            filter_func=filter_func,
        ):
            st = fpath.stat()
            ti = tarfile.TarInfo(name=arcname)
            ti.size = st.st_size
            if deterministic:
                ti.mtime = _deterministic_epoch()
                ti.mode = 0o755 if (st.st_mode & stat.S_IXUSR) else 0o644
                ti.uid = 0
                ti.gid = 0
                ti.uname = ""
                ti.gname = ""
            else:
                ti.mtime = int(st.st_mtime)
                ti.mode = stat.S_IMODE(st.st_mode) or 0o644
            with fpath.open("rb") as fh:
                tf.addfile(ti, fh)
            written_bytes += st.st_size
            if progress_callback is not None:
                progress_callback(written_bytes, total_hint)
        return written_bytes

    if isinstance(source, Mapping):
        items = sorted(source.items(), key=lambda kv: str(kv[0])) if deterministic else list(source.items())
        for raw_name, payload in items:
            name = str(raw_name)
            if name in seen_names:
                raise ArchiveError(f"Duplicate member name: {name!r}")
            seen_names.add(name)
            if not _matches_filters(name, include=include, exclude=exclude, filter_func=filter_func):
                continue
            ti = tarfile.TarInfo(name=name)
            ti.size = len(payload)
            ti.mode = 0o644
            ti.mtime = _deterministic_epoch() if deterministic else int(time.time())
            tf.addfile(ti, io.BytesIO(payload))
            written_bytes += len(payload)
            if progress_callback is not None:
                progress_callback(written_bytes, total_hint)
        return written_bytes

    members_list = list(source)
    if deterministic:
        members_list.sort(key=lambda m: m.info.name)
    for m in members_list:
        if m.info.name in seen_names:
            raise ArchiveError(f"Duplicate member name: {m.info.name!r}")
        seen_names.add(m.info.name)
        if not _matches_filters(m.info.name, include=include, exclude=exclude, filter_func=filter_func):
            continue
        ti = tarfile.TarInfo(name=m.info.name)
        ti.size = len(m.data)
        ti.mode = m.info.mode if (m.info.mode and not deterministic) else 0o644
        if deterministic:
            ti.mtime = _deterministic_epoch()
        elif m.info.mtime is not None:
            ti.mtime = int(m.info.mtime.timestamp())
        else:
            ti.mtime = int(time.time())
        tf.addfile(ti, io.BytesIO(m.data))
        written_bytes += len(m.data)
        if progress_callback is not None:
            progress_callback(written_bytes, total_hint)
    return written_bytes


def _write_zip_from_input(
    out_stream: BinaryIO,
    source: MembersInput,
    *,
    level: int,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    comp = zipfile.ZIP_STORED if level <= 0 else zipfile.ZIP_DEFLATED
    cl = level if comp == zipfile.ZIP_DEFLATED else None
    written_bytes = 0
    seen_names: set[str] = set()

    with zipfile.ZipFile(out_stream, "w", compression=comp, compresslevel=cl, allowZip64=True) as zf:
        if isinstance(source, (str, os.PathLike)):
            src_path = Path(source)
            for fpath, arcname in _iter_source_files(
                src_path,
                include_root=include_root,
                follow_symlinks=follow_symlinks,
                include=include,
                exclude=exclude,
                filter_func=filter_func,
            ):
                st = fpath.stat()
                dt = _zip_date_time(st.st_mtime, deterministic=deterministic)
                zi = zipfile.ZipInfo(filename=arcname, date_time=dt)
                zi.compress_type = comp
                mode = (0o755 if (st.st_mode & stat.S_IXUSR) else 0o644) if deterministic else (stat.S_IMODE(st.st_mode) or 0o644)
                zi.external_attr = (stat.S_IFREG | mode) << 16
                with fpath.open("rb") as src_fh, zf.open(zi, "w", force_zip64=(st.st_size >= (1 << 31))) as dst_fh:
                    while chunk := src_fh.read(CHUNK_SIZE):
                        dst_fh.write(chunk)
                        written_bytes += len(chunk)
                        if progress_callback is not None:
                            progress_callback(written_bytes, total_hint)
            return written_bytes

        if isinstance(source, Mapping):
            items = sorted(source.items(), key=lambda kv: str(kv[0])) if deterministic else list(source.items())
            for raw_name, payload in items:
                name = str(raw_name)
                if name in seen_names:
                    raise ArchiveError(f"Duplicate member name: {name!r}")
                seen_names.add(name)
                if not _matches_filters(name, include=include, exclude=exclude, filter_func=filter_func):
                    continue
                zi = zipfile.ZipInfo(filename=name, date_time=_zip_date_time(None, deterministic=deterministic))
                zi.compress_type = comp
                zi.external_attr = (stat.S_IFREG | 0o644) << 16
                zf.writestr(zi, payload, compress_type=comp, compresslevel=cl)
                written_bytes += len(payload)
                if progress_callback is not None:
                    progress_callback(written_bytes, total_hint)
            return written_bytes

        members_list = list(source)
        if deterministic:
            members_list.sort(key=lambda m: m.info.name)
        for m in members_list:
            if m.info.name in seen_names:
                raise ArchiveError(f"Duplicate member name: {m.info.name!r}")
            seen_names.add(m.info.name)
            if not _matches_filters(m.info.name, include=include, exclude=exclude, filter_func=filter_func):
                continue
            zi = zipfile.ZipInfo(
                filename=m.info.name,
                date_time=_zip_date_time(m.info.mtime, deterministic=deterministic),
            )
            zi.compress_type = comp
            mode = m.info.mode if (m.info.mode and not deterministic) else 0o644
            zi.external_attr = (stat.S_IFREG | mode) << 16
            zf.writestr(zi, m.data, compress_type=comp, compresslevel=cl)
            written_bytes += len(m.data)
            if progress_callback is not None:
                progress_callback(written_bytes, total_hint)
        return written_bytes


def _write_7z_from_input(
    out_stream: BinaryIO,
    source: MembersInput,
    *,
    level: int,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> int:
    py7zr = _require_py7zr()
    written_bytes = 0
    seen_names: set[str] = set()
    filters = [{"id": py7zr.FILTER_LZMA2, "preset": level}]

    def _populate(sz: Any) -> None:
        nonlocal written_bytes
        if isinstance(source, (str, os.PathLike)):
            src_path = Path(source)
            for fpath, arcname in _iter_source_files(
                src_path,
                include_root=include_root,
                follow_symlinks=follow_symlinks,
                include=include,
                exclude=exclude,
                filter_func=filter_func,
            ):
                sz.write(fpath, arcname=arcname)
                written_bytes += fpath.stat().st_size
                if progress_callback is not None:
                    progress_callback(written_bytes, total_hint)
            return

        if isinstance(source, Mapping):
            items = sorted(source.items(), key=lambda kv: str(kv[0])) if deterministic else list(source.items())
            for raw_name, payload in items:
                name = str(raw_name)
                if name in seen_names:
                    raise ArchiveError(f"Duplicate member name: {name!r}")
                seen_names.add(name)
                if not _matches_filters(name, include=include, exclude=exclude, filter_func=filter_func):
                    continue
                sz.writestr(payload, name)
                written_bytes += len(payload)
                if progress_callback is not None:
                    progress_callback(written_bytes, total_hint)
            return

        members_list = list(source)
        if deterministic:
            members_list.sort(key=lambda m: m.info.name)
        for m in members_list:
            if m.info.name in seen_names:
                raise ArchiveError(f"Duplicate member name: {m.info.name!r}")
            seen_names.add(m.info.name)
            if not _matches_filters(m.info.name, include=include, exclude=exclude, filter_func=filter_func):
                continue
            sz.writestr(m.data, m.info.name)
            written_bytes += len(m.data)
            if progress_callback is not None:
                progress_callback(written_bytes, total_hint)

    if out_stream.seekable():
        with py7zr.SevenZipFile(out_stream, "w", filters=filters) as sz:
            _populate(sz)
    else:
        with tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b") as spool:
            with py7zr.SevenZipFile(spool, "w", filters=filters) as sz:
                _populate(sz)
            spool.seek(0)
            shutil.copyfileobj(spool, out_stream, CHUNK_SIZE)
    return written_bytes


def pack_file(
    dst: StrPath | BinaryIO,
    source: MembersInput,
    fmt: Format | str | None = None,
    level: int | None = None,
    chunk: int = CHUNK_SIZE,
    *,
    threads: int | None = None,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    progress_callback: ProgressCallback | None = None,
    total_hint: int | None = None,
) -> Path | None:
    """Pack ``source`` (file, directory, ``Mapping[str, bytes]``, or ``Iterable[Member]``) into ``dst``."""
    dst_path = Path(dst) if isinstance(dst, (str, os.PathLike)) else None
    resolved_fmt = _resolve_fmt(dst_path, fmt, for_output=True)
    lvl = _resolve_container_level(resolved_fmt, level)

    def _write_to_stream(out_stream: BinaryIO) -> int:
        if resolved_fmt.container is None:
            assert resolved_fmt.compression is not None
            codec_name = resolved_fmt.compression.value
            if isinstance(source, (str, os.PathLike)):
                src_p = Path(source)
                if not src_p.is_file():
                    raise ValueError(
                        f"{resolved_fmt.format_id} is a single-file stream format; source must be a regular file."
                    )
                size_hint = total_hint if total_hint is not None else src_p.stat().st_size
                if resolved_fmt.compression is Compression.PBRT:
                    pbc = ParallelBrotliCompressor(
                        quality=lvl,
                        workers=threads,
                        progress_callback=progress_callback,
                    )
                    with src_p.open("rb") as fi:
                        return pbc.compress_stream(fi, out_stream, total_size=size_hint)
                with src_p.open("rb") as fi:
                    return _codec_compress_stream(
                        fi,
                        out_stream,
                        codec_name,
                        level=lvl,
                        threads=threads,
                        chunk_size=chunk,
                        deterministic=deterministic,
                        progress_callback=progress_callback,
                        total_hint=size_hint,
                    )
            if isinstance(source, Mapping):
                if len(source) != 1:
                    raise ArchiveError("Single-stream compression formats hold exactly one payload")
                (payload,) = source.values()
                return compress_stream_format(
                    io.BytesIO(payload),
                    out_stream,
                    resolved_fmt,
                    level=lvl,
                    chunk=chunk,
                    threads=threads,
                    deterministic=deterministic,
                )
            members_seq = list(source)
            if len(members_seq) != 1:
                raise ArchiveError("Single-stream compression formats hold exactly one payload")
            return compress_stream_format(
                io.BytesIO(members_seq[0].data),
                out_stream,
                resolved_fmt,
                level=lvl,
                chunk=chunk,
                threads=threads,
                deterministic=deterministic,
            )

        if resolved_fmt.container is Container.TAR:
            with open_tar_stream_writer(
                out_stream,
                resolved_fmt,
                level=lvl,
                threads=threads,
                deterministic=deterministic,
            ) as tf:
                return _write_tar_from_input(
                    tf,
                    source,
                    include_root=include_root,
                    follow_symlinks=follow_symlinks,
                    include=include,
                    exclude=exclude,
                    filter_func=filter_func,
                    deterministic=deterministic,
                    progress_callback=progress_callback,
                    total_hint=total_hint,
                )

        if resolved_fmt.container is Container.ZIP:
            if out_stream.seekable():
                return _write_zip_from_input(
                    out_stream,
                    source,
                    level=lvl,
                    include_root=include_root,
                    follow_symlinks=follow_symlinks,
                    include=include,
                    exclude=exclude,
                    filter_func=filter_func,
                    deterministic=deterministic,
                    progress_callback=progress_callback,
                    total_hint=total_hint,
                )
            with tempfile.SpooledTemporaryFile(max_size=_SPOOL_LIMIT, mode="w+b") as spool:
                total = _write_zip_from_input(
                    cast(BinaryIO, spool),
                    source,
                    level=lvl,
                    include_root=include_root,
                    follow_symlinks=follow_symlinks,
                    include=include,
                    exclude=exclude,
                    filter_func=filter_func,
                    deterministic=deterministic,
                    progress_callback=progress_callback,
                    total_hint=total_hint,
                )
                spool.seek(0)
                shutil.copyfileobj(spool, out_stream, chunk)
                return total

        if resolved_fmt.container is Container.SEVENZ:
            return _write_7z_from_input(
                out_stream,
                source,
                level=lvl,
                include_root=include_root,
                follow_symlinks=follow_symlinks,
                include=include,
                exclude=exclude,
                filter_func=filter_func,
                deterministic=deterministic,
                progress_callback=progress_callback,
                total_hint=total_hint,
            )

        raise UnsupportedFormatError(f"Unsupported container format: {resolved_fmt}")

    if dst_path is not None:
        with atomic_open(dst_path, "wb") as out_fh:
            _write_to_stream(out_fh)
        return dst_path

    assert not isinstance(dst, (str, os.PathLike))
    _write_to_stream(dst)
    return None


def pack(
    source: MembersInput,
    fmt: Format | str,
    level: int | None = None,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes:
    """Pack ``source`` into an in-memory ``bytes`` archive of format ``fmt``."""
    buf = io.BytesIO()
    pack_file(buf, source, fmt=fmt, level=level, threads=threads, deterministic=deterministic)
    return buf.getvalue()


def compress_file(
    src: StrPath,
    dst: StrPath,
    fmt: Format | str | None = None,
    level: int | None = None,
    chunk: int = CHUNK_SIZE,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> Path:
    """Compress file or directory ``src`` into ``dst`` atomically and return ``Path(dst)``."""
    out = pack_file(
        dst,
        src,
        fmt=fmt,
        level=level,
        chunk=chunk,
        threads=threads,
        deterministic=deterministic,
    )
    assert out is not None
    return out


@overload
def compress(
    source: bytes,
    output: Format | str,
    level: int | None = None,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes: ...


@overload
def compress(
    source: StrPath,
    output: StrPath,
    *,
    format: Format | str | None = None,
    level: int | None = None,
    threads: int | None = None,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    compute_sha256: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> CompressionStats: ...


def compress(
    source: StrPath | bytes,
    output: StrPath | Format | str,
    level: int | None = None,
    *,
    format: Format | str | None = None,
    threads: int | None = None,
    include_root: bool = False,
    follow_symlinks: bool = False,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    deterministic: bool = False,
    compute_sha256: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> CompressionStats | bytes:
    """Universal compression entry point supporting both filesystem paths and raw ``bytes``.

    - When ``source`` is ``bytes``: compresses in memory using ``output`` as the target
      :class:`Format` or codec string and returns ``bytes``.
    - When ``source`` is a file or directory path: streams into ``output`` atomically
      and returns :class:`~arkit.models.CompressionStats`.
    """
    if isinstance(source, bytes):
        if not isinstance(output, (Format, str)):
            raise TypeError("When compressing bytes, second argument must be a Format or format string")
        return compress_bytes(
            source,
            output,
            level=level,
            threads=threads,
            deterministic=deterministic,
        )

    if isinstance(output, Format):
        raise TypeError("When compressing a filesystem path, second argument must be an output path")

    src_path = Path(source)
    dst_path = Path(output)
    t0 = time.perf_counter()
    src_bytes = _compute_source_bytes(
        src_path,
        follow_symlinks=follow_symlinks,
        include=include,
        exclude=exclude,
        filter_func=filter_func,
    )
    pack_file(
        dst_path,
        src_path,
        fmt=format,
        level=level,
        threads=threads,
        include_root=include_root,
        follow_symlinks=follow_symlinks,
        include=include,
        exclude=exclude,
        filter_func=filter_func,
        deterministic=deterministic,
        progress_callback=progress_callback,
        total_hint=src_bytes,
    )
    elapsed = time.perf_counter() - t0
    digest = checksum(dst_path, "sha256") if compute_sha256 else None
    return compression_stats(src_bytes, dst_path.stat().st_size, elapsed_seconds=elapsed, sha256=digest)


# ---------------------------------------------------------------------------
# Safe Streaming Extraction & Decompression
# ---------------------------------------------------------------------------


def decompress_file(
    src: StrPath,
    dst: StrPath,
    fmt: Format | str | None = None,
    chunk: int = CHUNK_SIZE,
) -> Path:
    """Stream-decompress a single-stream file ``src`` directly to file ``dst`` atomically."""
    src_path = Path(src)
    dst_path = Path(dst)
    resolved = _resolve_fmt(src_path, fmt)
    with src_path.open("rb") as fi, atomic_open(dst_path, "wb") as fo:
        decompress_stream_format(fi, fo, resolved, chunk=chunk)
    return dst_path


def extract(
    source: StrPath | bytes,
    dest: StrPath,
    fmt: Format | str | None = None,
    members: Iterable[str] | None = None,
    *,
    safe: bool = True,
    policy: SafetyPolicy | None = None,
    output_name: str | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    strip_components: int = 0,
    overwrite: bool = True,
    preserve_permissions: bool = False,
    preserve_timestamps: bool = True,
    password: str | bytes | None = None,
    progress_callback: ProgressCallback | None = None,
) -> list[Path]:
    """Safely stream-extract ``source`` into directory ``dest`` and return extracted file paths.

    Enforces :class:`~arkit.models.SafetyPolicy` (Zip/Tar Slip prevention, symlink guards,
    special-file blocking, and mid-stream decompression-bomb limits) when ``safe=True``.
    """
    resolved_fmt = _resolve_fmt(source, fmt)
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)

    active_policy = (
        (policy if policy is not None else DEFAULT_SAFETY_POLICY)
        if safe
        else SafetyPolicy(
            allow_symlinks=True,
            allow_special=True,
            max_total_size=None,
            max_file_size=None,
            max_files=None,
            max_ratio=None,
        )
    )
    comp_size = len(source) if isinstance(source, bytes) else Path(source).stat().st_size
    guard = ExtractionGuard(
        dest_path,
        active_policy,
        compressed_size=comp_size,
        strip_components=strip_components,
    )
    wanted = set(members) if members is not None else None
    written: list[Path] = []
    progress_bytes = 0

    def _want(name: str) -> bool:
        if wanted is not None and name not in wanted:
            return False
        return _matches_filters(name, include=include, exclude=exclude, filter_func=filter_func)

    # 1. Single-file compressed stream (.gz, .zst, .br, .lz4, .snappy, .pbrt, ...)
    if resolved_fmt.container is None:
        assert resolved_fmt.compression is not None
        if output_name is not None:
            single_name = output_name
        elif isinstance(source, bytes):
            single_name = _PAYLOAD_NAME
        else:
            single_name = _derive_single_output_name(Path(source), resolved_fmt)

        if not _want(single_name):
            return written

        target = (
            guard.resolve_member(single_name)
            if safe
            else (dest_path / single_name)
        )
        if target is None:
            return written
        if not overwrite and target.exists():
            return written

        ensure_parent(target, root=guard.root if safe else None)
        raw_in: BinaryIO = io.BytesIO(source) if isinstance(source, bytes) else open(source, "rb")
        try:
            with open_compressed_reader(raw_in, resolved_fmt.compression.value) as reader, atomic_open(
                target, "wb"
            ) as writer:
                while chunk := reader.read(CHUNK_SIZE):
                    if safe:
                        guard.record_chunk(len(chunk))
                    writer.write(chunk)
                    progress_bytes += len(chunk)
                    if progress_callback is not None:
                        progress_callback(progress_bytes, None)
        finally:
            if not isinstance(source, bytes):
                raw_in.close()
        written.append(target)
        return written

    # 2. ZIP / WHL containers
    if resolved_fmt.container is Container.ZIP:
        pwd_bytes: bytes | None = (
            password.encode("utf-8") if isinstance(password, str) else password
        )
        zf_src: Any = io.BytesIO(source) if isinstance(source, bytes) else Path(source)
        with zipfile.ZipFile(zf_src, "r") as zf:
            for zi in zf.infolist():
                norm_name = zi.filename.replace("\\", "/").strip("/")
                if not norm_name:
                    continue
                if zi.is_dir():
                    if strip_components == 0 and wanted is None:
                        dir_target = (
                            guard.resolve_member(zi.filename)
                            if safe
                            else (dest_path / norm_name)
                        )
                        if dir_target is not None:
                            dir_target.mkdir(parents=True, exist_ok=True)
                    continue

                if not _want(zi.filename) and not _want(norm_name):
                    continue

                is_symlink = ((zi.external_attr >> 16) & stat.S_IFMT(0o170000)) == stat.S_IFLNK
                target = (
                    guard.resolve_member(zi.filename, declared_size=zi.file_size)
                    if safe
                    else (dest_path / norm_name)
                )
                if target is None:
                    continue
                if not overwrite and target.exists():
                    continue

                ensure_parent(target, root=guard.root if safe else None)
                if is_symlink:
                    link_target = zf.read(zi, pwd=pwd_bytes).decode("utf-8", errors="replace")
                    if safe:
                        guard.validate_link(target, link_target, is_hardlink=False)
                    if target.exists() or target.is_symlink():
                        target.unlink()
                    os.symlink(link_target, target)
                    written.append(target)
                    continue

                if (zi.flag_bits & 0x1) and pwd_bytes is None:
                    raise PasswordProtectedError(
                        f"ZIP member {zi.filename!r} is password-protected; provide password=..."
                    )
                try:
                    member_stream = zf.open(zi, "r", pwd=pwd_bytes)
                except RuntimeError as exc:
                    if "password" in str(exc).lower() or "encrypted" in str(exc).lower():
                        raise PasswordProtectedError(f"Cannot unlock ZIP member {zi.filename!r}: {exc}") from exc
                    raise
                with member_stream, atomic_open(target, "wb") as writer:
                    while chunk := member_stream.read(CHUNK_SIZE):
                        if safe:
                            guard.record_chunk(len(chunk))
                        writer.write(chunk)
                        progress_bytes += len(chunk)
                        if progress_callback is not None:
                            progress_callback(progress_bytes, None)

                if preserve_permissions:
                    unix_mode = (zi.external_attr >> 16) & 0o777
                    if unix_mode:
                        with contextlib.suppress(OSError):
                            os.chmod(target, unix_mode)
                if preserve_timestamps:
                    with contextlib.suppress(OSError, ValueError):
                        dt = datetime(*zi.date_time, tzinfo=timezone.utc)
                        ts = dt.timestamp()
                        os.utime(target, (ts, ts))
                written.append(target)
        return written

    # 3. TAR and compound TAR.* containers (single-pass streaming!)
    if resolved_fmt.container is Container.TAR:
        tar_src: StrPath | BinaryIO = io.BytesIO(source) if isinstance(source, bytes) else source
        with open_tar_stream_reader(tar_src, resolved_fmt) as tf:
            for member in tf:
                norm_name = member.name.replace("\\", "/").lstrip("./")
                if member.isdir():
                    if strip_components == 0 and wanted is None:
                        dir_target = (
                            guard.resolve_member(member.name)
                            if safe
                            else (dest_path / norm_name)
                        )
                        if dir_target is not None:
                            dir_target.mkdir(parents=True, exist_ok=True)
                    continue

                if not _want(member.name) and not _want(norm_name):
                    continue

                target = (
                    guard.resolve_member(member.name, declared_size=member.size if member.isfile() else 0)
                    if safe
                    else (dest_path / norm_name)
                )
                if target is None:
                    continue
                if not overwrite and target.exists():
                    continue

                if member.issym() or member.islnk():
                    if safe:
                        guard.validate_link(target, member.linkname, is_hardlink=member.islnk())
                    ensure_parent(target, root=guard.root if safe else None)
                    if target.exists() or target.is_symlink():
                        target.unlink()
                    if member.issym():
                        os.symlink(member.linkname, target)
                    else:
                        hard_src = safe_destination(dest_path, member.linkname, strip_components=strip_components)
                        if hard_src is not None and hard_src.exists():
                            os.link(hard_src, target)
                    written.append(target)
                    continue

                if not member.isfile():
                    if safe and not active_policy.allow_special:
                        from .exceptions import UnsafeArchivePathError

                        raise UnsafeArchivePathError(f"Unsupported special tar member: {member.name!r}")
                    continue

                ensure_parent(target, root=guard.root if safe else None)
                extracted = tf.extractfile(member)
                if extracted is None:
                    continue
                with extracted, atomic_open(target, "wb") as writer:
                    while chunk := extracted.read(CHUNK_SIZE):
                        if safe:
                            guard.record_chunk(len(chunk))
                        writer.write(chunk)
                        progress_bytes += len(chunk)
                        if progress_callback is not None:
                            progress_callback(progress_bytes, None)

                if preserve_permissions and member.mode:
                    with contextlib.suppress(OSError):
                        os.chmod(target, member.mode & 0o777)
                if preserve_timestamps and member.mtime:
                    with contextlib.suppress(OSError):
                        os.utime(target, (member.mtime, member.mtime))
                written.append(target)
        return written

    # 4. 7z container
    if resolved_fmt.container is Container.SEVENZ:
        py7zr = _require_py7zr()
        pwd_str = password.decode("utf-8") if isinstance(password, bytes) else password
        sz_src: Any = io.BytesIO(source) if isinstance(source, bytes) else Path(source)
        try:
            sz_ctx = py7zr.SevenZipFile(sz_src, "r", password=pwd_str)
        except Exception as exc:
            if "password" in str(exc).lower():
                raise PasswordProtectedError(f"7z archive requires a password: {exc}") from exc
            raise

        with sz_ctx as sz:
            if getattr(sz, "needs_password", lambda: False)() and not pwd_str:
                raise PasswordProtectedError("7z archive is encrypted; provide password=...")
            infos = sz.list()
            selected_targets: list[str] = []
            dest_mapping: dict[str, Path] = {}
            for fi in infos:
                if fi.is_directory:
                    if strip_components == 0 and wanted is None:
                        dt_dir = guard.resolve_member(fi.filename) if safe else (dest_path / fi.filename)
                        if dt_dir is not None:
                            dt_dir.mkdir(parents=True, exist_ok=True)
                    continue
                if not _want(fi.filename):
                    continue
                uncomp = int(fi.uncompressed or 0)
                target = guard.resolve_member(fi.filename, declared_size=uncomp) if safe else (dest_path / fi.filename)
                if target is None:
                    continue
                if safe:
                    guard.record_chunk(uncomp)
                if not overwrite and target.exists():
                    continue
                selected_targets.append(fi.filename)
                dest_mapping[fi.filename] = target

            if selected_targets:
                with tempfile.TemporaryDirectory(prefix="arkit_7z_ext_") as tmp_dir:
                    try:
                        sz.extract(path=tmp_dir, targets=selected_targets)
                    except Exception as exc:
                        if "password" in str(exc).lower():
                            raise PasswordProtectedError(f"Invalid or missing 7z password: {exc}") from exc
                        raise
                    for raw_name in selected_targets:
                        staged = Path(tmp_dir) / raw_name
                        if staged.is_file():
                            final_target = dest_mapping[raw_name]
                            ensure_parent(final_target, root=guard.root if safe else None)
                            shutil.move(str(staged), str(final_target))
                            written.append(final_target)
                            progress_bytes += final_target.stat().st_size
                            if progress_callback is not None:
                                progress_callback(progress_bytes, None)
        return written

    raise UnsupportedFormatError(f"Unsupported format for extraction: {resolved_fmt}")


@overload
def decompress(
    archive_path: bytes,
    destination: None = None,
    *,
    format: Format | str | None = None,
) -> bytes: ...


@overload
def decompress(
    archive_path: StrPath,
    destination: StrPath,
    *,
    format: Format | str | None = None,
    output_name: str | None = None,
    safe: bool = True,
    policy: SafetyPolicy | None = None,
    strip_components: int = 0,
    overwrite: bool = True,
    preserve_permissions: bool = False,
    preserve_timestamps: bool = True,
    members: Iterable[str] | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    password: str | bytes | None = None,
    progress_callback: ProgressCallback | None = None,
) -> Path: ...


def decompress(
    archive_path: StrPath | bytes,
    destination: StrPath | None = None,
    *,
    format: Format | str | None = None,
    output_name: str | None = None,
    safe: bool = True,
    policy: SafetyPolicy | None = None,
    strip_components: int = 0,
    overwrite: bool = True,
    preserve_permissions: bool = False,
    preserve_timestamps: bool = True,
    members: Iterable[str] | None = None,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    filter_func: FilterFunc | None = None,
    password: str | bytes | None = None,
    progress_callback: ProgressCallback | None = None,
) -> Path | bytes:
    """Universal decompression & extraction entry point.

    - When ``archive_path`` is ``bytes`` and ``destination`` is ``None``: returns decompressed ``bytes``.
    - When ``archive_path`` is a file path and ``destination`` is a directory path:
      safely extracts into ``destination`` (returning the extracted file path for
      single-stream formats or ``Path(destination)`` for multi-file containers).
    """
    if isinstance(archive_path, bytes) and destination is None:
        return decompress_bytes(archive_path, fmt=format)
    if destination is None:
        raise TypeError("destination directory path is required when decompressing a file")

    resolved_fmt = _resolve_fmt(archive_path, format)
    dest_dir = Path(destination)
    extracted_paths = extract(
        archive_path,
        dest_dir,
        fmt=resolved_fmt,
        members=members,
        safe=safe,
        policy=policy,
        output_name=output_name,
        include=include,
        exclude=exclude,
        filter_func=filter_func,
        strip_components=strip_components,
        overwrite=overwrite,
        preserve_permissions=preserve_permissions,
        preserve_timestamps=preserve_timestamps,
        password=password,
        progress_callback=progress_callback,
    )
    if resolved_fmt.container is None and len(extracted_paths) == 1:
        return extracted_paths[0]
    return dest_dir


# ---------------------------------------------------------------------------
# Streaming Member Listing, Info, Iteration, Read, Add, Remove, Diff
# ---------------------------------------------------------------------------


def _read_members_bytes(fmt: Format, data: bytes) -> list[Member]:
    if fmt.container is None:
        assert fmt.compression is not None
        payload = decompress_bytes_raw(data, fmt.compression.value)
        return [Member(MemberInfo(_PAYLOAD_NAME, len(payload), len(data), None), payload)]
    if fmt.container is Container.TAR:
        out: list[Member] = []
        with open_tar_stream_reader(io.BytesIO(data), fmt) as tf:
            for ti in tf:
                if not ti.isfile():
                    continue
                ef = tf.extractfile(ti)
                body = ef.read() if ef is not None else b""
                out.append(
                    Member(
                        MemberInfo(ti.name, ti.size, None, _tar_mtime_dt(ti), mode=ti.mode),
                        body,
                    )
                )
        return out
    if fmt.container is Container.ZIP:
        out_zip: list[Member] = []
        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            for zi in zf.infolist():
                if zi.is_dir():
                    continue
                out_zip.append(
                    Member(
                        MemberInfo(
                            zi.filename,
                            zi.file_size,
                            zi.compress_size,
                            datetime(*zi.date_time, tzinfo=timezone.utc),
                            mode=(zi.external_attr >> 16) & 0o777 or None,
                            crc=zi.CRC,
                        ),
                        zf.read(zi),
                    )
                )
        return out_zip
    if fmt.container is Container.SEVENZ:
        py7zr = _require_py7zr()
        pio = importlib.import_module("py7zr.io")
        out_7z: list[Member] = []
        with py7zr.SevenZipFile(io.BytesIO(data), "r") as sz:
            infos = [fi for fi in sz.list() if not fi.is_directory]
            if infos:
                factory = pio.BytesIOFactory(limit=1 << 30)
                sz.extract(targets=[fi.filename for fi in infos], factory=factory)
                for fi in infos:
                    bio = factory.products.get(fi.filename)
                    payload = bio.read() if bio is not None else b""
                    mt = getattr(fi, "creationtime", None)
                    out_7z.append(
                        Member(
                            MemberInfo(
                                fi.filename,
                                len(payload),
                                getattr(fi, "compressed", None),
                                mt if isinstance(mt, datetime) else None,
                                crc=getattr(fi, "crc32", None),
                            ),
                            payload,
                        )
                    )
        return out_7z
    raise UnsupportedFormatError(f"Unsupported format: {fmt}")


def list_members(source: StrPath | bytes, fmt: Format | str | None = None) -> list[MemberInfo]:
    """List :class:`~arkit.models.MemberInfo` entries without loading member bodies into RAM."""
    resolved = _resolve_fmt(source, fmt)
    if isinstance(source, bytes):
        if resolved.container is Container.ZIP:
            with zipfile.ZipFile(io.BytesIO(source), "r") as zf:
                return [
                    MemberInfo(
                        zi.filename,
                        zi.file_size,
                        zi.compress_size,
                        datetime(*zi.date_time, tzinfo=timezone.utc),
                        mode=(zi.external_attr >> 16) & 0o777 or None,
                        crc=zi.CRC,
                    )
                    for zi in zf.infolist()
                    if not zi.is_dir()
                ]
        return [m.info for m in _read_members_bytes(resolved, source)]

    path = Path(source)
    if resolved.container is None:
        assert resolved.compression is not None
        sink = _CountingSink()
        with path.open("rb") as fh:
            _codec_decompress_stream(fh, cast(BinaryIO, sink), resolved.compression.value)
        single_name = _derive_single_output_name(path, resolved)
        return [MemberInfo(single_name, sink.size, path.stat().st_size, None)]

    if resolved.container is Container.ZIP:
        with zipfile.ZipFile(path, "r") as zf:
            return [
                MemberInfo(
                    zi.filename,
                    zi.file_size,
                    zi.compress_size,
                    datetime(*zi.date_time, tzinfo=timezone.utc),
                    mode=(zi.external_attr >> 16) & 0o777 or None,
                    crc=zi.CRC,
                )
                for zi in zf.infolist()
                if not zi.is_dir()
            ]

    if resolved.container is Container.TAR:
        out: list[MemberInfo] = []
        with open_tar_stream_reader(path, resolved) as tf:
            for ti in tf:
                if not ti.isfile():
                    continue
                out.append(
                    MemberInfo(
                        ti.name,
                        ti.size,
                        None,
                        _tar_mtime_dt(ti),
                        mode=ti.mode,
                    )
                )
        return out

    if resolved.container is Container.SEVENZ:
        py7zr = _require_py7zr()
        with py7zr.SevenZipFile(path, "r") as sz:
            out_7z: list[MemberInfo] = []
            for fi in sz.list():
                if fi.is_directory:
                    continue
                mt = getattr(fi, "creationtime", None)
                out_7z.append(
                    MemberInfo(
                        fi.filename,
                        int(fi.uncompressed or 0),
                        getattr(fi, "compressed", None),
                        mt if isinstance(mt, datetime) else None,
                        crc=getattr(fi, "crc32", None),
                    )
                )
            return out_7z

    raise UnsupportedFormatError(f"Unsupported format: {resolved}")


def iter_members(
    source: StrPath | bytes,
    fmt: Format | str | None = None,
    chunk: int = CHUNK_SIZE,
) -> Iterator[Member]:
    """Stream-yield :class:`~arkit.models.Member` entries one at a time without loading the full archive."""
    _ = chunk
    resolved = _resolve_fmt(source, fmt)
    if isinstance(source, bytes):
        yield from _read_members_bytes(resolved, source)
        return

    path = Path(source)
    if resolved.container is None:
        assert resolved.compression is not None
        buf = io.BytesIO()
        with path.open("rb") as fh:
            _codec_decompress_stream(fh, buf, resolved.compression.value, chunk_size=chunk)
        payload = buf.getvalue()
        yield Member(
            MemberInfo(_derive_single_output_name(path, resolved), len(payload), path.stat().st_size, None),
            payload,
        )
        return

    if resolved.container is Container.ZIP:
        with zipfile.ZipFile(path, "r") as zf:
            for zi in zf.infolist():
                if zi.is_dir():
                    continue
                yield Member(
                    MemberInfo(
                        zi.filename,
                        zi.file_size,
                        zi.compress_size,
                        datetime(*zi.date_time, tzinfo=timezone.utc),
                        mode=(zi.external_attr >> 16) & 0o777 or None,
                        crc=zi.CRC,
                    ),
                    zf.read(zi),
                )
        return

    if resolved.container is Container.TAR:
        with open_tar_stream_reader(path, resolved) as tf:
            for ti in tf:
                if not ti.isfile():
                    continue
                ef = tf.extractfile(ti)
                body = ef.read() if ef is not None else b""
                yield Member(
                    MemberInfo(ti.name, ti.size, None, _tar_mtime_dt(ti), mode=ti.mode),
                    body,
                )
        return

    if resolved.container is Container.SEVENZ:
        py7zr = _require_py7zr()
        pio = importlib.import_module("py7zr.io")
        with py7zr.SevenZipFile(path, "r") as sz:
            for fi in sz.list():
                if fi.is_directory:
                    continue
                sz.reset()
                factory = pio.BytesIOFactory(limit=1 << 30)
                sz.extract(targets=[fi.filename], factory=factory)
                bio = factory.products.get(fi.filename)
                body = bio.read() if bio is not None else b""
                mt = getattr(fi, "creationtime", None)
                yield Member(
                    MemberInfo(
                        fi.filename,
                        len(body),
                        getattr(fi, "compressed", None),
                        mt if isinstance(mt, datetime) else None,
                    ),
                    body,
                )
        return

    raise UnsupportedFormatError(f"Unsupported format: {resolved}")


def read_member(source: StrPath | bytes, name: str, fmt: Format | str | None = None) -> bytes:
    """Read a single named member's bytes from ``source`` without extracting other members."""
    resolved = _resolve_fmt(source, fmt)
    if not isinstance(source, bytes) and resolved.container is Container.ZIP:
        with zipfile.ZipFile(Path(source), "r") as zf:
            try:
                return zf.read(name)
            except KeyError as exc:
                raise ArchiveError(f"Member not found: {name!r}") from exc
    for m in iter_members(source, resolved):
        if m.info.name == name or (resolved.container is None and name == _PAYLOAD_NAME):
            return m.data
    raise ArchiveError(f"Member not found: {name!r}")


def read_members(source: StrPath | bytes, fmt: Format | str | None = None) -> dict[str, bytes]:
    """Return ``{member_name: bytes}`` for every regular file member in ``source``."""
    return {m.info.name: m.data for m in iter_members(source, fmt)}


def add_member(
    path: StrPath,
    name: str,
    data: bytes,
    fmt: Format | str | None = None,
    level: int | None = None,
) -> Path:
    """Add ``(name, data)`` to container archive ``path`` atomically with streaming copy."""
    archive_path = Path(path)
    resolved = _resolve_fmt(archive_path, fmt)
    if resolved.container is None:
        raise ArchiveError("Single-stream compression formats do not support named members")

    existing = list_members(archive_path, resolved)
    if any(mi.name == name for mi in existing):
        raise ArchiveError(f"Member already exists: {name!r}")

    lvl = _resolve_container_level(resolved, level)
    now_dt = datetime.now(tz=timezone.utc)

    if resolved.container is Container.ZIP:
        comp = zipfile.ZIP_STORED if lvl <= 0 else zipfile.ZIP_DEFLATED
        cl = lvl if comp == zipfile.ZIP_DEFLATED else None
        with zipfile.ZipFile(archive_path, "r") as zin, atomic_open(archive_path, "wb") as raw_out:
            with zipfile.ZipFile(raw_out, "w", compression=comp, compresslevel=cl, allowZip64=True) as zout:
                for zi in zin.infolist():
                    if zi.is_dir():
                        continue
                    new_zi = zipfile.ZipInfo(filename=zi.filename, date_time=zi.date_time)
                    new_zi.compress_type = zi.compress_type
                    new_zi.external_attr = zi.external_attr
                    with zin.open(zi, "r") as s_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                        shutil.copyfileobj(s_fh, d_fh, CHUNK_SIZE)
                added_zi = zipfile.ZipInfo(filename=name, date_time=_zip_date_time(now_dt))
                added_zi.compress_type = comp
                added_zi.external_attr = (stat.S_IFREG | 0o644) << 16
                zout.writestr(added_zi, data, compress_type=comp, compresslevel=cl)
        return archive_path

    if resolved.container is Container.TAR:
        with open_tar_stream_reader(archive_path, resolved) as tin, atomic_open(archive_path, "wb") as raw_out:
            with open_tar_stream_writer(raw_out, resolved, level=lvl) as tout:
                for ti in tin:
                    if ti.isfile():
                        ef = tin.extractfile(ti)
                        tout.addfile(ti, ef)
                    else:
                        tout.addfile(ti)
                new_ti = tarfile.TarInfo(name=name)
                new_ti.size = len(data)
                new_ti.mode = 0o644
                new_ti.mtime = int(now_dt.timestamp())
                tout.addfile(new_ti, io.BytesIO(data))
        return archive_path

    # 7z container
    def _gen() -> Iterator[Member]:
        yield from iter_members(archive_path, resolved)
        yield Member(MemberInfo(name, len(data), None, now_dt), data)

    all_members = list(_gen())
    pack_file(archive_path, all_members, fmt=resolved, level=lvl)
    return archive_path


def remove_member(
    path: StrPath,
    name: str,
    fmt: Format | str | None = None,
    level: int | None = None,
) -> Path:
    """Remove ``name`` from container archive ``path`` atomically with streaming copy."""
    archive_path = Path(path)
    resolved = _resolve_fmt(archive_path, fmt)
    if resolved.container is None:
        raise ArchiveError("Single-stream compression formats do not support named members")

    existing = list_members(archive_path, resolved)
    if not any(mi.name == name for mi in existing):
        raise ArchiveError(f"Member not found: {name!r}")

    lvl = _resolve_container_level(resolved, level)

    if resolved.container is Container.ZIP:
        comp = zipfile.ZIP_STORED if lvl <= 0 else zipfile.ZIP_DEFLATED
        cl = lvl if comp == zipfile.ZIP_DEFLATED else None
        with zipfile.ZipFile(archive_path, "r") as zin, atomic_open(archive_path, "wb") as raw_out:
            with zipfile.ZipFile(raw_out, "w", compression=comp, compresslevel=cl, allowZip64=True) as zout:
                for zi in zin.infolist():
                    if zi.is_dir() or zi.filename == name:
                        continue
                    new_zi = zipfile.ZipInfo(filename=zi.filename, date_time=zi.date_time)
                    new_zi.compress_type = zi.compress_type
                    new_zi.external_attr = zi.external_attr
                    with zin.open(zi, "r") as s_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                        shutil.copyfileobj(s_fh, d_fh, CHUNK_SIZE)
        return archive_path

    if resolved.container is Container.TAR:
        with open_tar_stream_reader(archive_path, resolved) as tin, atomic_open(archive_path, "wb") as raw_out:
            with open_tar_stream_writer(raw_out, resolved, level=lvl) as tout:
                for ti in tin:
                    if ti.name == name:
                        continue
                    if ti.isfile():
                        ef = tin.extractfile(ti)
                        tout.addfile(ti, ef)
                    else:
                        tout.addfile(ti)
        return archive_path

    kept = [m for m in iter_members(archive_path, resolved) if m.info.name != name]
    pack_file(archive_path, kept, fmt=resolved, level=lvl)
    return archive_path


def info(
    source: StrPath | bytes,
    fmt: Format | str | None = None,
    *,
    compute_sha256: bool = False,
) -> ArchiveInfo:
    """Inspect ``source`` and return :class:`~arkit.models.ArchiveInfo` without loading member payloads."""
    resolved = _resolve_fmt(source, fmt)
    members = list_members(source, resolved)
    compressed_size = len(source) if isinstance(source, bytes) else Path(source).stat().st_size
    payload_size = sum(m.size for m in members)
    ratio = round(payload_size / compressed_size, 6) if compressed_size > 0 else 0.0
    digest = checksum(source, "sha256") if compute_sha256 else None
    return ArchiveInfo(
        path=None if isinstance(source, bytes) else Path(source),
        format=resolved,
        size=compressed_size,
        payload_size=payload_size,
        member_count=len(members),
        ratio=ratio,
        members=tuple(members),
        sha256=digest,
    )


def _stream_member_digests(source: StrPath | bytes, fmt: Format | str | None = None) -> dict[str, str]:
    resolved = _resolve_fmt(source, fmt)
    digests: dict[str, str] = {}
    if not isinstance(source, bytes) and resolved.container is Container.ZIP:
        with zipfile.ZipFile(Path(source), "r") as zf:
            for zi in zf.infolist():
                if zi.is_dir():
                    continue
                h = hashlib.sha256()
                with zf.open(zi, "r") as fh:
                    while chunk := fh.read(CHUNK_SIZE):
                        h.update(chunk)
                digests[zi.filename] = h.hexdigest()
        return digests

    if not isinstance(source, bytes) and resolved.container is Container.TAR:
        with open_tar_stream_reader(Path(source), resolved) as tf:
            for ti in tf:
                if not ti.isfile():
                    continue
                h = hashlib.sha256()
                ef = tf.extractfile(ti)
                if ef is not None:
                    with ef:
                        while chunk := ef.read(CHUNK_SIZE):
                            h.update(chunk)
                digests[ti.name] = h.hexdigest()
        return digests

    for m in iter_members(source, resolved):
        digests[m.info.name] = hashlib.sha256(m.data).hexdigest()
    return digests


def diff_archives(
    archive_a: StrPath | bytes,
    archive_b: StrPath | bytes,
    *,
    fmt_a: Format | str | None = None,
    fmt_b: Format | str | None = None,
) -> DiffReport:
    """Compare two archives by streaming member SHA-256 digests without disk extraction."""
    map_a = _stream_member_digests(archive_a, fmt_a)
    map_b = _stream_member_digests(archive_b, fmt_b)
    keys_a = set(map_a)
    keys_b = set(map_b)
    added = tuple(sorted(keys_b - keys_a))
    removed = tuple(sorted(keys_a - keys_b))
    common = sorted(keys_a & keys_b)
    modified = tuple(k for k in common if map_a[k] != map_b[k])
    unchanged = tuple(k for k in common if map_a[k] == map_b[k])
    return DiffReport(added=added, removed=removed, modified=modified, unchanged=unchanged)


# ---------------------------------------------------------------------------
# Streaming Validation & Integrity Verification
# ---------------------------------------------------------------------------


def validate(
    source: StrPath | bytes,
    fmt: Format | str | None = None,
    *,
    format: Format | str | None = None,
) -> HealthReport:
    """Validate archive or compressed stream integrity in bounded memory without temp files.

    Returns a :class:`~arkit.models.HealthReport` whose boolean value is ``report.healthy``.
    """
    explicit_fmt = format if format is not None else fmt
    fmt_label = "unknown"
    try:
        resolved = _resolve_fmt(source, explicit_fmt)
        fmt_label = resolved.format_id

        head: bytes
        if isinstance(source, bytes):
            head = source[:512]
        else:
            p = Path(source)
            if not p.is_file():
                return HealthReport(False, fmt_label, (f"FileNotFoundError: {p}",))
            with p.open("rb") as fh:
                head = fh.read(512)

        if not magic_ok(resolved, head):
            return HealthReport(
                False,
                fmt_label,
                (f"Magic signature mismatch for format {fmt_label}",),
            )

        if resolved.container is None:
            assert resolved.compression is not None
            sink = _CountingSink()
            raw_stream: BinaryIO = io.BytesIO(source) if isinstance(source, bytes) else open(source, "rb")
            try:
                _codec_decompress_stream(raw_stream, cast(BinaryIO, sink), resolved.compression.value)
            finally:
                if not isinstance(source, bytes):
                    raw_stream.close()
            return HealthReport(True, fmt_label, (), checked_members=1, checked_bytes=sink.size)

        if resolved.container is Container.ZIP:
            zf_src: Any = io.BytesIO(source) if isinstance(source, bytes) else Path(source)
            with zipfile.ZipFile(zf_src, "r") as zf:
                bad_member = zf.testzip()
                if bad_member is not None:
                    return HealthReport(False, fmt_label, (f"CRC failure in ZIP member: {bad_member}",))
                files = [zi for zi in zf.infolist() if not zi.is_dir()]
                total_b = sum(zi.file_size for zi in files)
                return HealthReport(True, fmt_label, (), checked_members=len(files), checked_bytes=total_b)

        if resolved.container is Container.TAR:
            tar_src: StrPath | BinaryIO = io.BytesIO(source) if isinstance(source, bytes) else source
            checked_members = 0
            checked_bytes = 0
            with open_tar_stream_reader(tar_src, resolved) as tf:
                for member in tf:
                    if not member.isfile():
                        continue
                    checked_members += 1
                    ef = tf.extractfile(member)
                    if ef is not None:
                        with ef:
                            while chunk := ef.read(CHUNK_SIZE):
                                checked_bytes += len(chunk)
            return HealthReport(
                True,
                fmt_label,
                (),
                checked_members=checked_members,
                checked_bytes=checked_bytes,
            )

        if resolved.container is Container.SEVENZ:
            py7zr = _require_py7zr()
            sz_src: Any = io.BytesIO(source) if isinstance(source, bytes) else Path(source)
            with py7zr.SevenZipFile(sz_src, "r") as sz:
                bad_7z = sz.testzip()
                if bad_7z is not None:
                    return HealthReport(False, fmt_label, (f"Corrupt 7z member: {bad_7z}",))
                sz.reset()
                res = sz.test()
                if res is False:
                    return HealthReport(False, fmt_label, ("7z CRC verification failed",))
                sz.reset()
                files_7z = [fi for fi in sz.list() if not fi.is_directory]
                total_7z = sum(int(fi.uncompressed or 0) for fi in files_7z)
            return HealthReport(True, fmt_label, (), checked_members=len(files_7z), checked_bytes=total_7z)

        return HealthReport(False, fmt_label, (f"Unsupported format: {fmt_label}",))
    except Exception as exc:
        return HealthReport(False, fmt_label, (f"{type(exc).__name__}: {exc}",))


def validate_file(path: StrPath, fmt: Format | str | None = None, chunk: int = CHUNK_SIZE) -> bool:
    """Return ``True`` if archive file ``path`` passes full streaming validation."""
    _ = chunk
    return bool(validate(path, fmt=fmt))


def check_integrity(archive_path: StrPath) -> bool:
    """Compatibility alias returning ``True`` when ``archive_path`` passes integrity verification."""
    return bool(validate(archive_path))


def verify_archive(archive_file: StrPath, archive_type: Format | str | None = None) -> bool:
    """Verify ``archive_file`` and raise :class:`~arkit.exceptions.CorruptedArchiveError` on failure."""
    p = Path(archive_file)
    if not p.is_file():
        raise CorruptedArchiveError(f'Failed to verify file "{p}": file does not exist')
    # Raise UnknownFormatError/ValueError directly if caller passed an unsupported format string
    resolved = _resolve_fmt(p, archive_type)
    report = validate(p, fmt=resolved)
    if not report.healthy:
        detail = "; ".join(report.errors) if report.errors else "integrity check failed"
        raise CorruptedArchiveError(f'Failed to verify file "{p}": {detail}')
    return True


# ---------------------------------------------------------------------------
# Streaming Cross-Format Conversion
# ---------------------------------------------------------------------------


def convert_bytes(
    data: bytes,
    fmt: Format | str,
    level: int | None = None,
    src_fmt: Format | str | None = None,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes:
    """Convert archive or stream ``data`` in memory to target format ``fmt``."""
    sf = _resolve_fmt(data, src_fmt)
    tf = Format.parse(fmt)
    if sf == tf and level is None:
        return data
    lvl = _resolve_container_level(tf, level)
    if sf.container is not None and tf.container is not None:
        if sf.container is Container.TAR and tf.container is Container.TAR:
            raw_tar = data if sf.compression is None else decompress_bytes_raw(data, sf.compression.value)
            if tf.compression is None:
                return raw_tar
            return compress_bytes_raw(
                raw_tar,
                tf.compression.value,
                level=lvl,
                threads=threads,
                deterministic=deterministic,
            )
        members = _read_members_bytes(sf, data)
        return pack(members, tf, level=lvl, threads=threads, deterministic=deterministic)

    if sf.container is None and tf.container is not None:
        payload = decompress_bytes(data, sf)
        return pack({_PAYLOAD_NAME: payload}, tf, level=lvl, threads=threads, deterministic=deterministic)

    if sf.container is not None and tf.container is None:
        members = _read_members_bytes(sf, data)
        if len(members) != 1:
            raise ValueError(
                f"Converting container to single-stream format {tf.format_id} requires exactly 1 file, found {len(members)}"
            )
        return compress_bytes(members[0].data, tf, level=lvl, threads=threads, deterministic=deterministic)

    return compress_bytes(
        decompress_bytes(data, sf),
        tf,
        level=lvl,
        threads=threads,
        deterministic=deterministic,
    )


def convert_file(
    src: StrPath,
    dst: StrPath,
    fmt: Format | str | None = None,
    level: int | None = None,
    *,
    src_fmt: Format | str | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> Path:
    """Stream-convert ``src`` into ``dst`` atomically with minimal memory and temp disk usage."""
    src_path = Path(src)
    dst_path = Path(dst)
    sf = _resolve_fmt(src_path, src_fmt)
    tf = _resolve_fmt(dst_path, fmt, for_output=True)
    lvl = _resolve_container_level(tf, level)

    if sf == tf and level is None:
        if src_path.resolve() != dst_path.resolve():
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            with src_path.open("rb") as fi, atomic_open(dst_path, "wb") as fo:
                shutil.copyfileobj(fi, fo, CHUNK_SIZE)
        return dst_path

    # Fast path 1: Stream -> Stream (1 MiB pipe, zero disk staging!)
    if sf.container is None and tf.container is None:
        assert sf.compression is not None and tf.compression is not None
        with src_path.open("rb") as fi, open_compressed_reader(fi, sf.compression.value) as reader, atomic_open(
            dst_path, "wb"
        ) as fo:
            if tf.compression is Compression.PBRT:
                pbc = ParallelBrotliCompressor(quality=lvl, workers=threads)
                pbc.compress_stream(reader, fo)
            else:
                _codec_compress_stream(
                    reader,
                    fo,
                    tf.compression.value,
                    level=lvl,
                    threads=threads,
                    deterministic=deterministic,
                )
        return dst_path

    # Fast path 2: TAR.* -> TAR.* (pure byte-pipe recompression of underlying tar stream!)
    if (
        sf.container is Container.TAR
        and tf.container is Container.TAR
        and sf.compression is not Compression.SEVENZ
        and tf.compression is not Compression.SEVENZ
    ):
        with ExitStack() as stack:
            raw_in = stack.enter_context(src_path.open("rb"))
            tar_byte_reader: BinaryIO = (
                raw_in
                if sf.compression is None
                else stack.enter_context(open_compressed_reader(raw_in, sf.compression.value))
            )
            raw_out = stack.enter_context(atomic_open(dst_path, "wb"))
            if tf.compression is None:
                shutil.copyfileobj(tar_byte_reader, raw_out, CHUNK_SIZE)
            else:
                cwriter = stack.enter_context(
                    open_compressed_writer(
                        raw_out,
                        tf.compression.value,
                        level=lvl,
                        threads=threads,
                        deterministic=deterministic,
                    )
                )
                shutil.copyfileobj(tar_byte_reader, cwriter, CHUNK_SIZE)
        return dst_path

    # Fast path 3: Stream -> Container
    if sf.container is None and tf.container is not None:
        assert sf.compression is not None
        inner_name = _derive_single_output_name(src_path, sf)
        with tempfile.TemporaryDirectory(prefix="arkit_conv_") as tmp_dir:
            staged = Path(tmp_dir) / inner_name
            with src_path.open("rb") as fi, open_compressed_reader(fi, sf.compression.value) as reader, staged.open(
                "wb"
            ) as fo:
                shutil.copyfileobj(reader, fo, CHUNK_SIZE)
            pack_file(dst_path, staged, fmt=tf, level=lvl, threads=threads, deterministic=deterministic)
        return dst_path

    # Fast path 4: Container -> Stream (validate exactly 1 member, then stream!)
    if sf.container is not None and tf.container is None:
        members_meta = list_members(src_path, sf)
        if len(members_meta) != 1:
            raise ValueError(
                f"Converting to a stream format ({tf.format_id}) requires exactly one extracted file, found {len(members_meta)}."
            )
        with tempfile.TemporaryDirectory(prefix="arkit_conv_s_") as tmp_dir:
            extracted_files = extract(src_path, tmp_dir, fmt=sf)
            if len(extracted_files) != 1:
                raise ValueError("Converting to a stream format requires exactly one extracted file.")
            pack_file(dst_path, extracted_files[0], fmt=tf, level=lvl, threads=threads, deterministic=deterministic)
        return dst_path

    # Fast path 5: ZIP <-> TAR streaming member-by-member without full extraction when possible
    if sf.container is Container.ZIP and tf.container is Container.TAR:
        with zipfile.ZipFile(src_path, "r") as zin, atomic_open(dst_path, "wb") as raw_out:
            with open_tar_stream_writer(raw_out, tf, level=lvl, threads=threads, deterministic=deterministic) as tout:
                for zi in zin.infolist():
                    if zi.is_dir():
                        continue
                    ti = tarfile.TarInfo(name=zi.filename)
                    ti.size = zi.file_size
                    ti.mode = ((zi.external_attr >> 16) & 0o777) or 0o644
                    ti.mtime = (
                        _deterministic_epoch()
                        if deterministic
                        else int(datetime(*zi.date_time, tzinfo=timezone.utc).timestamp())
                    )
                    with zin.open(zi, "r") as zfh:
                        tout.addfile(ti, zfh)
        return dst_path

    if sf.container is Container.TAR and tf.container is Container.ZIP:
        comp = zipfile.ZIP_STORED if lvl <= 0 else zipfile.ZIP_DEFLATED
        cl = lvl if comp == zipfile.ZIP_DEFLATED else None
        with open_tar_stream_reader(src_path, sf) as tin, atomic_open(dst_path, "wb") as raw_out:
            with zipfile.ZipFile(raw_out, "w", compression=comp, compresslevel=cl, allowZip64=True) as zout:
                for ti in tin:
                    if not ti.isfile():
                        continue
                    ef = tin.extractfile(ti)
                    if ef is None:
                        continue
                    zi = zipfile.ZipInfo(
                        filename=ti.name,
                        date_time=_zip_date_time(ti.mtime, deterministic=deterministic),
                    )
                    zi.compress_type = comp
                    zi.external_attr = (stat.S_IFREG | (ti.mode & 0o777 if ti.mode else 0o644)) << 16
                    with ef, zout.open(zi, "w", force_zip64=(ti.size >= (1 << 31))) as zfh:
                        shutil.copyfileobj(ef, zfh, CHUNK_SIZE)
        return dst_path

    # General fallback (7z involved): extract safely to temporary workspace and repack
    with tempfile.TemporaryDirectory(prefix="arkit_conv_gen_") as tmp_dir:
        workspace = Path(tmp_dir) / "extracted"
        extract(src_path, workspace, fmt=sf)
        pack_file(dst_path, workspace, fmt=tf, level=lvl, threads=threads, deterministic=deterministic)
    return dst_path


@overload
def convert(
    source: bytes,
    output: Format | str,
    level: int | None = None,
    src_fmt: Format | str | None = None,
    *,
    threads: int | None = None,
    deterministic: bool = False,
) -> bytes: ...


@overload
def convert(
    source: StrPath,
    output: StrPath,
    *,
    output_format: Format | str | None = None,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
    compute_sha256: bool = False,
) -> CompressionStats: ...


def convert(
    source: StrPath | bytes,
    output: StrPath | Format | str,
    level: int | None = None,
    src_fmt: Format | str | None = None,
    *,
    output_format: Format | str | None = None,
    threads: int | None = None,
    deterministic: bool = False,
    compute_sha256: bool = False,
) -> CompressionStats | bytes:
    """Universal conversion entry point supporting both filesystem paths and raw ``bytes``."""
    if isinstance(source, bytes):
        if not isinstance(output, (Format, str)):
            raise TypeError("When converting bytes, second argument must be target Format or format string")
        return convert_bytes(
            source,
            output,
            level=level,
            src_fmt=src_fmt,
            threads=threads,
            deterministic=deterministic,
        )

    if isinstance(output, Format):
        raise TypeError("When converting a file, second argument must be output file path")

    src_path = Path(source)
    dst_path = Path(output)
    t0 = time.perf_counter()
    convert_file(
        src_path,
        dst_path,
        fmt=output_format,
        level=level,
        threads=threads,
        deterministic=deterministic,
    )
    elapsed = time.perf_counter() - t0
    digest = checksum(dst_path, "sha256") if compute_sha256 else None
    return compression_stats(src_path.stat().st_size, dst_path.stat().st_size, elapsed_seconds=elapsed, sha256=digest)


# ---------------------------------------------------------------------------
# Stream-Oriented Container & Validation Helpers
# ---------------------------------------------------------------------------


def pack_stream(
    dst: BinaryIO,
    source: StrPath | MembersInput,
    fmt: Format | str,
    *,
    level: int | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> int:
    """Pack ``source`` into binary stream ``dst`` using container format ``fmt``."""
    resolved = Format.parse(fmt)
    if resolved.container is None:
        raise ValueError(f"Format {resolved} is a single-stream codec, not a container")
    lvl = _resolve_container_level(resolved, level)
    with tempfile.TemporaryDirectory(prefix="arkit_pack_stream_") as tmp_dir:
        tmp_arch = Path(tmp_dir) / f"archive{resolved.ext}"
        pack_file(
            tmp_arch,
            source,
            fmt=resolved,
            level=lvl,
            threads=threads,
            deterministic=deterministic,
        )
        with tmp_arch.open("rb") as fh:
            shutil.copyfileobj(fh, dst, CHUNK_SIZE)
        return tmp_arch.stat().st_size


def extract_stream(
    src: BinaryIO,
    dest: StrPath,
    fmt: Format | str | None = None,
    *,
    policy: SafetyPolicy | None = None,
    overwrite: bool = True,
    strip_components: int = 0,
) -> list[Path]:
    """Safely extract archive stream ``src`` into directory ``dest``."""
    with tempfile.TemporaryDirectory(prefix="arkit_ext_stream_") as tmp_dir:
        ext = Format.parse(fmt).ext if fmt is not None else ".bin"
        tmp_arch = Path(tmp_dir) / f"input{ext}"
        with tmp_arch.open("wb") as out:
            shutil.copyfileobj(src, out, CHUNK_SIZE)
        return extract(
            tmp_arch,
            dest,
            fmt=fmt,
            policy=policy,
            overwrite=overwrite,
            strip_components=strip_components,
        )


def validate_bytes(data: bytes, fmt: Format | str | None = None) -> bool:
    """Return ``True`` if archive or compressed buffer ``data`` passes integrity validation."""
    return bool(validate(data, fmt=fmt))


def validate_stream(src: BinaryIO, fmt: Format | str | None = None) -> bool:
    """Return ``True`` if archive or compressed stream ``src`` passes integrity validation."""
    with tempfile.TemporaryDirectory(prefix="arkit_val_stream_") as tmp_dir:
        ext = Format.parse(fmt).ext if fmt is not None else ".bin"
        tmp_arch = Path(tmp_dir) / f"input{ext}"
        with tmp_arch.open("wb") as out:
            shutil.copyfileobj(src, out, CHUNK_SIZE)
        return bool(validate(tmp_arch, fmt=fmt))


def convert_stream(
    src: BinaryIO,
    dst: BinaryIO,
    fmt: Format | str,
    *,
    level: int | None = None,
    src_fmt: Format | str | None = None,
    threads: int | None = None,
    deterministic: bool = False,
) -> int:
    """Stream-convert ``src`` into ``dst`` with target format ``fmt``."""
    tf = Format.parse(fmt)
    if src_fmt is not None:
        sf = Format.parse(src_fmt)
        if sf.container is None and tf.container is None:
            assert sf.compression is not None and tf.compression is not None
            lvl = resolve_level(tf.compression.value, level)
            with open_compressed_reader(src, sf.compression.value) as reader:
                with open_compressed_writer(dst, tf.compression.value, level=lvl, threads=threads) as writer:
                    shutil.copyfileobj(reader, writer, CHUNK_SIZE)
            return 0

    with tempfile.TemporaryDirectory(prefix="arkit_conv_stream_") as tmp_dir:
        in_ext = Format.parse(src_fmt).ext if src_fmt is not None else ".bin"
        in_file = Path(tmp_dir) / f"in{in_ext}"
        out_file = Path(tmp_dir) / f"out{tf.ext}"
        with in_file.open("wb") as fh:
            shutil.copyfileobj(src, fh, CHUNK_SIZE)
        convert_file(
            in_file,
            out_file,
            fmt=tf,
            level=level,
            src_fmt=src_fmt,
            threads=threads,
            deterministic=deterministic,
        )
        with out_file.open("rb") as rfh:
            shutil.copyfileobj(rfh, dst, CHUNK_SIZE)
        return out_file.stat().st_size

