"""Archive-transparent filesystem walkers (lazy read-only & two-way read/write).

Provides two complementary interfaces unified from ``walkfs`` and ``advanced_walker``:

1. :func:`walk` + :class:`VirtualPath` — Pure generator recursively visiting files,
   directories, and nested archives of arbitrary depth without holding open handles.
   Uses ``SpooledTemporaryFile`` for compound TARs and nested archives so multi-GB
   archives spill gracefully to disk instead of exhausting RAM, and uses stdlib
   :mod:`logging` with zero mandatory third-party runtime dependencies.
2. :class:`AdvancedWalker` (alias :class:`ArchiveWalker`) + :class:`VirtualFile` —
   Two-way context manager allowing callers to inspect and modify files on disk or
   deep inside nested archives (``.zip``, ``.whl``, ``.tar.*``, ``.7z``, ``.gz``,
   ``.bz2``, ``.xz``, ``.zst``, ``.lz4``, ``.br``, ``.snappy``). Rebuilds only dirty
   archives from deepest nesting level outward on exit using streaming chunk copies
   and crash-safe :func:`~arkit.atomicio.atomic_open`.
"""

from __future__ import annotations

import fnmatch
import importlib
import io
import logging
import os
import shutil
import stat as _stat
import tarfile
import tempfile
import zipfile
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from .atomicio import atomic_open
from .codecs import (
    CHUNK_SIZE,
    BinaryIO,
    _require_py7zr,
    codec_available,
    open_compressed_reader,
    open_compressed_writer,
    resolve_level,
)
from .formats import ArchiveFormat, Container, Format, find_archive_format
from .models import StrPath
from .operations import _resolve_container_level, _resolve_fmt
from .tar_stream import open_tar_stream_reader, open_tar_stream_writer

logger = logging.getLogger("arkit.walker")

__all__ = [
    "AdvancedWalker",
    "ArchiveWalker",
    "DEFAULT_SKIP_DIRS",
    "VirtualFile",
    "VirtualPath",
    "logger",
    "walk",
]

DEFAULT_SKIP_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".svn",
        ".hg",
        "__pycache__",
        "node_modules",
        ".tox",
        "venv",
        ".venv",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".cache",
        "dist",
        "build",
        ".eggs",
        ".idea",
        ".vscode",
    }
)

_WALK_SPOOL_BYTES: int = 16 * 1024 * 1024  # 16 MiB before spilling compound/nested archives to temp disk


# ===========================================================================
# Part 1: Read-Only Lazy Generator Walker (`walk` + `VirtualPath`)
# ===========================================================================


class _FileSource:
    """Real file on disk; every ``open()`` opens an independent binary handle."""

    __slots__ = ("path",)

    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def open(self) -> Iterator[BinaryIO]:
        with self.path.open("rb") as fh:
            yield fh


class _SpooledSource:
    """Memory-bounded random-access buffer backed by ``SpooledTemporaryFile``.

    Stays in RAM for payloads <= ``max_size`` and transparently spills to a temporary
    file on disk for large compound TARs or nested archives, returning independent
    readable views on each ``open()`` call.
    """

    __slots__ = ("_spool", "_size")

    def __init__(self, stream: BinaryIO, *, max_size: int = _WALK_SPOOL_BYTES) -> None:
        self._spool: Any = tempfile.SpooledTemporaryFile(max_size=max_size, mode="w+b")
        self._size = 0
        while chunk := stream.read(CHUNK_SIZE):
            self._spool.write(chunk)
            self._size += len(chunk)
        self._spool.seek(0)

    @contextmanager
    def open(self) -> Iterator[BinaryIO]:
        # Create an independent read cursor by duplicating or slicing without closing the shared spool
        self._spool.seek(0)
        if not getattr(self._spool, "_rolled", False):
            yield io.BytesIO(self._spool.read())
        else:
            dup_fd = os.dup(self._spool.fileno())
            with os.fdopen(dup_fd, "rb") as fh:
                fh.seek(0)
                yield fh

    def __del__(self) -> None:
        try:
            self._spool.close()
        except Exception:
            pass


@dataclass(frozen=True, slots=True)
class _WalkMember:
    name: str
    raw: str
    is_dir: bool
    is_symlink: bool = False
    size: int | None = None


def _norm_walk_name(name: str) -> str | None:
    parts = [p for p in name.replace("\\", "/").split("/") if p not in ("", ".")]
    return "/".join(parts) if parts else None


class _ZipWalkReader:
    __slots__ = ("_source",)

    def __init__(self, source: _FileSource | _SpooledSource) -> None:
        self._source = source

    def members(self) -> list[_WalkMember]:
        out: list[_WalkMember] = []
        seen: set[tuple[str, bool]] = set()
        encrypted = False
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            zf = stack.enter_context(zipfile.ZipFile(fh, "r"))
            for zi in zf.infolist():
                name = _norm_walk_name(zi.filename)
                if name is None:
                    continue
                mode = (zi.external_attr >> 16) & 0o170000
                is_dir = zi.is_dir() or mode == 0o040000
                is_symlink = mode == 0o120000
                if zi.flag_bits & 0x1:
                    encrypted = True
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _WalkMember(
                        name=name,
                        raw=zi.filename,
                        is_dir=is_dir,
                        is_symlink=is_symlink,
                        size=None if is_dir else zi.file_size,
                    )
                )
        if encrypted:
            logger.warning("Archive contains password-protected ZIP members")
        return out

    @contextmanager
    def open_member(self, member: _WalkMember) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            zf = stack.enter_context(zipfile.ZipFile(fh, "r"))
            try:
                stream = stack.enter_context(zf.open(member.raw, "r"))
            except RuntimeError as exc:
                raise RuntimeError(f"Cannot open {member.raw!r}: {exc}") from exc
            yield stream


class _TarWalkReader:
    __slots__ = ("_source",)

    def __init__(
        self,
        source: _FileSource | _SpooledSource,
        fmt: ArchiveFormat,
        *,
        spool_max_size: int = _WALK_SPOOL_BYTES,
    ) -> None:
        if fmt.codec is None:
            self._source: _FileSource | _SpooledSource = source
        elif fmt.codec == "7z":
            py7zr = _require_py7zr()
            pio = importlib.import_module("py7zr.io")
            with ExitStack() as stack:
                fh = stack.enter_context(source.open())
                sz = stack.enter_context(py7zr.SevenZipFile(fh, "r"))
                files = [f for f in sz.list() if not f.is_directory]
                if len(files) != 1:
                    raise ValueError(f".tar.7z must contain 1 tar member, found {len(files)}")
                factory = pio.BytesIOFactory(limit=1 << 30)
                sz.extract(targets=[files[0].filename], factory=factory)
                bio = factory.products[files[0].filename]
                self._source = _SpooledSource(bio, max_size=spool_max_size)
        else:
            with ExitStack() as stack:
                fh = stack.enter_context(source.open())
                creader = stack.enter_context(open_compressed_reader(fh, fmt.codec))
                self._source = _SpooledSource(creader, max_size=spool_max_size)

    def members(self) -> list[_WalkMember]:
        out: list[_WalkMember] = []
        seen: set[tuple[str, bool]] = set()
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            tf = stack.enter_context(tarfile.open(fileobj=fh, mode="r:"))
            for m in tf.getmembers():
                name = _norm_walk_name(m.name)
                if name is None:
                    continue
                is_dir, is_symlink = m.isdir(), m.issym()
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _WalkMember(
                        name=name,
                        raw=m.name,
                        is_dir=is_dir,
                        is_symlink=is_symlink,
                        size=None if is_dir else m.size,
                    )
                )
        return out

    @contextmanager
    def open_member(self, member: _WalkMember) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            tf = stack.enter_context(tarfile.open(fileobj=fh, mode="r:"))
            try:
                ti = tf.getmember(member.raw)
            except KeyError:
                raise KeyError(f"Member {member.raw!r} not found") from None
            fobj = tf.extractfile(ti)
            if fobj is None:
                raise OSError(f"Member {member.name!r} has no regular file payload")
            stack.enter_context(closing(fobj))
            yield fobj


class _SevenZipWalkReader:
    __slots__ = ("_source",)

    def __init__(self, source: _FileSource | _SpooledSource) -> None:
        self._source = source

    def members(self) -> list[_WalkMember]:
        py7zr = _require_py7zr()
        out: list[_WalkMember] = []
        seen: set[tuple[str, bool]] = set()
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            sz = stack.enter_context(py7zr.SevenZipFile(fh, "r"))
            for info in sz.list():
                raw = getattr(info, "filename", None)
                if not raw:
                    continue
                name = _norm_walk_name(raw)
                if name is None:
                    continue
                is_dir = bool(getattr(info, "is_directory", False))
                uncomp = getattr(info, "uncompressed", None)
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _WalkMember(
                        name=name,
                        raw=raw,
                        is_dir=is_dir,
                        is_symlink=False,
                        size=None if is_dir else (int(uncomp) if uncomp is not None else None),
                    )
                )
        return out

    @contextmanager
    def open_member(self, member: _WalkMember) -> Iterator[BinaryIO]:
        py7zr = _require_py7zr()
        pio = importlib.import_module("py7zr.io")
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            sz = stack.enter_context(py7zr.SevenZipFile(fh, "r"))
            factory = pio.BytesIOFactory(limit=1 << 30)
            sz.extract(targets=[member.raw], factory=factory)
            bio = factory.products.get(member.raw)
            yield bio if bio is not None else io.BytesIO()


class _StreamWalkReader:
    __slots__ = ("_source", "_codec", "_inner")

    def __init__(
        self,
        source: _FileSource | _SpooledSource,
        fmt: ArchiveFormat,
        base_name: str,
    ) -> None:
        assert fmt.codec is not None
        self._source = source
        self._codec: str = fmt.codec
        inner = base_name
        for suf in fmt.suffixes:
            if inner.lower().endswith(suf) and len(inner) > len(suf):
                inner = inner[: -len(suf)]
                break
        else:
            inner = f"{base_name}.decoded"
        self._inner = inner

    def members(self) -> list[_WalkMember]:
        return [_WalkMember(name=self._inner, raw=self._inner, is_dir=False, is_symlink=False, size=None)]

    @contextmanager
    def open_member(self, member: _WalkMember) -> Iterator[BinaryIO]:
        _ = member
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            stream = stack.enter_context(open_compressed_reader(fh, self._codec))
            yield stream


class _ManagedStream:
    """Readable binary stream owning its underlying archive ``ExitStack``."""

    __slots__ = ("_stream", "_stack")

    def __init__(self, stream: BinaryIO, stack: ExitStack) -> None:
        self._stream = stream
        self._stack = stack

    def __getattr__(self, name: str) -> Any:
        if name in ("_stream", "_stack"):
            raise AttributeError(name)
        return getattr(self._stream, name)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        try:
            return bool(self._stream.seekable())
        except Exception:
            return False

    def close(self) -> None:
        self._stack.close()

    def __enter__(self) -> _ManagedStream:
        return self

    def __exit__(self, *exc_info: object) -> Literal[False]:
        self.close()
        return False

    def __del__(self) -> None:
        try:
            self._stack.close()
        except Exception:
            pass

    def __iter__(self) -> Iterator[bytes]:
        return iter(self._stream)

    def __repr__(self) -> str:
        return f"<_ManagedStream {self._stream!r}>"


class VirtualPath:
    """Pathlib-flavored virtual path representing a real filesystem or nested archive entry.

    Intentionally omits ``__fspath__`` because virtual entries inside archives are not
    direct OS paths; access content via :meth:`open`, :meth:`read_bytes`, or :meth:`read_text`.
    """

    __slots__ = (
        "_depth",
        "_is_symlink",
        "_kind",
        "_opener",
        "_origin",
        "_parts",
        "_real",
        "_size",
        "_str",
    )

    def __init__(
        self,
        parts: tuple[str, ...] | list[str],
        *,
        origin: StrPath,
        real: Path | None = None,
        depth: int = 0,
        kind: str = "file",
        is_symlink: bool = False,
        size: int | None = None,
        opener: Callable[[], Any] | None = None,
    ) -> None:
        self._parts: tuple[str, ...] = tuple(str(p) for p in parts)
        self._origin: Path = Path(origin)
        self._real: Path | None = Path(real) if real is not None else None
        self._depth: int = int(depth)
        self._kind: str = kind
        self._is_symlink: bool = bool(is_symlink)
        self._size: int | None = size
        self._opener: Callable[[], Any] | None = opener
        self._str: str = str(PurePosixPath(*self._parts)) if self._parts else "."

    def as_posix(self) -> str:
        return self._str

    def __str__(self) -> str:
        return self._str

    def __repr__(self) -> str:
        return f"VirtualPath({self._str!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, VirtualPath) and self._origin == other._origin and self._parts == other._parts

    def __hash__(self) -> int:
        return hash((self._origin, self._parts))

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, VirtualPath):
            return NotImplemented
        return self._parts < other._parts

    def __truediv__(self, other: StrPath) -> VirtualPath:
        if isinstance(other, VirtualPath):
            raise TypeError("Cannot append a VirtualPath; append a str or PathLike instead")
        text = os.fspath(other) if isinstance(other, os.PathLike) else str(other)
        extra = tuple(p for p in text.split("/") if p not in ("", "."))
        return VirtualPath(
            self._parts + extra,
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind="file",
        )

    @property
    def parts(self) -> tuple[str, ...]:
        return self._parts

    @property
    def name(self) -> str:
        return self._parts[-1] if self._parts else ""

    @property
    def stem(self) -> str:
        return PurePosixPath(self.name).stem

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.name).suffix

    @property
    def suffixes(self) -> list[str]:
        return PurePosixPath(self.name).suffixes

    @property
    def parent(self) -> VirtualPath:
        if self._parts == ("/",):
            return self
        if not self._parts:
            return VirtualPath((), origin=self._origin, real=self._real, depth=self._depth, kind="dir")
        return VirtualPath(
            self._parts[:-1],
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind="dir",
        )

    @property
    def parents(self) -> Iterator[VirtualPath]:
        current = self
        while len(current._parts) > 1:
            current = current.parent
            yield current

    def match(self, pattern: str) -> bool:
        return PurePosixPath(*self._parts).match(pattern)

    def relative_to(self, other: StrPath | VirtualPath) -> VirtualPath:
        if isinstance(other, VirtualPath):
            base = other._parts
        else:
            base = PurePosixPath(Path(other).as_posix()).parts
        if self._parts[: len(base)] != base:
            raise ValueError(f"{self} is not relative to {other}")
        return VirtualPath(
            self._parts[len(base) :],
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind=self._kind,
            is_symlink=self._is_symlink,
            size=self._size,
            opener=self._opener,
        )

    @property
    def origin(self) -> Path:
        return self._origin

    @property
    def real(self) -> Path | None:
        return self._real

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def in_archive(self) -> bool:
        return self._depth > 0

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def size(self) -> int | None:
        return self._size

    def is_dir(self) -> bool:
        return self._kind == "dir"

    def is_file(self) -> bool:
        return self._kind == "file"

    def is_symlink(self) -> bool:
        return self._is_symlink

    def open(self, mode: str = "rb") -> _ManagedStream:
        """Open a lazy readable binary stream for this entry."""
        if mode not in ("r", "rb"):
            raise ValueError(f"Unsupported mode {mode!r}: only binary reading ('rb'/'r') is supported")
        if self._kind == "dir":
            raise IsADirectoryError(f"{self} is a directory")
        if self._kind == "symlink":
            raise OSError(f"{self} is an unfollowed symlink entry")
        if self._kind == "special":
            raise OSError(f"{self} is a special file (fifo/socket/device)")
        if self._opener is None:
            raise ValueError(f"{self} has no backing content opener")
        stack = ExitStack()
        stream = stack.enter_context(self._opener())
        return _ManagedStream(stream, stack)

    def read_bytes(self) -> bytes:
        with self.open() as fh:
            return fh.read()  # type: ignore[no-any-return]

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self.read_bytes().decode(encoding, errors)


def _real_opener(path: Path) -> Callable[[], Any]:
    @contextmanager
    def _open() -> Iterator[BinaryIO]:
        with path.open("rb") as fh:
            yield fh

    return _open


def _make_walk_reader(
    source: _FileSource | _SpooledSource,
    fmt: ArchiveFormat,
    base_name: str,
    spool_max_size: int,
) -> Any:
    if fmt.kind == "zip":
        return _ZipWalkReader(source)
    if fmt.kind == "tar":
        return _TarWalkReader(source, fmt, spool_max_size=spool_max_size)
    if fmt.kind == "7z":
        return _SevenZipWalkReader(source)
    if fmt.kind == "stream":
        return _StreamWalkReader(source, fmt, base_name)
    raise ValueError(f"Unknown format kind: {fmt.kind!r}")


def walk(
    root: StrPath,
    *,
    max_depth: int = 3,
    include_dirs: bool = True,
    follow_symlinks: bool = False,
    skip_dirs: set[str] | frozenset[str] | None = None,
    pattern: str | None = None,
    spool_max_size: int = _WALK_SPOOL_BYTES,
) -> Iterator[VirtualPath]:
    """Lazily walk ``root`` and transparently descend into archives (including nested archives).

    Errors inside unreadable or corrupt archives are logged via ``arkit.walker`` and
    gracefully degrade (treating the archive as a regular file) without aborting the walk.
    """
    root_path = Path(root)
    try:
        if not (root_path.is_symlink() or root_path.exists()):
            logger.error("Walk root does not exist: %s", root_path)
            return
    except OSError as exc:
        logger.error("Cannot access walk root %s: %s", root_path, exc)
        return

    root_parts = PurePosixPath(root_path.as_posix()).parts
    ancestry: list[Path] = []

    def _emit(vp: VirtualPath) -> Iterator[VirtualPath]:
        if pattern is None or vp.match(pattern) or fnmatch.fnmatch(vp.name, pattern):
            yield vp

    def _real_kind(path: Path) -> tuple[str, bool, int | None]:
        try:
            st = path.lstat()
        except OSError as exc:
            logger.warning("Cannot stat %s: %s", path, exc)
            return "file", False, None
        is_link = _stat.S_ISLNK(st.st_mode)
        if is_link and follow_symlinks:
            try:
                target = path.stat()
            except OSError as exc:
                logger.warning("Cannot stat symlink target %s: %s", path, exc)
                return "symlink", True, None
            if _stat.S_ISDIR(target.st_mode):
                return "dir", True, None
            if _stat.S_ISREG(target.st_mode):
                return "file", True, target.st_size
            return "special", True, target.st_size
        if is_link:
            return "symlink", True, st.st_size
        if _stat.S_ISDIR(st.st_mode):
            return "dir", False, None
        if _stat.S_ISREG(st.st_mode):
            return "file", False, st.st_size
        return "special", False, st.st_size

    def _walk_dir(dir_path: Path, base_parts: tuple[str, ...]) -> Iterator[VirtualPath]:
        try:
            children = sorted(dir_path.iterdir(), key=lambda p: p.name)
        except OSError as exc:
            logger.error("Cannot list directory %s: %s", dir_path, exc)
            return
        for child in children:
            if skip_dirs and child.name in skip_dirs and child.is_dir():
                continue
            try:
                yield from _walk_entry(child, base_parts + (child.name,))
            except Exception as exc:
                logger.error("Unexpected error visiting %s: %s", child, exc)

    def _walk_entry(path: Path, parts: tuple[str, ...]) -> Iterator[VirtualPath]:
        kind, is_link, size = _real_kind(path)
        entry = VirtualPath(
            parts,
            origin=root_path,
            real=path,
            depth=0,
            kind=kind,
            is_symlink=is_link,
            size=size,
            opener=_real_opener(path) if kind == "file" else None,
        )
        if kind == "dir":
            resolved = None
            if follow_symlinks:
                try:
                    resolved = path.resolve()
                except OSError:
                    resolved = None
                if resolved is not None and resolved in ancestry:
                    logger.warning("Symlink loop detected at %s -- not descending", path)
                    if include_dirs:
                        yield from _emit(entry)
                    return
            if include_dirs:
                yield from _emit(entry)
            if resolved is not None:
                ancestry.append(resolved)
            yield from _walk_dir(path, parts)
            if resolved is not None:
                ancestry.pop()
            return

        yield from _emit(entry)
        if kind != "file":
            return
        fmt = find_archive_format(path.name)
        if fmt is not None and max_depth >= 1:
            yield from _walk_archive(_FileSource(path), fmt, parts, path, 1)

    def _walk_archive(
        source: _FileSource | _SpooledSource,
        fmt: ArchiveFormat,
        arch_parts: tuple[str, ...],
        arch_real: Path,
        depth: int,
    ) -> Iterator[VirtualPath]:
        if fmt.codec is not None and not codec_available(fmt.codec):
            logger.warning("Codec %r is not installed -- treating %s as plain file", fmt.codec, "/".join(arch_parts))
            return
        if fmt.kind == "7z" and not codec_available("7z"):
            logger.warning("Codec '7z' is not installed -- treating %s as plain file", "/".join(arch_parts))
            return
        try:
            reader = _make_walk_reader(source, fmt, arch_parts[-1], spool_max_size)
            members = reader.members()
        except Exception as exc:
            logger.error(
                "Cannot open %s archive %s (%s: %s) -- treating as plain file",
                fmt.name,
                "/".join(arch_parts),
                type(exc).__name__,
                exc,
            )
            return

        for member in members:
            mparts = arch_parts + tuple(member.name.split("/"))
            entry = VirtualPath(
                mparts,
                origin=root_path,
                real=arch_real,
                depth=depth,
                kind="dir" if member.is_dir else ("symlink" if member.is_symlink else "file"),
                is_symlink=member.is_symlink,
                size=member.size,
                opener=None if (member.is_dir or member.is_symlink) else partial(reader.open_member, member),
            )
            if member.is_dir:
                if include_dirs:
                    yield from _emit(entry)
                continue
            yield from _emit(entry)
            if member.is_symlink:
                continue
            inner_fmt = find_archive_format(member.name)
            if inner_fmt is not None and depth + 1 <= max_depth:
                try:
                    with reader.open_member(member) as fh:
                        nested = _SpooledSource(fh, max_size=spool_max_size)
                except Exception as exc:
                    logger.error("Cannot read nested archive %s (%s: %s)", "/".join(mparts), type(exc).__name__, exc)
                    continue
                yield from _walk_archive(nested, inner_fmt, mparts, arch_real, depth + 1)

    try:
        root_is_link = root_path.is_symlink()
        root_is_dir = root_path.is_dir()
    except OSError:
        root_is_link, root_is_dir = False, False

    if root_is_dir and (follow_symlinks or not root_is_link):
        if follow_symlinks:
            try:
                ancestry.append(root_path.resolve())
            except OSError:
                pass
        yield from _walk_dir(root_path, root_parts)
    else:
        yield from _walk_entry(root_path, root_parts)


# ===========================================================================
# Part 2: Two-Way Read/Write Filesystem & Nested Archive Walker (`AdvancedWalker`)
# ===========================================================================


class VirtualFile:
    """Unified read/write handle for a filesystem file or an archive member.

    Unlike legacy ``VirtualFile``, ``read_bytes()`` does not retain an unbounded
    copy in RAM unless ``cache_reads=True`` is requested, keeping peak memory flat
    when scanning thousands of entries.
    """

    __slots__ = ("_cache", "_cache_reads", "_read_func", "_write_func", "path")

    def __init__(
        self,
        path: str,
        *,
        read_func: Callable[[], bytes],
        write_func: Callable[[bytes], None] | None = None,
        cache_reads: bool = False,
    ) -> None:
        self.path: str = path
        self._read_func = read_func
        self._write_func = write_func
        self._cache_reads = cache_reads
        self._cache: bytes | None = None

    def read_bytes(self) -> bytes:
        """Read current bytes of this file or archive member."""
        if self._cache is not None:
            return self._cache
        data = self._read_func()
        if self._cache_reads:
            self._cache = data
        return data

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Decode current content as text."""
        return self.read_bytes().decode(encoding, errors)

    def write_bytes(self, data: bytes) -> None:
        """Record modified bytes to be written atomically (or rebuilt on walker exit)."""
        if self._write_func is None:
            raise RuntimeError(f"Read-only entry: {self.path}")
        if self._cache_reads:
            self._cache = data
        self._write_func(data)

    def write_text(self, text: str, encoding: str = "utf-8", errors: str = "strict") -> None:
        """Encode ``text`` and record modification."""
        self.write_bytes(text.encode(encoding, errors))

    def __repr__(self) -> str:
        return f"VirtualFile({self.path!r})"


class _SpoolPayload:
    """Stores a modified archive member in RAM up to 4 MiB, spilling larger blobs to disk."""

    __slots__ = ("_spool", "size")

    def __init__(self, data: bytes) -> None:
        self.size = len(data)
        self._spool: Any = tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024, mode="w+b")
        self._spool.write(data)
        self._spool.seek(0)

    def read_bytes(self) -> bytes:
        self._spool.seek(0)
        return bytes(self._spool.read())

    @contextmanager
    def open_reader(self) -> Iterator[BinaryIO]:
        self._spool.seek(0)
        yield self._spool

    def close(self) -> None:
        self._spool.close()


class _ArchiveEditHandler:
    """Internal handler managing lazy reads, spooled updates, and streaming rebuilds for one archive."""

    def __init__(
        self,
        source: Path | bytes,
        fmt: Format,
        depth: int,
        parent: _ArchiveEditHandler | None = None,
        parent_member: str | None = None,
    ) -> None:
        self.source: Path | bytes = source
        self.fmt: Format = fmt
        self.depth: int = depth
        self.parent: _ArchiveEditHandler | None = parent
        self.parent_member: str | None = parent_member
        self.is_top_level: bool = isinstance(source, Path)
        self.top_path: Path | None = source if isinstance(source, Path) else None
        self.updates: dict[str, _SpoolPayload] = {}
        self.dirty: bool = False

    def update(self, member: str, data: bytes) -> None:
        old = self.updates.pop(member, None)
        if old is not None:
            old.close()
        self.updates[member] = _SpoolPayload(data)
        self.dirty = True

    def list_members(self) -> list[str]:
        if self.fmt.container is None:
            return ["content"]
        if self.fmt.container is Container.ZIP:
            z_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            with zipfile.ZipFile(z_src, "r") as zf:
                return [zi.filename for zi in zf.infolist() if not zi.is_dir()]
        if self.fmt.container is Container.TAR:
            t_src: StrPath | BinaryIO = (
                self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            )
            with open_tar_stream_reader(t_src, self.fmt) as tf:
                return [m.name for m in tf if m.isfile()]
        if self.fmt.container is Container.SEVENZ:
            py7zr = _require_py7zr()
            s_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            with py7zr.SevenZipFile(s_src, "r") as sz:
                return [fi.filename for fi in sz.list() if not fi.is_directory]
        return []

    def read_member_bytes(self, member: str) -> bytes:
        if member in self.updates:
            return self.updates[member].read_bytes()

        if self.fmt.container is None:
            assert self.fmt.compression is not None
            raw_in: BinaryIO = (
                self.source.open("rb") if isinstance(self.source, Path) else io.BytesIO(self.source)
            )
            try:
                with open_compressed_reader(raw_in, self.fmt.compression.value) as reader:
                    return reader.read()
            finally:
                if isinstance(self.source, Path):
                    raw_in.close()

        if self.fmt.container is Container.ZIP:
            z_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            with zipfile.ZipFile(z_src, "r") as zf:
                return zf.read(member)

        if self.fmt.container is Container.TAR:
            t_src: StrPath | BinaryIO = (
                self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            )
            with open_tar_stream_reader(t_src, self.fmt) as tf:
                for m in tf:
                    if m.isfile() and m.name == member:
                        ef = tf.extractfile(m)
                        return ef.read() if ef is not None else b""
            return b""

        if self.fmt.container is Container.SEVENZ:
            py7zr = _require_py7zr()
            pio = importlib.import_module("py7zr.io")
            s_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            with py7zr.SevenZipFile(s_src, "r") as sz:
                factory = pio.BytesIOFactory(limit=1 << 30)
                sz.extract(targets=[member], factory=factory)
                bio = factory.products.get(member)
                return bio.read() if bio is not None else b""
        return b""

    def write_rebuilt_into(self, out_stream: BinaryIO) -> None:
        """Stream-rebuild this archive into ``out_stream``, copying untouched members in chunks."""
        lvl = _resolve_container_level(self.fmt, None)

        if self.fmt.container is None:
            assert self.fmt.compression is not None
            payload_obj = self.updates.get("content")
            with open_compressed_writer(out_stream, self.fmt.compression.value, level=lvl) as writer:
                if payload_obj is not None:
                    with payload_obj.open_reader() as r_fh:
                        shutil.copyfileobj(r_fh, writer, CHUNK_SIZE)
                else:
                    writer.write(self.read_member_bytes("content"))
            return

        if self.fmt.container is Container.ZIP:
            comp = zipfile.ZIP_DEFLATED
            z_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            with zipfile.ZipFile(z_src, "r") as zin, zipfile.ZipFile(
                out_stream, "w", compression=comp, allowZip64=True
            ) as zout:
                for info in zin.infolist():
                    if info.is_dir():
                        continue
                    new_zi = zipfile.ZipInfo(filename=info.filename, date_time=info.date_time)
                    new_zi.compress_type = info.compress_type or comp
                    new_zi.external_attr = info.external_attr
                    upd = self.updates.get(info.filename)
                    if upd is not None:
                        with upd.open_reader() as r_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                            shutil.copyfileobj(r_fh, d_fh, CHUNK_SIZE)
                    else:
                        with zin.open(info, "r") as s_fh, zout.open(new_zi, "w", force_zip64=True) as d_fh:
                            shutil.copyfileobj(s_fh, d_fh, CHUNK_SIZE)
            return

        if self.fmt.container is Container.TAR:
            t_src: StrPath | BinaryIO = (
                self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
            )
            with open_tar_stream_reader(t_src, self.fmt) as tin, open_tar_stream_writer(
                out_stream, self.fmt, level=lvl
            ) as tout:
                for member in tin:
                    if not member.isfile():
                        tout.addfile(member)
                        continue
                    upd = self.updates.get(member.name)
                    ti = tarfile.TarInfo(name=member.name)
                    ti.mtime = member.mtime
                    ti.mode = member.mode
                    ti.uid = member.uid
                    ti.gid = member.gid
                    if upd is not None:
                        ti.size = upd.size
                        with upd.open_reader() as r_fh:
                            tout.addfile(ti, r_fh)
                    else:
                        ti.size = member.size
                        ef = tin.extractfile(member)
                        tout.addfile(ti, ef)
            return

        if self.fmt.container is Container.SEVENZ:
            py7zr = _require_py7zr()
            filters = [{"id": py7zr.FILTER_LZMA2, "preset": resolve_level("7z", None)}]
            with tempfile.TemporaryDirectory(prefix="arkit_walk7z_") as tmp_dir:
                staging = Path(tmp_dir)
                s_src: Any = self.source if isinstance(self.source, Path) else io.BytesIO(self.source)
                with py7zr.SevenZipFile(s_src, "r") as sz_in:
                    names = [fi.filename for fi in sz_in.list() if not fi.is_directory]
                    sz_in.extractall(path=staging)
                for name, upd in self.updates.items():
                    target = staging / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with upd.open_reader() as r_fh, target.open("wb") as w_fh:
                        shutil.copyfileobj(r_fh, w_fh, CHUNK_SIZE)
                with py7zr.SevenZipFile(out_stream, "w", filters=filters) as sz_out:
                    for name in names:
                        fp = staging / name
                        if fp.is_file():
                            sz_out.write(fp, arcname=name)

    def close(self) -> None:
        for upd in self.updates.values():
            upd.close()
        self.updates.clear()


class AdvancedWalker:
    """Two-way context manager & iterator yielding :class:`VirtualFile` entries across
    directories, archives, and nested archives.

    All pending modifications recorded via ``entry.write_bytes(...)`` are atomically
    flushed on clean ``with``-block exit (rebuilding nested archives deepest-first).
    """

    def __init__(
        self,
        root: StrPath = ".",
        *,
        skip_dirs: set[str] | frozenset[str] | None = None,
        max_archive_depth: int = 4,
        expand_archives: bool = True,
        cache_reads: bool = False,
    ) -> None:
        self.root: Path = Path(root).resolve()
        self.skip_dirs: set[str] | frozenset[str] = (
            skip_dirs if skip_dirs is not None else DEFAULT_SKIP_DIRS
        )
        self.max_archive_depth: int = max_archive_depth
        self.expand_archives: bool = expand_archives
        self.cache_reads: bool = cache_reads
        self._handlers: list[_ArchiveEditHandler] = []

    def __enter__(self) -> AdvancedWalker:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is None:
                self.flush()
        finally:
            for h in self._handlers:
                h.close()
            self._handlers.clear()

    def __iter__(self) -> Iterator[VirtualFile]:
        if self.root.is_file():
            name = self.root.name
            if self.expand_archives and find_archive_format(name) is not None:
                yield from self._expand_archive(self.root, name, depth=0)
            else:
                target_file = self.root
                yield VirtualFile(
                    path=name,
                    read_func=target_file.read_bytes,
                    write_func=partial(self._write_disk_file, target_file),
                    cache_reads=self.cache_reads,
                )
            return
        yield from self._walk_fs(self.root)

    @staticmethod
    def _write_disk_file(path: Path, data: bytes) -> None:
        with atomic_open(path, "wb") as fh:
            fh.write(data)

    def flush(self) -> None:
        """Rebuild every modified archive (deepest nesting level first) atomically."""
        # Propagate dirty flags upward first so sorting covers modified ancestors
        ordered = sorted(self._handlers, key=lambda h: -h.depth)
        for h in ordered:
            if not h.dirty:
                continue
            if h.parent is not None and h.parent_member is not None:
                buf = io.BytesIO()
                h.write_rebuilt_into(buf)
                h.parent.update(h.parent_member, buf.getvalue())
            elif h.is_top_level and h.top_path is not None:
                # Top-level archive: stream-rebuild into a temp file FIRST (since reading
                # untouched members reads from h.top_path!), then atomically replace!
                with atomic_open(h.top_path, "wb") as out_fh:
                    h.write_rebuilt_into(out_fh)
            h.dirty = False

    def _walk_fs(self, directory: Path) -> Iterator[VirtualFile]:
        for dirpath, dirnames, filenames in os.walk(directory, followlinks=False):
            dirnames.sort()
            filenames.sort()
            dirnames[:] = [
                d
                for d in dirnames
                if d not in self.skip_dirs and not (Path(dirpath) / d).is_symlink()
            ]
            for name in filenames:
                path = Path(dirpath) / name
                if path.is_symlink() or not path.is_file():
                    continue
                rel = path.relative_to(self.root).as_posix()
                if self.expand_archives and find_archive_format(name) is not None:
                    yield from self._expand_archive(path, rel, depth=0)
                else:
                    yield VirtualFile(
                        path=rel,
                        read_func=path.read_bytes,
                        write_func=partial(self._write_disk_file, path),
                        cache_reads=self.cache_reads,
                    )

    def _expand_archive(
        self,
        source: Path | bytes,
        virtual_prefix: str,
        depth: int,
        parent: _ArchiveEditHandler | None = None,
        parent_member: str | None = None,
    ) -> Iterator[VirtualFile]:
        if depth > self.max_archive_depth:
            return

        name_hint = virtual_prefix.split("!")[-1] if "!" in virtual_prefix else virtual_prefix
        af = find_archive_format(name_hint)
        if af is None or af.format_enum is None:
            return
        if af.codec is not None and not codec_available(af.codec):
            return
        if af.kind == "7z" and not codec_available("7z"):
            return

        try:
            fmt = _resolve_fmt(source, af.format_enum)
            handler = _ArchiveEditHandler(source, fmt, depth, parent, parent_member)
            members = handler.list_members()
        except Exception:
            return

        self._handlers.append(handler)

        if fmt.container is None:
            yield VirtualFile(
                path=virtual_prefix,
                read_func=partial(handler.read_member_bytes, "content"),
                write_func=partial(handler.update, "content"),
                cache_reads=self.cache_reads,
            )
            return

        for member_name in members:
            vpath = f"{virtual_prefix}!{member_name}"
            if find_archive_format(member_name) is not None and depth < self.max_archive_depth:
                try:
                    nested_bytes = handler.read_member_bytes(member_name)
                except Exception:
                    continue
                yield from self._expand_archive(
                    nested_bytes,
                    vpath,
                    depth + 1,
                    parent=handler,
                    parent_member=member_name,
                )
            else:
                yield VirtualFile(
                    path=vpath,
                    read_func=partial(handler.read_member_bytes, member_name),
                    write_func=partial(handler.update, member_name),
                    cache_reads=self.cache_reads,
                )


#: Alias matching ``archive_walker.py``.
ArchiveWalker = AdvancedWalker
