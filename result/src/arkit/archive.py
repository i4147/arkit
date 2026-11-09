"""Object-oriented ``Archive`` interface and ``open_archive`` context manager.

Unifies the ``Archive`` classes from ``arutils`` (path- or bytes-backed archive
wrapper) and ``core`` (reusable compressor/decompressor configuration) so both
patterns work seamlessly.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from .exceptions import ArchiveError
from .formats import Format
from .models import (
    ArchiveInfo,
    CompressionStats,
    DiffReport,
    Member,
    MemberInfo,
    MembersInput,
    SafetyPolicy,
    StrPath,
)
from .operations import (
    _resolve_fmt,
    add_member,
    compress,
    convert_bytes,
    convert_file,
    decompress,
    decompress_bytes,
    diff_archives,
    extract,
    info,
    iter_members,
    list_members,
    pack_file,
    read_member,
    read_members,
    remove_member,
    validate,
)
from .split import split, split_file

__all__ = [
    "Archive",
    "open_archive",
]


class Archive:
    """High-level object-oriented handle for an archive (path-backed or bytes-backed)
    or a configured compressor/decompressor engine.

    Supports two construction modes:
    1. **Bound archive handle** (``Archive("bundle.tar.zst")`` or ``Archive(raw_bytes)``):
       exposes ``.members()``, ``.read()``, ``.read_all()``, ``.extract()``, ``.add()``,
       ``.remove()``, ``.info()``, ``.validate()``, ``.convert()``, ``.split()``, and iteration.
    2. **Configured engine** (``Archive(compression_level=6, threads=4)``):
       exposes ``.compress(src, dst, format_type=...)`` and ``.decompress(src, dst, validate=...)``
       for full compatibility with ``core.Archive``.
    """

    CHUNK_SIZE: int = 1024 * 1024

    def __init__(
        self,
        source: StrPath | bytes | None = None,
        fmt: Format | str | None = None,
        *,
        compression_level: int | None = None,
        threads: int = 1,
    ) -> None:
        self.compression_level: int | None = compression_level
        self.threads: int = max(1, threads)
        self._path: Path | None = None
        self._blob: bytes | None = None
        self._fmt: Format | None = None

        if source is not None:
            self._fmt = _resolve_fmt(source, fmt)
            if isinstance(source, bytes):
                self._blob = source
            else:
                self._path = Path(source)

    @property
    def format(self) -> Format:
        """Resolved :class:`~arkit.formats.Format` of this bound archive."""
        if self._fmt is None:
            raise ArchiveError("Archive instance is not bound to a source")
        return self._fmt

    @property
    def path(self) -> Path | None:
        """Filesystem :class:`~pathlib.Path` if bound to a file, else ``None``."""
        return self._path

    def _source(self) -> Path | bytes:
        if self._path is not None:
            return self._path
        if self._blob is not None:
            return self._blob
        raise ArchiveError("Archive instance has no bound source")

    # -- Engine mode methods (core.Archive compatibility) -------------------

    def compress(
        self,
        source_path: StrPath,
        archive_path: StrPath,
        format_type: Format | str | None = None,
    ) -> CompressionStats:
        """Compress ``source_path`` into ``archive_path`` using engine settings."""
        return compress(
            source_path,
            archive_path,
            format=format_type,
            level=self.compression_level,
            threads=self.threads,
        )

    def decompress(
        self,
        archive_path: StrPath,
        output_dir: StrPath,
        validate_first: bool = False,
        *,
        safe: bool = True,
        policy: SafetyPolicy | None = None,
    ) -> Path:
        """Decompress ``archive_path`` into ``output_dir`` using engine settings."""
        if validate_first:
            report = validate(archive_path)
            if not report.healthy:
                raise ArchiveError(f"Archive failed validation: {'; '.join(report.errors)}")
        return decompress(archive_path, output_dir, safe=safe, policy=policy)

    # -- Bound archive handle methods (arutils.Archive compatibility) -------

    def members(self) -> list[MemberInfo]:
        """List member metadata entries without loading full member payloads."""
        return list_members(self._source(), self.format)

    def read(self, name: str) -> bytes:
        """Read bytes of member ``name``."""
        return read_member(self._source(), name, self.format)

    def read_all(self) -> dict[str, bytes]:
        """Read all members into a ``{name: bytes}`` dictionary."""
        return read_members(self._source(), self.format)

    def extract(
        self,
        dest: StrPath,
        members: Iterable[str] | None = None,
        *,
        safe: bool = True,
        policy: SafetyPolicy | None = None,
        strip_components: int = 0,
    ) -> list[Path]:
        """Safely stream-extract this archive into ``dest``."""
        return extract(
            self._source(),
            dest,
            self.format,
            members,
            safe=safe,
            policy=policy,
            strip_components=strip_components,
        )

    def add(self, name: str, data: bytes, level: int | None = None) -> None:
        """Add ``(name, data)`` to this file-backed archive atomically."""
        if self._path is None:
            raise ArchiveError("Bytes-backed or unbound archives are immutable")
        add_member(self._path, name, data, self.format, level if level is not None else self.compression_level)

    def remove(self, name: str, level: int | None = None) -> None:
        """Remove ``name`` from this file-backed archive atomically."""
        if self._path is None:
            raise ArchiveError("Bytes-backed or unbound archives are immutable")
        remove_member(self._path, name, self.format, level if level is not None else self.compression_level)

    def payload(self) -> bytes:
        """Return decompressed payload bytes for single-stream or single-entry archives."""
        src = self._source()
        raw = src.read_bytes() if isinstance(src, Path) else src
        return decompress_bytes(raw, self.format)

    def info(self, *, compute_sha256: bool = False) -> ArchiveInfo:
        """Return :class:`~arkit.models.ArchiveInfo` summary."""
        return info(self._source(), self.format, compute_sha256=compute_sha256)

    def validate(self) -> bool:
        """Return ``True`` if this archive passes full streaming validation."""
        return bool(validate(self._source(), fmt=self.format))

    def convert(self, fmt: Format | str, level: int | None = None) -> bytes:
        """Convert this archive to ``fmt`` in memory and return ``bytes``."""
        src = self._source()
        raw = src.read_bytes() if isinstance(src, Path) else src
        return convert_bytes(
            raw,
            fmt,
            level if level is not None else self.compression_level,
            self.format,
            threads=self.threads,
        )

    def convert_to(self, dst: StrPath, fmt: Format | str | None = None, level: int | None = None) -> Path:
        """Stream-convert this archive into ``dst`` atomically."""
        target = Path(dst)
        eff_level = level if level is not None else self.compression_level
        if self._path is not None:
            return convert_file(self._path, target, fmt, eff_level, threads=self.threads)
        resolved_target_fmt = fmt if fmt is not None else _resolve_fmt(target, None, for_output=True)
        blob = self.convert(resolved_target_fmt, eff_level)
        from .atomicio import atomic_write

        atomic_write(target, blob)
        return target

    def split(self, part_size: int, out_dir: StrPath | None = None) -> list[Path] | list[bytes]:
        """Split this archive into parts of ``part_size`` bytes."""
        if self._path is not None:
            return split_file(self._path, part_size, out_dir)
        return split(self._blob or b"", part_size)

    @classmethod
    def pack(
        cls,
        path: StrPath,
        source: StrPath | MembersInput,
        fmt: Format | str | None = None,
        *,
        level: int | None = None,
        threads: int | None = None,
        deterministic: bool = False,
    ) -> Archive:
        """Create a new archive at ``path`` from ``source`` and return a bound :class:`Archive`."""
        created = pack_file(
            path,
            source,
            fmt=fmt,
            level=level,
            threads=threads,
            deterministic=deterministic,
        )
        return cls(created, fmt)

    create = pack

    def iter_members(self) -> Iterator[Member]:
        """Lazily yield :class:`~arkit.models.Member` items from this archive."""
        yield from iter_members(self._source(), self.format)

    def diff(self, other: StrPath | bytes | Archive, other_fmt: Format | str | None = None) -> DiffReport:
        """Compare this archive against ``other`` and return a :class:`~arkit.models.DiffReport`."""
        if isinstance(other, Archive):
            return diff_archives(self._source(), other._source(), fmt_a=self.format, fmt_b=other.format)
        return diff_archives(self._source(), other, fmt_a=self.format, fmt_b=other_fmt)

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return any(m.name == name for m in self.members())

    def __iter__(self) -> Iterator[Member]:
        yield from iter_members(self._source(), self.format)

    def __len__(self) -> int:
        return len(self.members())

    def __enter__(self) -> Archive:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def __repr__(self) -> str:
        if self._fmt is None:
            return f"Archive(compression_level={self.compression_level}, threads={self.threads})"
        origin = str(self._path) if self._path is not None else f"<bytes:{len(self._blob or b'')}>"
        return f"Archive(format={self._fmt.ext!r}, source={origin!r})"


@contextmanager
def open_archive(source: StrPath | bytes, fmt: Format | str | None = None) -> Iterator[Archive]:
    """Context manager opening ``source`` as an :class:`Archive` handle."""
    yield Archive(source, fmt)
