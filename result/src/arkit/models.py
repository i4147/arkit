"""Core data models, statistics reports, and security policy configurations."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from .formats import Format

__all__ = [
    "ArchiveInfo",
    "CompressionStats",
    "DiffReport",
    "FilterFunc",
    "HealthReport",
    "Member",
    "MemberInfo",
    "MembersInput",
    "PathLike",
    "ProgressCallback",
    "SafetyPolicy",
    "StrPath",
]

StrPath: TypeAlias = str | os.PathLike[str]
PathLike: TypeAlias = StrPath
FilterFunc: TypeAlias = Callable[[str], bool]
ProgressCallback: TypeAlias = Callable[[int, int | None], None]


@dataclass(frozen=True, slots=True)
class MemberInfo:
    """Metadata describing a single entry inside an archive or compressed stream.

    Attributes:
        name: Normalized POSIX path of the member inside the archive.
        size: Uncompressed payload size in bytes.
        compressed_size: Compressed size in bytes if reported by container, else ``None``.
        mtime: Modification timestamp (UTC :class:`~datetime.datetime`) if available.
        mode: POSIX permission mode bits if available.
        is_dir: Whether this entry represents a directory.
        is_symlink: Whether this entry represents a symbolic link.
        crc: Optional CRC32/checksum integer if stored in container headers.
    """

    name: str
    size: int
    compressed_size: int | None = None
    mtime: datetime | None = None
    mode: int | None = None
    is_dir: bool = False
    is_symlink: bool = False
    crc: int | None = None


@dataclass(frozen=True, slots=True, init=False)
class Member:
    """In-memory representation of an archive member pairing metadata with bytes."""

    info: MemberInfo
    data: bytes

    def __init__(self, info: MemberInfo | str, data: bytes) -> None:
        resolved_info = MemberInfo(name=info, size=len(data)) if isinstance(info, str) else info
        object.__setattr__(self, "info", resolved_info)
        object.__setattr__(self, "data", data)

    @property
    def name(self) -> str:
        """Shortcut for ``self.info.name``."""
        return self.info.name

    @property
    def size(self) -> int:
        """Shortcut for ``self.info.size``."""
        return self.info.size


MembersInput: TypeAlias = StrPath | Mapping[str, bytes] | Iterable[Member]


@dataclass(frozen=True, slots=True)
class ArchiveInfo:
    """Summary metadata and member listing for an archive or compressed stream.

    Attributes:
        path: Backing filesystem path, or ``None`` for in-memory byte buffers.
        format: Resolved :class:`~arkit.formats.Format` of the archive.
        size: Compressed archive size in bytes.
        payload_size: Total uncompressed payload size across all members in bytes.
        member_count: Number of regular file members.
        ratio: Expansion factor (``payload_size / size``) when ``size > 0``, else ``0.0``.
        members: Tuple of :class:`MemberInfo` records.
        sha256: Optional hex SHA-256 digest of the archive file if requested.
    """

    path: Path | None
    format: Format
    size: int
    payload_size: int
    member_count: int
    ratio: float
    members: tuple[MemberInfo, ...] = ()
    sha256: str | None = None

    @property
    def space_saving_percent(self) -> float:
        """Percentage of space saved relative to uncompressed payload size."""
        if self.payload_size <= 0:
            return 0.0
        return (1.0 - (self.size / self.payload_size)) * 100.0


@dataclass(frozen=True, slots=True)
class CompressionStats:
    """Compression metrics returned by :func:`~arkit.compress` and :func:`~arkit.convert`.

    Attributes:
        source_bytes: Total uncompressed input size in bytes (summed recursively for directories).
        result_bytes: Resulting compressed file size in bytes.
        ratio: ``result_bytes / source_bytes`` (smaller means stronger compression).
        space_freed: ``source_bytes - result_bytes`` in bytes.
        space_change_percent: Percentage size reduction ``(1.0 - ratio) * 100``.
        elapsed_seconds: Wall-clock duration of the operation in seconds.
        sha256: Optional SHA-256 hex digest of the output archive if requested.
    """

    source_bytes: int
    result_bytes: int
    ratio: float
    space_freed: int
    space_change_percent: float
    elapsed_seconds: float = 0.0
    sha256: str | None = None

    @property
    def compression_factor(self) -> float:
        """Uncompressed-to-compressed factor (``source_bytes / result_bytes``)."""
        if self.result_bytes <= 0:
            return 0.0
        return self.source_bytes / self.result_bytes


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Integrity validation result returned by :func:`~arkit.validate`.

    Implements :meth:`__bool__` returning ``self.healthy`` so instances can be
    used both as structured reports (``report.healthy``, ``report.errors``) and
    directly in boolean expressions (``if validate(path): ...``).
    """

    healthy: bool
    format: str
    errors: tuple[str, ...] = ()
    checked_members: int = 0
    checked_bytes: int = 0

    def __bool__(self) -> bool:
        return self.healthy


@dataclass(frozen=True, slots=True)
class DiffReport:
    """Comparison result between two archives computed without full extraction to disk."""

    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    modified: tuple[str, ...] = ()
    unchanged: tuple[str, ...] = ()

    @property
    def identical(self) -> bool:
        """Return ``True`` when both archives have identical member paths and contents."""
        return not (self.added or self.removed or self.modified)


@dataclass(frozen=True, slots=True)
class SafetyPolicy:
    """Configurable security guardrails for archive extraction.

    Protects against:
    - Path traversal (Zip Slip / Tar Slip: ``../``, leading ``/``, drive letters, NUL bytes)
    - Symlink and hardlink escape attacks
    - Special device / FIFO / socket creation
    - Decompression bombs (total extracted bytes, single-file size, member count, ratio)

    Attributes:
        allow_symlinks: Permit symbolic/hard links only when their resolved target stays
            strictly inside the extraction root. Defaults to ``False``.
        allow_special: Permit FIFOs, sockets, or device nodes. Defaults to ``False``.
        max_total_size: Maximum total uncompressed bytes across the entire extraction
            (``None`` disables limit; default ``10 GiB``).
        max_file_size: Maximum uncompressed bytes for any single member
            (``None`` disables limit; default ``4 GiB``).
        max_files: Maximum number of extracted entries (``None`` disables limit; default ``500_000``).
        max_ratio: Maximum allowed expansion ratio ``uncompressed / compressed`` once
            uncompressed output exceeds ``ratio_min_bytes`` (``None`` disables check; default ``1000.0``).
        ratio_min_bytes: Minimum extracted bytes before ``max_ratio`` is enforced (default ``10 MiB``),
            avoiding false positives on tiny highly repetitive files.
        expected_top_dir: If set, require every archive member to reside under this top-level directory.
    """

    allow_symlinks: bool = False
    allow_special: bool = False
    max_total_size: int | None = 10 * (1024**3)
    max_file_size: int | None = 4 * (1024**3)
    max_files: int | None = 500_000
    max_ratio: float | None = 1000.0
    ratio_min_bytes: int = 10 * (1024**2)
    expected_top_dir: str | None = None
    extra_blocked_names: frozenset[str] = field(default_factory=frozenset)
