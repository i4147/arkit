"""Security guardrails for archive path sanitization and decompression-bomb defense."""

from __future__ import annotations

import os
import posixpath
import tarfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from .exceptions import ArchiveBombError, UnsafeArchivePathError
from .models import SafetyPolicy, StrPath

__all__ = [
    "DEFAULT_SAFETY_POLICY",
    "ExtractionGuard",
    "ensure_parent",
    "safe_destination",
    "sanitize_member_name",
]

DEFAULT_SAFETY_POLICY = SafetyPolicy()


def sanitize_member_name(
    member_name: str,
    *,
    strip_components: int = 0,
    expected_top_dir: str | None = None,
) -> str | None:
    """Validate and normalize an archive member path into a safe relative POSIX path.

    Raises :class:`~arkit.exceptions.UnsafeArchivePathError` if the path contains
    NUL bytes, absolute prefixes, Windows drive letters, UNC prefixes, or ``..``
    parent-directory traversal segments.

    Returns ``None`` if ``strip_components`` strips away all path segments (e.g.
    the top-level directory entry itself when ``strip_components=1``).
    """
    if "\x00" in member_name:
        raise UnsafeArchivePathError(f"Archive member contains NUL byte: {member_name!r}")

    normalized_sep = member_name.replace("\\", "/")
    if normalized_sep.startswith("/") or (
        len(normalized_sep) >= 2 and normalized_sep[1] == ":" and normalized_sep[0].isalpha()
    ):
        raise UnsafeArchivePathError(f"Absolute or drive-prefixed member path is unsafe: {member_name!r}")

    raw_parts = [p for p in normalized_sep.split("/") if p not in ("", ".")]
    if any(part == ".." for part in raw_parts):
        raise UnsafeArchivePathError(f"Archive member escapes extraction root: {member_name!r}")

    if not raw_parts:
        return None

    if expected_top_dir is not None and raw_parts[0] != expected_top_dir:
        raise UnsafeArchivePathError(
            f"Archive member is outside expected top directory {expected_top_dir!r}: {member_name!r}"
        )

    if strip_components < 0:
        raise ValueError("strip_components must be >= 0")
    if strip_components > 0:
        if len(raw_parts) <= strip_components:
            return None
        raw_parts = raw_parts[strip_components:]

    return PurePosixPath(*raw_parts).as_posix()


def safe_destination(
    root: StrPath,
    member_name: str,
    *,
    strip_components: int = 0,
    expected_top_dir: str | None = None,
) -> Path | None:
    """Resolve ``member_name`` safely beneath ``root``.

    Returns the resolved :class:`~pathlib.Path` inside ``root``, or ``None`` when
    the entry is empty after ``strip_components`` stripping. Raises
    :class:`~arkit.exceptions.UnsafeArchivePathError` if the target escapes ``root``
    either lexically or via pre-existing symlinks on disk.
    """
    clean = sanitize_member_name(
        member_name,
        strip_components=strip_components,
        expected_top_dir=expected_top_dir,
    )
    if clean is None:
        return None

    resolved_root = Path(root).resolve()
    destination = (resolved_root / clean).resolve()
    try:
        destination.relative_to(resolved_root)
    except ValueError as exc:
        raise UnsafeArchivePathError(f"Archive member escapes extraction root: {member_name!r}") from exc
    return destination


def ensure_parent(path: Path, *, root: Path | None = None) -> None:
    """Create parent directories for ``path``, rejecting symlink escapes if ``root`` is given."""
    if root is not None:
        resolved_root = root.resolve()
        resolved_parent = path.parent.resolve()
        try:
            resolved_parent.relative_to(resolved_root)
        except ValueError as exc:
            raise UnsafeArchivePathError(
                f"Parent directory escapes extraction root via symlink: {path}"
            ) from exc
    path.parent.mkdir(parents=True, exist_ok=True)


class ExtractionGuard:
    """Stateful streaming monitor enforcing :class:`~arkit.models.SafetyPolicy` during extraction."""

    __slots__ = (
        "_compressed_size",
        "_current_file_bytes",
        "_current_member",
        "_file_count",
        "_policy",
        "_root",
        "_strip_components",
        "_total_bytes",
    )

    def __init__(
        self,
        root: StrPath,
        policy: SafetyPolicy = DEFAULT_SAFETY_POLICY,
        *,
        compressed_size: int | None = None,
        strip_components: int = 0,
    ) -> None:
        self._root = Path(root).resolve()
        self._policy = policy
        self._compressed_size = compressed_size if (compressed_size and compressed_size > 0) else None
        self._strip_components = strip_components
        self._file_count = 0
        self._total_bytes = 0
        self._current_file_bytes = 0
        self._current_member = ""

    @property
    def root(self) -> Path:
        """Resolved extraction root directory."""
        return self._root

    @property
    def total_bytes(self) -> int:
        """Total uncompressed bytes written so far."""
        return self._total_bytes

    @property
    def file_count(self) -> int:
        """Total extracted entries counted so far."""
        return self._file_count

    def resolve_member(self, member_name: str, *, declared_size: int | None = None) -> Path | None:
        """Validate member path, count limits, and declared size before extracting."""
        target = safe_destination(
            self._root,
            member_name,
            strip_components=self._strip_components,
            expected_top_dir=self._policy.expected_top_dir,
        )
        if target is None:
            return None

        if target.name in self._policy.extra_blocked_names:
            raise UnsafeArchivePathError(f"Blocked archive member name: {member_name!r}")

        self._file_count += 1
        if self._policy.max_files is not None and self._file_count > self._policy.max_files:
            raise ArchiveBombError(
                f"Archive exceeds maximum allowed member count ({self._policy.max_files})"
            )

        self._current_file_bytes = 0
        self._current_member = member_name
        if declared_size is not None and declared_size > 0:
            if self._policy.max_file_size is not None and declared_size > self._policy.max_file_size:
                raise ArchiveBombError(
                    f"Member {member_name!r} declared size ({declared_size} B) exceeds "
                    f"max_file_size ({self._policy.max_file_size} B)"
                )
            if (
                self._policy.max_total_size is not None
                and (self._total_bytes + declared_size) > self._policy.max_total_size
            ):
                raise ArchiveBombError(
                    f"Archive declared uncompressed total exceeds max_total_size ({self._policy.max_total_size} B)"
                )
        return target

    def record_chunk(self, byte_count: int) -> None:
        """Account for ``byte_count`` newly decompressed bytes and enforce bomb limits mid-stream."""
        self._current_file_bytes += byte_count
        self._total_bytes += byte_count

        if self._policy.max_file_size is not None and self._current_file_bytes > self._policy.max_file_size:
            raise ArchiveBombError(
                f"Member {self._current_member!r} exceeded max_file_size ({self._policy.max_file_size} B)"
            )
        if self._policy.max_total_size is not None and self._total_bytes > self._policy.max_total_size:
            raise ArchiveBombError(
                f"Extraction exceeded max_total_size ({self._policy.max_total_size} B)"
            )
        if (
            self._policy.max_ratio is not None
            and self._compressed_size is not None
            and self._total_bytes >= self._policy.ratio_min_bytes
        ):
            ratio = self._total_bytes / self._compressed_size
            if ratio > self._policy.max_ratio:
                raise ArchiveBombError(
                    f"Suspicious decompression ratio {ratio:.1f}x exceeds max_ratio ({self._policy.max_ratio}x)"
                )

    def validate_link(self, target: Path, link_target: str, *, is_hardlink: bool = False) -> None:
        """Verify symbolic or hard link permission and ensure its target stays within ``root``."""
        if not self._policy.allow_symlinks:
            raise UnsafeArchivePathError(f"Archive links are disabled by policy: {self._current_member!r}")

        if "\x00" in link_target:
            raise UnsafeArchivePathError(f"Link target contains NUL byte: {link_target!r}")

        if is_hardlink:
            resolved = safe_destination(self._root, link_target, strip_components=self._strip_components)
            if resolved is None:
                raise UnsafeArchivePathError(f"Unsafe hardlink target: {link_target!r}")
            return

        if os.path.isabs(link_target) or link_target.startswith(("/", "\\")):
            raise UnsafeArchivePathError(f"Absolute symlink target is unsafe: {link_target!r}")

        rel_dir = target.parent.relative_to(self._root).as_posix()
        joined = posixpath.normpath(posixpath.join(rel_dir, link_target.replace("\\", "/")))
        if joined == ".." or joined.startswith("../") or joined.startswith("/"):
            raise UnsafeArchivePathError(
                f"Symlink target {link_target!r} escapes extraction root from {self._current_member!r}"
            )

    def make_tar_filter(self) -> Callable[[tarfile.TarInfo, str], tarfile.TarInfo | None]:
        """Build a ``tarfile.extractall(filter=...)`` compatible filter bound to this guard."""

        def _filter(member: tarfile.TarInfo, destination: str) -> tarfile.TarInfo | None:
            _ = destination
            target = self.resolve_member(member.name, declared_size=member.size if member.isfile() else 0)
            if target is None:
                return None

            if member.issym() or member.islnk():
                self.validate_link(target, member.linkname, is_hardlink=member.islnk())
            elif not (member.isfile() or member.isdir()):
                if not self._policy.allow_special:
                    raise UnsafeArchivePathError(f"Unsupported special archive entry: {member.name!r}")

            cloned = tarfile.TarInfo(name=target.relative_to(self._root).as_posix())
            cloned.size = member.size
            cloned.mtime = member.mtime
            cloned.mode = (member.mode & 0o777) if member.mode else (0o755 if member.isdir() else 0o644)
            cloned.type = member.type
            cloned.linkname = member.linkname
            if member.isfile():
                self.record_chunk(member.size)
            return cloned

        return _filter
