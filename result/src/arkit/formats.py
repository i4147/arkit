"""Archive and compression format registry, enums, extension matching, and magic sniffing.

Supports O(1)-memory shallow and deep format detection (including tar streams
wrapped inside gzip, bzip2, xz, zstandard, brotli, lz4, snappy, or 7z without
loading multi-gigabyte archives into RAM).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType

from .codecs import codec_available, make_incremental_decoder
from .exceptions import UnknownFormatError
from .models import StrPath

__all__ = [
    "ArchiveFormat",
    "ArchiveType",
    "Compression",
    "Container",
    "FORMATS",
    "Format",
    "SUPPORTED_EXTENSIONS",
    "detect",
    "detect_archive_type",
    "detect_file",
    "detect_format",
    "find_archive_format",
    "get_compression_type",
    "guess_format",
    "preferred_suffix",
    "register_format",
]

_SNIFF_BYTES: int = 64 * 1024
_SNIFF_STEP: int = 16 * 1024
_MAGIC_7Z: bytes = b"7z\xbc\xaf\x27\x1c"
_MAGIC_PBRT: bytes = b"PBRT0001"


class ArchiveType:
    """Legacy archive type constants preserved for compatibility."""

    GZ = "gz"
    BZ2 = "bz2"
    XZ = "xz"
    ZIP = "zip"
    ZST = "zst"
    BR = "br"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    SEVENZ = "7z"
    TAR = "tar"


class Compression(Enum):
    """Supported stream compression algorithms."""

    BR = "br"
    GZ = "gz"
    XZ = "xz"
    BZ2 = "bz2"
    LZ4 = "lz4"
    ZST = "zst"
    SNAPPY = "snappy"
    PBRT = "pbrt"
    SEVENZ = "7z"

    @property
    def magic(self) -> bytes:
        """Leading magic signature bytes for this compression stream (empty if none)."""
        return _COMPRESSION_MAGIC.get(self, b"")


_COMPRESSION_MAGIC: Mapping[Compression, bytes] = {
    Compression.GZ: b"\x1f\x8b",
    Compression.XZ: b"\xfd7zXZ\x00",
    Compression.BZ2: b"BZh",
    Compression.LZ4: b"\x04\x22\x4d\x18",
    Compression.ZST: b"\x28\xb5\x2f\xfd",
    Compression.SNAPPY: b"\xff\x06\x00\x00sNaPpY",
    Compression.PBRT: _MAGIC_PBRT,
    Compression.SEVENZ: _MAGIC_7Z,
    Compression.BR: b"",
}


class Container(Enum):
    """Supported multi-member archive container formats."""

    TAR = "tar"
    ZIP = "zip"
    SEVENZ = "7z"

    @property
    def magic(self) -> bytes:
        """Leading magic signature bytes for this container."""
        if self is Container.ZIP:
            return b"PK\x03\x04"
        if self is Container.SEVENZ:
            return _MAGIC_7Z
        return b""


class Format(Enum):
    """Canonical archive and single-stream formats supported by ``arkit``."""

    BR = (None, Compression.BR, "br", (".br",))
    GZ = (None, Compression.GZ, "gz", (".gz",))
    XZ = (None, Compression.XZ, "xz", (".xz",))
    BZ2 = (None, Compression.BZ2, "bz2", (".bz2",))
    LZ4 = (None, Compression.LZ4, "lz4", (".lz4",))
    ZST = (None, Compression.ZST, "zst", (".zst",))
    SNAPPY = (None, Compression.SNAPPY, "snappy", (".snappy", ".sz"))
    PBRT = (None, Compression.PBRT, "pbrt", (".pbrt",))

    ZIP = (Container.ZIP, None, "zip", (".zip",))
    WHL = (Container.ZIP, None, "whl", (".whl",))
    SEVENZ = (Container.SEVENZ, None, "7z", (".7z",))

    TAR = (Container.TAR, None, "tar", (".tar",))
    TAR_BR = (Container.TAR, Compression.BR, "tar.br", (".tar.br", ".tbr"))
    TAR_GZ = (Container.TAR, Compression.GZ, "tar.gz", (".tar.gz", ".tgz", ".taz"))
    TAR_XZ = (Container.TAR, Compression.XZ, "tar.xz", (".tar.xz", ".txz"))
    TAR_BZ2 = (Container.TAR, Compression.BZ2, "tar.bz2", (".tar.bz2", ".tbz2", ".tbz"))
    TAR_LZ4 = (Container.TAR, Compression.LZ4, "tar.lz4", (".tar.lz4", ".tlz4"))
    TAR_ZST = (Container.TAR, Compression.ZST, "tar.zst", (".tar.zst", ".tzst"))
    TAR_SNAPPY = (Container.TAR, Compression.SNAPPY, "tar.snappy", (".tar.snappy", ".tar.sz"))
    TAR_SEVENZ = (Container.TAR, Compression.SEVENZ, "tar.7z", (".tar.7z",))

    def __init__(
        self,
        container: Container | None,
        compression: Compression | None,
        format_id: str,
        suffixes: tuple[str, ...],
    ) -> None:
        self.container: Container | None = container
        self.compression: Compression | None = compression
        self.format_id: str = format_id
        self.suffixes: tuple[str, ...] = suffixes

    def __str__(self) -> str:
        return self.ext

    @property
    def ext(self) -> str:
        """Primary canonical filename extension (including leading dot)."""
        return self.suffixes[0]

    @property
    def kind(self) -> str:
        """High-level structural category: ``'tar'``, ``'zip'``, ``'7z'``, or ``'stream'``."""
        if self.container is Container.TAR:
            return "tar"
        if self.container is Container.ZIP:
            return "zip"
        if self.container is Container.SEVENZ:
            return "7z"
        return "stream"

    @property
    def magic(self) -> bytes:
        """Leading magic bytes of the outer layer."""
        if self.compression is not None:
            return self.compression.magic
        if self.container is not None:
            return self.container.magic
        return b""

    @property
    def is_archive(self) -> bool:
        """Return ``True`` if this format is a multi-member container (tar/zip/7z)."""
        return self.container is not None

    @classmethod
    def parse(cls, value: Format | str) -> Format:
        """Resolve a :class:`Format` from an enum instance, format name, alias, or suffix."""
        if not isinstance(value, str):
            return value
        raw = value.strip().lower()
        if not raw:
            raise UnknownFormatError("Empty format specifier")

        upper = raw.upper().replace(".", "_").replace("+", "_").replace("-", "_")
        if upper in cls.__members__:
            return cls[upper]

        no_dot = raw.lstrip(".")
        if no_dot in _FORMAT_ALIASES:
            return _FORMAT_ALIASES[no_dot]

        with_dot = "." + no_dot
        for member in cls:
            if member.format_id == no_dot or with_dot in member.suffixes:
                return member

        raise UnknownFormatError(f"Unsupported archive format: {value!r}")


_FORMAT_ALIASES: dict[str, Format] = {
    "gzip": Format.GZ,
    "bzip2": Format.BZ2,
    "lzma": Format.XZ,
    "zstd": Format.ZST,
    "zstandard": Format.ZST,
    "brotli": Format.BR,
    "7zip": Format.SEVENZ,
    "sevenz": Format.SEVENZ,
    "wheel": Format.WHL,
    "tgz": Format.TAR_GZ,
    "taz": Format.TAR_GZ,
    "txz": Format.TAR_XZ,
    "tbz": Format.TAR_BZ2,
    "tbz2": Format.TAR_BZ2,
    "tzst": Format.TAR_ZST,
    "tbr": Format.TAR_BR,
    "tlz4": Format.TAR_LZ4,
    "tar+gzip": Format.TAR_GZ,
    "tar+bzip2": Format.TAR_BZ2,
    "tar+xz": Format.TAR_XZ,
    "tar+zstd": Format.TAR_ZST,
    "tar+brotli": Format.TAR_BR,
    "tar+lz4": Format.TAR_LZ4,
    "tar+snappy": Format.TAR_SNAPPY,
    "tar+7z": Format.TAR_SEVENZ,
}


# ---------------------------------------------------------------------------
# Pluggable ArchiveFormat Registry (compatible with both api.formats & walkfs)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ArchiveFormat:
    """Descriptor for a registered archive or stream format.

    Attributes:
        name: Canonical identifier (e.g. ``'tar.zst'``, ``'zip'``, ``'gz'``).
        kind: Structural reader category: ``'zip'``, ``'tar'``, ``'7z'``, or ``'stream'`` / ``'single'``.
        codec: Optional compression codec identifier (e.g. ``'zst'``, ``'gz'``), or ``None``.
        suffixes: Tuple of matching lowercase file suffixes (with leading dot).
        format_enum: Associated :class:`Format` enum member if built-in, else ``None``.
    """

    name: str
    kind: str
    codec: str | None = None
    suffixes: tuple[str, ...] = ()
    format_enum: Format | None = None


_REGISTERED_FORMATS: dict[str, ArchiveFormat] = {}
_SUFFIX_MAP: dict[str, ArchiveFormat] = {}
_SORTED_SUFFIXES: list[str] = []


def _rebuild_registry() -> None:
    _SUFFIX_MAP.clear()
    for af in _REGISTERED_FORMATS.values():
        for suf in af.suffixes:
            _SUFFIX_MAP[suf] = af
    _SORTED_SUFFIXES.clear()
    _SORTED_SUFFIXES.extend(sorted(_SUFFIX_MAP, key=len, reverse=True))


def register_format(
    name: str,
    *,
    suffixes: tuple[str, ...],
    kind: str,
    codec: str | None = None,
    format_enum: Format | None = None,
) -> ArchiveFormat:
    """Register or replace an archive/stream format in the live registry.

    Accepts ``kind`` in ``{'zip', 'tar', '7z', 'stream', 'single'}`` (``'single'``
    is normalized to ``'stream'`` for compatibility with ``walkfs``).
    """
    normalized_kind = "stream" if kind == "single" else kind
    if normalized_kind not in {"zip", "tar", "7z", "stream"}:
        raise ValueError(f"kind must be one of zip/tar/7z/stream/single, got {kind!r}")
    norm_suffixes = tuple((s.lower() if s.startswith(".") else f".{s.lower()}") for s in suffixes)
    if not norm_suffixes:
        raise ValueError("At least one suffix is required")
    entry = ArchiveFormat(
        name=name,
        kind=normalized_kind,
        codec=codec,
        suffixes=norm_suffixes,
        format_enum=format_enum,
    )
    _REGISTERED_FORMATS[name] = entry
    _rebuild_registry()
    return entry


def _init_default_formats() -> None:
    for fmt in Format:
        codec = fmt.compression.value if fmt.compression is not None else None
        register_format(
            fmt.format_id,
            suffixes=fmt.suffixes,
            kind=fmt.kind,
            codec=codec,
            format_enum=fmt,
        )


_init_default_formats()

#: Live read-only mapping of registered format names to :class:`ArchiveFormat`.
FORMATS: Mapping[str, ArchiveFormat] = MappingProxyType(_REGISTERED_FORMATS)

#: Set of all currently registered filename suffixes.
SUPPORTED_EXTENSIONS: set[str] = set(_SUFFIX_MAP.keys())


def _is_tar_header(data: bytes) -> bool:
    """Check POSIX ustar/PAX/GNU tar magic signature at byte offset 257."""
    return len(data) >= 262 and data[257:262] == b"ustar"


def _detect_shallow_bytes(data: bytes) -> Format | None:
    """Detect outer format from leading magic bytes without decompression."""
    if _is_tar_header(data):
        return Format.TAR
    if data.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return Format.ZIP
    if data.startswith(_MAGIC_7Z):
        return Format.SEVENZ
    if data.startswith(_MAGIC_PBRT):
        return Format.PBRT
    for comp in (
        Compression.GZ,
        Compression.XZ,
        Compression.BZ2,
        Compression.LZ4,
        Compression.ZST,
        Compression.SNAPPY,
    ):
        magic = comp.magic
        if magic and data.startswith(magic):
            return Format[comp.name]
    return None


def magic_ok(fmt: Format, head: bytes) -> bool:
    """Verify whether leading bytes ``head`` match ``fmt``'s expected signature."""
    if fmt.container is Container.TAR and fmt.compression is None:
        return _is_tar_header(head)
    if fmt.container is Container.ZIP:
        return head.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"))
    magic = fmt.magic
    if not magic:
        return True
    return head.startswith(magic)


def detect(data: bytes, deep: bool = False) -> Format | None:
    """Detect :class:`Format` from raw bytes ``data``.

    When ``deep=True``, inspects the first 512 decompressed bytes of compressed
    streams (or 7z headers) to distinguish compound ``.tar.*`` archives from
    standalone compressed files.
    """
    shallow = _detect_shallow_bytes(data)
    if shallow is None or not deep:
        return shallow

    if shallow is Format.SEVENZ:
        if not codec_available("7z"):
            return shallow
        try:
            import io

            import py7zr
            import py7zr.io

            with py7zr.SevenZipFile(io.BytesIO(data), "r") as sz:
                files = [f for f in sz.list() if not f.is_directory]
                if len(files) == 1:
                    if files[0].filename.lower().endswith(".tar"):
                        return Format.TAR_SEVENZ
                    if (files[0].uncompressed or 0) <= (16 * 1024 * 1024):
                        factory = py7zr.io.BytesIOFactory(limit=16 * 1024 * 1024)
                        sz.extract(targets=[files[0].filename], factory=factory)
                        bio = factory.products.get(files[0].filename)
                        if bio is not None and _is_tar_header(bio.read(512)):
                            return Format.TAR_SEVENZ
        except Exception:
            return shallow
        return shallow

    comp = shallow.compression
    if shallow.container is not None or comp is None or comp is Compression.PBRT:
        return shallow
    if not codec_available(comp.value):
        return shallow

    try:
        dec = make_incremental_decoder(comp.value)
        probe = bytearray()
        for offset in range(0, len(data), _SNIFF_STEP):
            probe.extend(dec.feed(data[offset : offset + _SNIFF_STEP]))
            if len(probe) >= 512:
                break
        if len(probe) < 512:
            probe.extend(dec.finish())
    except Exception:
        return shallow

    if _is_tar_header(bytes(probe)):
        tar_key = f"TAR_{comp.name}"
        if tar_key in Format.__members__:
            return Format[tar_key]
    return shallow


def detect_file(path: StrPath, deep: bool = False) -> Format | None:
    """Detect :class:`Format` of ``path`` in ``O(1)`` memory without loading the whole file."""
    file_path = Path(path)
    with file_path.open("rb") as fh:
        head = fh.read(_SNIFF_BYTES)
        shallow = _detect_shallow_bytes(head)
        if shallow is None or not deep:
            return shallow

        if shallow is Format.SEVENZ:
            if not codec_available("7z"):
                return shallow
            try:
                import py7zr
                import py7zr.io

                with py7zr.SevenZipFile(file_path, "r") as sz:
                    files = [f for f in sz.list() if not f.is_directory]
                    if len(files) == 1:
                        if files[0].filename.lower().endswith(".tar"):
                            return Format.TAR_SEVENZ
                        if (files[0].uncompressed or 0) <= (16 * 1024 * 1024):
                            factory = py7zr.io.BytesIOFactory(limit=16 * 1024 * 1024)
                            sz.extract(targets=[files[0].filename], factory=factory)
                            bio = factory.products.get(files[0].filename)
                            if bio is not None and _is_tar_header(bio.read(512)):
                                return Format.TAR_SEVENZ
            except Exception:
                return shallow
            return shallow

        comp = shallow.compression
        if shallow.container is not None or comp is None or comp is Compression.PBRT:
            return shallow
        if not codec_available(comp.value):
            return shallow

        try:
            dec = make_incremental_decoder(comp.value)
            probe = bytearray(dec.feed(head))
            # Read additional small chunks only until 512 uncompressed bytes are available
            max_reads = 64
            reads = 0
            while len(probe) < 512 and reads < max_reads:
                chunk = fh.read(_SNIFF_STEP)
                if not chunk:
                    probe.extend(dec.finish())
                    break
                probe.extend(dec.feed(chunk))
                reads += 1
        except Exception:
            return shallow

        if _is_tar_header(bytes(probe)):
            tar_key = f"TAR_{comp.name}"
            if tar_key in Format.__members__:
                return Format[tar_key]
        return shallow


def find_archive_format(path: StrPath) -> ArchiveFormat | None:
    """Match ``path`` against registered filename suffixes (longest suffix first).

    Returns ``None`` if no suffix matches (never raises).
    """
    raw_name = os.fspath(path) if isinstance(path, os.PathLike) else str(path)
    lowered = Path(raw_name).name.lower()
    for suffix in _SORTED_SUFFIXES:
        if lowered.endswith(suffix):
            return _SUFFIX_MAP[suffix]
    return None


def guess_format(path: StrPath) -> Format | None:
    """Guess canonical :class:`Format` from ``path``'s filename suffix, or ``None``."""
    matched = find_archive_format(path)
    if matched is not None and matched.format_enum is not None:
        return matched.format_enum
    return None


def detect_format(path: StrPath, *, allow_magic: bool = True) -> ArchiveFormat:
    """Resolve the :class:`ArchiveFormat` of ``path`` by suffix, falling back to magic bytes.

    Raises :class:`~arkit.exceptions.UnknownFormatError` (a :class:`ValueError`
    subclass) if neither extension nor magic signature identifies a supported format.
    """
    matched = find_archive_format(path)
    if matched is not None:
        return matched

    if allow_magic:
        candidate = Path(path)
        if candidate.is_file():
            detected = detect_file(candidate, deep=True)
            if detected is not None:
                return _REGISTERED_FORMATS[detected.format_id]

    raise UnknownFormatError(f"Unsupported archive format: {path}")


def preferred_suffix(format_name: Format | str) -> str:
    """Return the primary filename suffix (e.g. ``'.tar.zst'``) for ``format_name``."""
    if isinstance(format_name, Format):
        return format_name.ext
    key = format_name.strip().lower().lstrip(".")
    if key in _REGISTERED_FORMATS:
        return _REGISTERED_FORMATS[key].suffixes[0]
    fmt = Format.parse(format_name)
    return fmt.ext


def detect_archive_type(filename: StrPath) -> str | None:
    """Compatibility helper returning archive/codec short string from extension or magic."""
    af = find_archive_format(filename)
    if af is not None:
        return af.codec or af.kind
    p = Path(filename)
    if p.is_file():
        f = detect_file(p, deep=False)
        if f is not None:
            return f.compression.value if f.compression is not None else f.kind
    return None


def get_compression_type(path: StrPath) -> str | None:
    """Compatibility helper returning compression/format id for ``path`` (used by batch extractor)."""
    af = find_archive_format(path)
    if af is None:
        return None
    return af.name
