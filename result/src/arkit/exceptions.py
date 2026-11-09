"""Exception hierarchy for ``arkit``.

All custom exceptions inherit from :class:`ArchiveError` so callers can catch
a single base class when desired, while also inheriting from standard Python
exceptions (:class:`ValueError`, :class:`ImportError`, :class:`RuntimeError`)
where appropriate for drop-in compatibility.
"""

from __future__ import annotations

__all__ = [
    "ArchiveBombError",
    "ArchiveError",
    "ArchiveInPlaceError",
    "CorruptedArchiveError",
    "CorruptedContent",
    "MissingCodecError",
    "PasswordProtectedError",
    "UnknownFormatError",
    "UnsafeArchivePathError",
    "UnsupportedFormatError",
]


class ArchiveError(Exception):
    """Base exception raised for archive and compression errors in ``arkit``."""


class UnknownFormatError(ArchiveError, ValueError):
    """Raised when an archive or compression format cannot be determined or is unknown."""


class UnsupportedFormatError(UnknownFormatError):
    """Raised when an operation is not supported for a given archive or codec format."""


class ArchiveInPlaceError(ArchiveError):
    """Raised when in-place archive processing or atomic replacement fails."""


class UnsafeArchivePathError(ArchiveError, ValueError):
    """Raised when an archive member violates extraction path safety rules (e.g. Zip/Tar Slip)."""


class ArchiveBombError(ArchiveError, ValueError):
    """Raised when an archive exceeds configured decompression bomb safety thresholds."""


class CorruptedArchiveError(ArchiveError):
    """Raised when an archive or compressed stream fails integrity, CRC, or trailer checks."""


#: Alias preserved for compatibility with legacy ``valarchive.CorruptedContent``.
CorruptedContent = CorruptedArchiveError


class MissingCodecError(ArchiveError, ImportError):
    """Raised when an optional third-party compression backend is required but not installed."""


class PasswordProtectedError(ArchiveError, RuntimeError):
    """Raised when opening an encrypted archive member without a valid password."""
