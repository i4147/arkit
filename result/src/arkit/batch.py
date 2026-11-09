"""Recursive archive discovery and parallel batch extraction utilities.

Modernizes ``archivex.py`` with:
- Safe extraction by default (preventing Zip/Tar Slip & decompression bombs)
- Non-destructive extraction by default (``remove_source=False`` unless explicitly requested)
- Optional recursive nested archive extraction with depth protection
- Multi-worker batch processing across directory trees or explicit archive lists
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .formats import find_archive_format
from .models import SafetyPolicy, StrPath
from .operations import check_integrity, extract

logger = logging.getLogger("arkit.batch")

__all__ = [
    "batch_extract",
    "extract_archive",
    "find_archives",
    "nested_candidates",
    "scan_archives",
]


def scan_archives(directory: StrPath) -> list[Path]:
    """Recursively discover all supported archive/compressed files under ``directory``."""
    base = Path(directory)
    if not base.exists():
        return []
    if base.is_file():
        return [base] if find_archive_format(base) is not None else []

    found: list[Path] = []
    for root_str, dirs, files in os.walk(base, followlinks=False):
        dirs.sort()
        files.sort()
        root_p = Path(root_str)
        for fname in files:
            candidate = root_p / fname
            if candidate.is_file() and not candidate.is_symlink() and find_archive_format(fname) is not None:
                found.append(candidate)
    return found


def find_archives(directory: StrPath = ".") -> list[Path]:
    """Alias for :func:`scan_archives` defaulting to the current working directory."""
    return scan_archives(directory)


def nested_candidates(extract_dir: StrPath, extracted_paths: Iterable[StrPath]) -> list[Path]:
    """Return sorted archive files among ``extracted_paths`` strictly inside ``extract_dir``."""
    base = Path(extract_dir).resolve()
    candidates: set[Path] = set()
    for p_raw in extracted_paths:
        p = Path(p_raw)
        if not p.is_absolute():
            p = Path(extract_dir) / p
        if not p.is_file() or p.is_symlink():
            continue
        if find_archive_format(p) is None:
            continue
        if not p.resolve().is_relative_to(base):
            continue
        candidates.add(p)
    return sorted(candidates)


def extract_archive(
    archive_path: StrPath,
    *,
    output_dir: StrPath | None = None,
    extract_nested: bool = True,
    max_nested_depth: int = 3,
    remove_source: bool = False,
    check_first: bool = True,
    safe: bool = True,
    policy: SafetyPolicy | None = None,
    _depth: int = 0,
) -> bool:
    """Validate and extract ``archive_path`` (recursively expanding nested archives if enabled).

    Unlike legacy ``archivex.extract_archive``, ``remove_source`` defaults to ``False``
    so source archives are never silently deleted unless opted in.
    """
    path = Path(archive_path)
    if not path.is_file():
        return False
    if find_archive_format(path) is None:
        logger.warning("Skipping unsupported archive format: %s", path)
        return False
    if check_first and not check_integrity(path):
        logger.warning("Skipping archive failing integrity check: %s", path)
        return False

    target_dir = Path(output_dir) if output_dir is not None else path.parent
    try:
        written = extract(path, target_dir, safe=safe, policy=policy)
        if remove_source:
            path.unlink(missing_ok=True)
    except Exception as exc:
        logger.error("Extraction failed for %s: %s", path, exc)
        return False

    if extract_nested and _depth < max_nested_depth:
        for nested in nested_candidates(target_dir, written):
            # Nested archives extracted into workspace may be removed after expansion when remove_source=True
            extract_archive(
                nested,
                output_dir=nested.parent,
                extract_nested=True,
                max_nested_depth=max_nested_depth,
                remove_source=remove_source,
                check_first=check_first,
                safe=safe,
                policy=policy,
                _depth=_depth + 1,
            )
    return True


def batch_extract(
    targets: StrPath | Iterable[StrPath],
    *,
    output_dir: StrPath | None = None,
    workers: int = 4,
    extract_nested: bool = True,
    max_nested_depth: int = 3,
    remove_source: bool = False,
    check_first: bool = True,
    safe: bool = True,
    policy: SafetyPolicy | None = None,
) -> dict[Path, bool]:
    """Extract multiple archives (or all archives under a directory) concurrently.

    Returns a mapping ``{archive_path: succeeded_bool}``.
    """
    if isinstance(targets, (str, os.PathLike)):
        p = Path(targets)
        archives = [p] if p.is_file() else scan_archives(p)
    else:
        archives = []
        for item in targets:
            ip = Path(item)
            if ip.is_file():
                archives.append(ip)
            elif ip.is_dir():
                archives.extend(scan_archives(ip))

    if not archives:
        return {}

    def _run_one(arch: Path) -> tuple[Path, bool]:
        dest = (Path(output_dir) / arch.stem) if output_dir is not None and len(archives) > 1 else output_dir
        ok = extract_archive(
            arch,
            output_dir=dest,
            extract_nested=extract_nested,
            max_nested_depth=max_nested_depth,
            remove_source=remove_source,
            check_first=check_first,
            safe=safe,
            policy=policy,
        )
        return arch, ok

    max_workers = max(1, min(workers, len(archives)))
    if max_workers == 1:
        return dict(_run_one(a) for a in archives)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return dict(pool.map(_run_one, archives))
