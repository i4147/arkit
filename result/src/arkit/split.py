"""Memory-bounded binary & archive splitting and merging with optional SHA-256 manifests."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterable
from pathlib import Path

from .atomicio import atomic_open, atomic_write
from .codecs import CHUNK_SIZE
from .exceptions import CorruptedArchiveError, UnknownFormatError
from .formats import Format, detect
from .models import StrPath

__all__ = [
    "merge",
    "merge_archive",
    "merge_files",
    "split",
    "split_archive",
    "split_file",
]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def split(data: bytes, part_size: int) -> list[bytes]:
    """Split ``data`` into a list of byte slices of at most ``part_size`` bytes."""
    if part_size <= 0:
        raise ValueError("part_size must be positive")
    return [data[i : i + part_size] for i in range(0, len(data), part_size)]


def split_file(
    src: StrPath,
    part_size: int,
    out_dir: StrPath | None = None,
    prefix: str | None = None,
    *,
    digits: int = 3,
    manifest: bool = False,
) -> list[Path]:
    """Stream-split ``src`` into numbered part files in bounded ``O(CHUNK_SIZE)`` RAM.

    Unlike naive ``fh.read(part_size)`` implementations that allocate multi-gigabyte
    buffers when ``part_size`` is large, this function copies in 1 MiB slices.
    When ``manifest=True``, also writes ``<prefix>.sha256`` alongside the parts.
    """
    if part_size <= 0:
        raise ValueError("part_size must be positive")

    src_path = Path(src)
    directory = Path(out_dir) if out_dir is not None else src_path.parent
    stem = prefix if prefix is not None else src_path.name
    directory.mkdir(parents=True, exist_ok=True)

    parts: list[Path] = []
    manifest_lines: list[str] = []
    index = 0

    with src_path.open("rb") as fi:
        while True:
            probe = fi.read(min(CHUNK_SIZE, part_size))
            if not probe:
                break
            part_path = directory / f"{stem}.{index:0{digits}d}"
            part_hash = hashlib.sha256()
            remaining = part_size
            with atomic_open(part_path, "wb") as fo:
                fo.write(probe)
                if manifest:
                    part_hash.update(probe)
                remaining -= len(probe)
                while remaining > 0:
                    chunk = fi.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    fo.write(chunk)
                    if manifest:
                        part_hash.update(chunk)
                    remaining -= len(chunk)
            parts.append(part_path)
            if manifest:
                manifest_lines.append(f"{part_hash.hexdigest()}  {part_path.name}")
            index += 1

    if manifest and parts:
        manifest_path = directory / f"{stem}.sha256"
        atomic_write(manifest_path, "\n".join(manifest_lines) + "\n")

    return parts


def merge(parts: Iterable[bytes]) -> bytes:
    """Concatenate an iterable of byte parts into a single ``bytes`` buffer."""
    return b"".join(parts)


def merge_files(
    parts: Iterable[StrPath],
    dst: StrPath,
    sort: bool = False,
    *,
    verify_manifest: StrPath | bool = False,
) -> Path:
    """Stream-merge ``parts`` into ``dst`` atomically in bounded ``O(CHUNK_SIZE)`` RAM.

    If ``verify_manifest`` is ``True`` (auto-discovers ``<dst>.sha256`` or sibling
    manifest) or a path to a ``.sha256`` file, verifies each part's SHA-256 digest
    before committing the merged output.
    """
    dst_path = Path(dst)
    ordered = sorted((Path(x) for x in parts), key=lambda q: q.name) if sort else [Path(x) for x in parts]
    if not ordered:
        raise ValueError("No part files provided to merge_files")

    expected_hashes: dict[str, str] = {}
    if verify_manifest:
        if isinstance(verify_manifest, bool):
            # Derive stem from first part by stripping trailing .NNN
            first = ordered[0]
            base_stem = first.name.rsplit(".", 1)[0]
            manifest_file = first.parent / f"{base_stem}.sha256"
        else:
            manifest_file = Path(verify_manifest)
        if not manifest_file.is_file():
            raise FileNotFoundError(f"Manifest file not found: {manifest_file}")
        for raw_line in manifest_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            digest_hex, name = line.split(None, 1)
            expected_hashes[name.strip().lstrip("*")] = digest_hex.lower()

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_open(dst_path, "wb") as fo:
        for part in ordered:
            if part.name in expected_hashes:
                actual = _sha256_file(part)
                if actual != expected_hashes[part.name]:
                    raise CorruptedArchiveError(
                        f"SHA-256 mismatch for part {part.name}: expected {expected_hashes[part.name]}, got {actual}"
                    )
            with part.open("rb") as fi:
                shutil.copyfileobj(fi, fo, CHUNK_SIZE)
    return dst_path


def split_archive(
    data: bytes,
    fmt: Format | str,
    part_size: int,
    level: int | None = None,
) -> list[bytes]:
    """Compress ``data`` using ``fmt`` and split the resulting payload into byte parts."""
    from .operations import compress_bytes

    return split(compress_bytes(data, fmt, level=level), part_size)


def merge_archive(parts: Iterable[bytes], fmt: Format | str | None = None) -> bytes:
    """Merge byte ``parts`` and decompress the reconstructed payload."""
    from .operations import decompress_bytes

    blob = merge(parts)
    resolved: Format | str | None = fmt
    if resolved is None:
        detected = detect(blob, deep=True)
        if detected is None:
            raise UnknownFormatError("Unable to determine archive format from merged parts")
        resolved = detected
    return decompress_bytes(blob, resolved)
