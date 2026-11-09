"""Tests for ``arkit.atomicio`` and ``arkit.split``."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

from arkit import (
    CorruptedArchiveError,
    Format,
    atomic_open,
    atomic_replace,
    atomic_save,
    atomic_write,
    atomic_write_json,
    atomic_write_text,
    merge,
    merge_archive,
    merge_files,
    split,
    split_archive,
    split_file,
    write_atomic,
)


@dataclass
class _SampleConfig:
    name: str
    tags: set[str]
    path: Path
    created: date


def test_atomic_open_commit_and_rollback_on_exception(tmp_path: Path) -> None:
    target = tmp_path / "important.bin"
    target.write_bytes(b"original-safe-state")

    # Failure mid-stream leaves original file 100% intact and cleans up .tmp file!
    with pytest.raises(RuntimeError, match="disk write aborted"):
        with atomic_open(target, "wb") as fh:
            fh.write(b"partial-corrupt-bytes")
            raise RuntimeError("disk write aborted")

    assert target.read_bytes() == b"original-safe-state"
    assert list(tmp_path.glob(".*.tmp")) == []

    # Clean exit atomically replaces target
    with atomic_open(target, "w", encoding="utf-8", chmod_mode=0o640) as tfh:
        tfh.write("updated-cleanly\n")
    assert target.read_text(encoding="utf-8") == "updated-cleanly\n"


def test_atomic_write_json_backups_aliases_and_skip_unchanged(tmp_path: Path) -> None:
    assert write_atomic is atomic_write
    assert atomic_save is atomic_write
    assert atomic_replace is atomic_write
    assert atomic_write_text is atomic_write
    assert atomic_write_json is atomic_write

    cfg_path = tmp_path / "sub" / "config.json"
    cfg = _SampleConfig(
        name="arkit",
        tags={"fast", "safe"},
        path=Path("/srv/data"),
        created=date(2026, 11, 9),
    )
    lock = threading.Lock()
    assert atomic_write(cfg_path, cfg, lock, sort_keys=True) is True
    parsed = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert parsed["name"] == "arkit"
    assert sorted(parsed["tags"]) == ["fast", "safe"]
    assert parsed["path"] == "/srv/data"
    assert parsed["created"] == "2026-11-09"

    # Backup on overwrite via polymorphic opt=True
    assert atomic_write(cfg_path, "plain text\n", True, newline="\r\n") is True
    assert cfg_path.with_name("config.json.bak").is_file()
    assert cfg_path.read_bytes() == b"plain text\r\n"

    # skip_unchanged avoids touching mtime/rewriting when identical
    mtime_before = cfg_path.stat().st_mtime_ns
    assert atomic_write(cfg_path, b"plain text\r\n", skip_unchanged=True) is True
    assert cfg_path.stat().st_mtime_ns == mtime_before


def test_split_and_merge_files_with_sha256_manifest(tmp_path: Path, single_file: Path) -> None:
    parts_dir = tmp_path / "parts"
    parts = split_file(single_file, part_size=30_000, out_dir=parts_dir, manifest=True)
    assert len(parts) >= 4
    manifest_file = parts_dir / f"{single_file.name}.sha256"
    assert manifest_file.is_file()

    # Merge out-of-order list using sort=True and verify_manifest=True
    restored = tmp_path / "merged.dat"
    merge_files(list(reversed(parts)), restored, sort=True, verify_manifest=True)
    assert restored.read_bytes() == single_file.read_bytes()

    # Corrupting 1 byte in a part must fail manifest verification!
    tampered = bytearray(parts[1].read_bytes())
    tampered[0] ^= 0xFF
    parts[1].write_bytes(bytes(tampered))
    with pytest.raises(CorruptedArchiveError, match="SHA-256 mismatch"):
        merge_files(parts, tmp_path / "should_not_exist.dat", verify_manifest=True)
    assert not (tmp_path / "should_not_exist.dat").exists()


def test_in_memory_split_and_merge_archive() -> None:
    payload = b"in-memory split archive test " * 2000
    parts = split_archive(payload, Format.ZST, part_size=256)
    assert len(parts) >= 1
    assert merge_archive(parts) == payload
    assert merge(split(b"abcdef", 2)) == b"abcdef"
