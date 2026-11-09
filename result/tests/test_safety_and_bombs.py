"""Security tests: Zip/Tar Slip traversal, symlink escapes, special nodes, and archive bombs."""

from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from arkit import (
    ArchiveBombError,
    SafetyPolicy,
    UnsafeArchivePathError,
    decompress,
    extract,
    safe_destination,
)


@pytest.mark.parametrize(
    "bad_member",
    [
        "../outside.txt",
        "subdir/../../outside.txt",
        "/etc/passwd",
        "\\windows\\system32\\cmd.exe",
        "C:/evil.dll",
        "valid/path\x00/hidden.txt",
    ],
)
def test_safe_destination_rejects_traversal_and_injections(tmp_path: Path, bad_member: str) -> None:
    with pytest.raises(UnsafeArchivePathError):
        safe_destination(tmp_path, bad_member)


def test_zip_slip_blocked_during_extraction(tmp_path: Path) -> None:
    malicious_zip = tmp_path / "slip.zip"
    with zipfile.ZipFile(malicious_zip, "w") as zf:
        zf.writestr("../../escaped.txt", b"pwned")

    dest = tmp_path / "sandbox"
    with pytest.raises(UnsafeArchivePathError):
        extract(malicious_zip, dest)
    assert not (tmp_path.parent / "escaped.txt").exists()


def test_tar_slip_symlink_escape_and_special_fifo_blocked(tmp_path: Path) -> None:
    # 1. Tar path traversal
    slip_tar = tmp_path / "slip.tar.gz"
    with tarfile.open(slip_tar, "w:gz") as tf:
        ti = tarfile.TarInfo("../escape_tar.txt")
        ti.size = 4
        tf.addfile(ti, io.BytesIO(b"nope"))

    with pytest.raises(UnsafeArchivePathError):
        decompress(slip_tar, tmp_path / "out1")

    # 2. Symlink blocked by default; when allow_symlinks=True, escaping link still blocked,
    #    while internal relative symlink is allowed!
    sym_escape_tar = tmp_path / "sym_escape.tar"
    with tarfile.open(sym_escape_tar, "w") as tf:
        sym = tarfile.TarInfo("sub/escape_link")
        sym.type = tarfile.SYMTYPE
        sym.linkname = "../../secret.txt"
        tf.addfile(sym)

    with pytest.raises(UnsafeArchivePathError):
        extract(sym_escape_tar, tmp_path / "out2")
    with pytest.raises(UnsafeArchivePathError):
        extract(sym_escape_tar, tmp_path / "out2", policy=SafetyPolicy(allow_symlinks=True))

    sym_safe_tar = tmp_path / "sym_safe.tar"
    with tarfile.open(sym_safe_tar, "w") as tf:
        reg = tarfile.TarInfo("sub/real.txt")
        reg.size = 5
        tf.addfile(reg, io.BytesIO(b"hello"))
        sym_ok = tarfile.TarInfo("sub/link.txt")
        sym_ok.type = tarfile.SYMTYPE
        sym_ok.linkname = "real.txt"
        tf.addfile(sym_ok)

    with pytest.raises(UnsafeArchivePathError):
        extract(sym_safe_tar, tmp_path / "out_sym_blocked")

    out_sym_ok = tmp_path / "out_sym_ok"
    extract(sym_safe_tar, out_sym_ok, policy=SafetyPolicy(allow_symlinks=True))
    assert (out_sym_ok / "sub" / "link.txt").is_symlink()
    assert (out_sym_ok / "sub" / "link.txt").read_bytes() == b"hello"

    # 3. Special FIFO entry blocked by default
    fifo_tar = tmp_path / "fifo.tar"
    with tarfile.open(fifo_tar, "w") as tf:
        fifo = tarfile.TarInfo("named_pipe")
        fifo.type = tarfile.FIFOTYPE
        tf.addfile(fifo)

    with pytest.raises(UnsafeArchivePathError):
        extract(fifo_tar, tmp_path / "out_fifo")


def test_strip_components_and_expected_top_dir(tmp_path: Path) -> None:
    tar_p = tmp_path / "release.tar.gz"
    with tarfile.open(tar_p, "w:gz") as tf:
        for rel, body in [("app-1.0/bin/run.sh", b"#!/bin/sh\n"), ("app-1.0/README", b"hi")]:
            ti = tarfile.TarInfo(rel)
            ti.size = len(body)
            tf.addfile(ti, io.BytesIO(body))

    # Wrong expected_top_dir raises UnsafeArchivePathError
    with pytest.raises(UnsafeArchivePathError):
        extract(tar_p, tmp_path / "bad_top", policy=SafetyPolicy(expected_top_dir="other-dir"))

    # Matching expected_top_dir + strip_components=1 extracts cleanly at root!
    dest = tmp_path / "stripped"
    extract(
        tar_p,
        dest,
        strip_components=1,
        policy=SafetyPolicy(expected_top_dir="app-1.0"),
    )
    assert (dest / "bin" / "run.sh").read_bytes() == b"#!/bin/sh\n"
    assert (dest / "README").read_bytes() == b"hi"


def test_decompression_bomb_limits_enforced_mid_stream(tmp_path: Path) -> None:
    # Build a highly compressible 4 MiB stream of zeroes (~4 KiB compressed -> >1000x ratio)
    bomb_zst = tmp_path / "zeros.bin.zst"
    raw_zeros = tmp_path / "zeros.bin"
    raw_zeros.write_bytes(b"\x00" * (4 * 1024 * 1024))
    from arkit import compress

    compress(raw_zeros, bomb_zst)

    # 1. Single-file size limit
    with pytest.raises(ArchiveBombError, match="max_file_size"):
        decompress(
            bomb_zst,
            tmp_path / "bomb_file_limit",
            policy=SafetyPolicy(max_file_size=512 * 1024),
        )

    # 2. Ratio limit
    with pytest.raises(ArchiveBombError, match="max_ratio"):
        decompress(
            bomb_zst,
            tmp_path / "bomb_ratio_limit",
            policy=SafetyPolicy(max_ratio=50.0, ratio_min_bytes=256 * 1024),
        )

    # 3. Max file count limit
    many_zip = tmp_path / "many.zip"
    with zipfile.ZipFile(many_zip, "w") as zf:
        for i in range(10):
            zf.writestr(f"f_{i}.txt", b"x")
    with pytest.raises(ArchiveBombError, match="member count"):
        extract(many_zip, tmp_path / "bomb_count", policy=SafetyPolicy(max_files=5))
