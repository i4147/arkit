"""Compatibility tests verifying all original usage snippets, stream/bytes APIs, and ``python -m arkit``."""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import brotli

from arkit import (
    Archive,
    Format,
    Member,
    PBCompressor,
    compress,
    compression_stats,
    convert,
    convert_bytes,
    convert_stream,
    decompress,
    extract_stream,
    pack,
    pack_file,
    pack_stream,
    process_archive,
    read_members,
    validate,
    validate_bytes,
    validate_stream,
)


def test_usage_py_workflow(tmp_path: Path, sample_tree: Path) -> None:
    arch_zst = tmp_path / "project.tar.zst"
    stats = compress(
        sample_tree,
        arch_zst,
        format="tar.zst",
        level=12,
        threads=4,
    )
    assert stats.source_bytes > 0
    assert stats.result_bytes > 0
    assert stats.ratio > 0
    assert isinstance(stats.space_freed, int)
    assert isinstance(stats.space_change_percent, float)

    restored = tmp_path / "restored-project"
    decompress(arch_zst, restored)
    report = validate(arch_zst)
    assert report.healthy is True
    assert report.errors == ()

    proj_zip = tmp_path / "project.zip"
    compress(sample_tree, proj_zip)
    proj_txz = tmp_path / "project.tar.xz"
    conv_stats = convert(
        proj_zip,
        proj_txz,
        output_format="tar.xz",
        level=9,
    )
    assert conv_stats.result_bytes == proj_txz.stat().st_size
    assert validate(proj_txz).healthy is True


def test_usage_arutil_and_usage_br_workflows(tmp_path: Path) -> None:
    # usage_arutil.py
    src_tgz = tmp_path / "data.tar.gz"
    dst_tgz = tmp_path / "cleaned.tar.gz"
    pack_file(src_tgz, {"doc.txt": b"one\n\ntwo\n   \nthree\n"})

    def remove_blank_lines(data: bytes) -> bytes:
        return b"\n".join(line for line in data.splitlines() if line.strip())

    new_archive = process_archive(str(src_tgz), remove_blank_lines, str(dst_tgz))
    assert Path(new_archive) == dst_tgz
    assert read_members(dst_tgz)["doc.txt"] == b"one\ntwo\nthree"

    # usage_br.py
    bigfile = tmp_path / "bigfile.bin"
    bigfile.write_bytes(b"{\"hello\": \"world\"}\n" * 10000)
    pbrt_out = tmp_path / "bigfile.pbrt"
    restored = tmp_path / "bigfile.out"

    seen: list[float] = []

    def on_progress(done: int, total: int | None) -> None:
        pct = 100 * done / total if total else 0.0
        seen.append(pct)

    compressor = PBCompressor(
        quality=3,
        chunk_size=64 * 1024,
        workers=2,
        mode=brotli.MODE_TEXT,
        progress_callback=on_progress,
    )
    compressor.compress_file(str(bigfile), str(pbrt_out))
    assert seen[-1] == 100.0
    PBCompressor.decompress_file(str(pbrt_out), str(restored))
    assert restored.read_bytes() == bigfile.read_bytes()


def test_stream_and_bytes_helpers_and_archive_methods(tmp_path: Path) -> None:
    # pack_stream / extract_stream / validate_stream / validate_bytes / convert_bytes / convert_stream
    members_iter = [
        Member("alpha.txt", b"alpha content " * 50),
        Member("sub/beta.txt", b"beta content " * 50),
    ]
    buf = io.BytesIO()
    pack_stream(buf, members_iter, fmt=Format.TAR_GZ)
    raw_tgz = buf.getvalue()

    assert validate_bytes(raw_tgz) is True
    assert validate_stream(io.BytesIO(raw_tgz), Format.TAR_GZ) is True

    # Convert bytes tar.gz -> zip -> tar.zst
    raw_zip = convert_bytes(raw_tgz, Format.ZIP)
    assert validate_bytes(raw_zip) is True

    zst_out = io.BytesIO()
    convert_stream(io.BytesIO(raw_tgz), zst_out, Format.TAR_ZST, src_fmt=Format.TAR_GZ)
    assert validate_bytes(zst_out.getvalue()) is True

    ext_dir = tmp_path / "from_stream"
    extract_stream(io.BytesIO(zst_out.getvalue()), ext_dir, fmt=Format.TAR_ZST)
    assert (ext_dir / "alpha.txt").read_bytes() == b"alpha content " * 50

    # compression_stats helper
    raw_file = tmp_path / "raw.txt"
    gz_file = tmp_path / "raw.txt.gz"
    raw_file.write_bytes(b"stats check " * 500)
    compress(raw_file, gz_file)
    st = compression_stats(raw_file, gz_file, compute_sha256=True)
    assert st.source_bytes == raw_file.stat().st_size
    assert st.sha256 is not None

    # Archive OO bound methods: pack, add, remove, __contains__, __iter__, iter_members, diff, info, extract
    arch_file = tmp_path / "oo.zip"
    Archive.pack(arch_file, {"m1.txt": b"one", "m2.txt": b"two"})
    handle = Archive(arch_file)
    assert "m1.txt" in handle
    assert len([m.name for m in handle]) == 2
    assert {m.name: m.data for m in handle.iter_members()} == {"m1.txt": b"one", "m2.txt": b"two"}
    handle.add("m3.txt", b"three")
    handle.remove("m2.txt")
    assert handle.info().member_count == 2

    other_file = tmp_path / "oo2.tar.zst"
    pack(other_file.read_bytes() if other_file.exists() else {"m1.txt": b"one", "m3.txt": b"three"}, "tar.zst")
    pack_file(other_file, {"m1.txt": b"one", "m3.txt": b"three"})
    assert handle.diff(other_file).identical is True

    oo_ext = tmp_path / "oo_ext"
    handle.extract(oo_ext)
    assert (oo_ext / "m3.txt").read_bytes() == b"three"


def test_python_m_arkit_module_entrypoint() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "arkit", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "arkit 1.0.0" in proc.stdout
