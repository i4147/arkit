"""Tests for ``ArchiveProcessor``, ``process_archive``, and parallel ``batch`` extraction."""

from __future__ import annotations

from pathlib import Path
from typing import IO

import pytest

from arkit import (
    ArchiveInPlaceError,
    ArchiveProcessor,
    batch_extract,
    compress,
    extract_archive,
    find_archives,
    pack_file,
    process_archive,
    process_tar_archive,
    process_zip_archive,
    read_members,
    scan_archives,
)


def _strip_blank_lines(data: bytes) -> bytes:
    return b"\n".join(line for line in data.splitlines() if line.strip())


@pytest.mark.parametrize("ext", [".tar.gz", ".tar.zst", ".zip", ".whl", ".7z"])
def test_process_archive_copy_and_inplace_modes(tmp_path: Path, ext: str) -> None:
    src = tmp_path / f"input{ext}"
    pack_file(
        src,
        {
            "notes.txt": b"alpha\n\nbeta\n   \ngamma\n",
            "raw.bin": b"\x00\x01\x02\n\n\x03",
        },
    )

    # 1. Copy-to-destination with 1-arg callback (usage_arutil.py style)
    dst = tmp_path / f"cleaned{ext}"
    returned = process_archive(src, _strip_blank_lines, dst)
    assert returned == dst
    assert read_members(dst)["notes.txt"] == b"alpha\nbeta\ngamma"

    # 2. In-place atomic processing with 2-arg callback + filter_func (archiveutils.py style)
    def upper_with_header(data: bytes, filename: str) -> bytes:
        return f"[{filename}]\n".encode() + data.upper()

    count = process_archive(
        src,
        upper_with_header,
        lambda name: name.endswith(".txt"),
    )
    assert count == 1
    updated = read_members(src)
    assert updated["notes.txt"].startswith(b"[notes.txt]\nALPHA")
    assert updated["raw.bin"] == b"\x00\x01\x02\n\n\x03"  # untouched!


def test_process_single_stream_and_streaming_callback(tmp_path: Path) -> None:
    plain = tmp_path / "log.txt"
    plain.write_bytes(b"foo\nbar\nbaz\n")
    gz_file = tmp_path / "log.txt.gz"
    compress(plain, gz_file)

    proc = ArchiveProcessor(gz_file)
    assert proc.list_files() == ["log.txt"]
    count = proc.process_files(lambda d, _n: d.replace(b"bar", b"BAR"))
    assert count == 1

    # Stream-processor callback on a .tar.br archive
    tar_br = tmp_path / "logs.tar.br"
    pack_file(tar_br, {"a.log": b"line1\nline2\n", "b.dat": b"keep"})
    tproc = ArchiveProcessor(tar_br)

    def stream_line_numbers(reader: IO[bytes], writer: IO[bytes], name: str) -> None:
        _ = name
        for idx, line in enumerate(reader, start=1):
            writer.write(f"{idx}:".encode() + line)

    modified = tproc.process_streams(stream_line_numbers, filter_func=lambda n: n.endswith(".log"))
    assert modified == 1
    members = read_members(tar_br)
    assert members["a.log"] == b"1:line1\n2:line2\n"
    assert members["b.dat"] == b"keep"

    # Error handling modes
    def failing_cb(_d: bytes) -> bytes:
        raise ValueError("boom")

    with pytest.raises(ArchiveInPlaceError):
        tproc.process_files(failing_cb, on_error="raise")
    assert tproc.process_files(failing_cb, on_error="skip") == 0


def test_process_tar_and_zip_helpers(tmp_path: Path) -> None:
    tpath = tmp_path / "demo.tar.gz"
    zpath = tmp_path / "demo.zip"
    pack_file(tpath, {"x.txt": b"hello"})
    pack_file(zpath, {"x.txt": b"hello"})

    tout = process_tar_archive(tpath, lambda b: b + b"!")
    zout = process_zip_archive(zpath, lambda b: b + b"!")
    assert read_members(tout)["x.txt"] == b"hello!"
    assert read_members(zout)["x.txt"] == b"hello!"


def test_scan_and_parallel_batch_extract_with_nested_archives(tmp_path: Path) -> None:
    # Build nested archive: outer.zip -> inner.tar.zst -> deep.txt
    inner_bytes_path = tmp_path / "build_inner.tar.zst"
    pack_file(inner_bytes_path, {"deep.txt": b"nested treasure"})

    jobs_dir = tmp_path / "jobs"
    (jobs_dir / "job1").mkdir(parents=True)
    (jobs_dir / "job2").mkdir(parents=True)

    arch1 = jobs_dir / "job1" / "outer.zip"
    pack_file(arch1, {"inner.tar.zst": inner_bytes_path.read_bytes(), "top.txt": b"top1"})
    inner_bytes_path.unlink()

    arch2 = jobs_dir / "job2" / "second.tar.lz4"
    pack_file(arch2, {"hello.txt": b"from lz4"})

    discovered = scan_archives(jobs_dir)
    assert discovered == [arch1, arch2]
    assert find_archives(jobs_dir) == [arch1, arch2]

    # Non-destructive batch extraction (source archives kept by default)
    results = batch_extract(jobs_dir, workers=2, extract_nested=True, remove_source=False)
    assert results == {arch1: True, arch2: True}
    assert arch1.exists() and arch2.exists()
    assert (jobs_dir / "job1" / "top.txt").read_bytes() == b"top1"
    assert (jobs_dir / "job1" / "deep.txt").read_bytes() == b"nested treasure"
    assert (jobs_dir / "job2" / "hello.txt").read_bytes() == b"from lz4"

    # Explicit remove_source=True cleans up archive after extraction
    cleanup_arch = tmp_path / "cleanup.tar.gz"
    pack_file(cleanup_arch, {"done.txt": b"ok"})
    assert extract_archive(cleanup_arch, remove_source=True) is True
    assert not cleanup_arch.exists()
    assert (tmp_path / "done.txt").read_bytes() == b"ok"
