"""Tests for core ``arkit`` operations, conversions, validation, diffing, and ``Archive``."""

from __future__ import annotations

from pathlib import Path

import pytest

from arkit import (
    Archive,
    ArchiveError,
    CorruptedArchiveError,
    Format,
    add_member,
    check_integrity,
    checksum,
    compress,
    compress_file,
    convert,
    convert_file,
    decompress,
    decompress_file,
    diff_archives,
    extract,
    info,
    iter_members,
    list_members,
    open_archive,
    pack_file,
    read_member,
    read_members,
    remove_member,
    validate,
    validate_file,
    verify_archive,
)


@pytest.mark.parametrize(
    "fmt_str",
    [
        "zip",
        "whl",
        "tar",
        "tar.gz",
        "tar.bz2",
        "tar.xz",
        "tar.zst",
        "tar.br",
        "tar.lz4",
        "tar.snappy",
        "tar.7z",
        "7z",
    ],
)
def test_container_compress_validate_inspect_extract_roundtrip(
    tmp_path: Path,
    sample_tree: Path,
    fmt_str: str,
) -> None:
    fmt = Format.parse(fmt_str)
    archive_path = tmp_path / f"bundle{fmt.ext}"

    progress_events: list[tuple[int, int | None]] = []
    stats = compress(
        sample_tree,
        archive_path,
        format=fmt_str,
        compute_sha256=True,
        progress_callback=lambda done, total: progress_events.append((done, total)),
    )
    assert archive_path.is_file()
    assert stats.source_bytes > 10_000  # sums real file contents, NOT 4096 dir inode!
    assert stats.result_bytes == archive_path.stat().st_size
    assert stats.sha256 == checksum(archive_path)
    assert len(progress_events) >= 1

    # Validation
    report = validate(archive_path)
    assert report.healthy is True
    assert bool(report) is True
    assert report.checked_members == 4
    assert validate_file(archive_path) is True
    assert check_integrity(archive_path) is True
    assert verify_archive(archive_path, fmt_str) is True

    # Metadata inspection (streaming)
    arch_info = info(archive_path, compute_sha256=True)
    assert arch_info.member_count == 4
    assert arch_info.payload_size == stats.source_bytes
    assert arch_info.sha256 == stats.sha256

    # Extract & compare byte-for-byte
    restored = tmp_path / f"restored_{fmt.name}"
    decompress(archive_path, restored)
    assert (restored / "README.md").read_bytes() == (sample_tree / "README.md").read_bytes()
    assert (restored / "src" / "pkg" / "data.bin").read_bytes() == (
        sample_tree / "src" / "pkg" / "data.bin"
    ).read_bytes()


@pytest.mark.parametrize("stream_fmt", ["gz", "bz2", "xz", "zst", "br", "lz4", "snappy", "pbrt"])
def test_single_stream_compress_validate_decompress(
    tmp_path: Path,
    single_file: Path,
    stream_fmt: str,
) -> None:
    fmt = Format.parse(stream_fmt)
    out_arch = tmp_path / f"payload.dat{fmt.ext}"
    stats = compress(single_file, out_arch, format=stream_fmt, threads=2)
    assert stats.source_bytes == single_file.stat().st_size
    assert stats.ratio < 0.25  # Highly repetitive payload compresses strongly

    rep = validate(out_arch)
    assert rep.healthy is True
    assert rep.checked_bytes == stats.source_bytes

    extract_dir = tmp_path / f"ext_{stream_fmt}"
    restored_file = decompress(out_arch, extract_dir)
    assert restored_file == extract_dir / "payload.dat"
    assert restored_file.read_bytes() == single_file.read_bytes()

    # Direct file-to-file decompression
    direct_dst = tmp_path / f"direct_{stream_fmt}.bin"
    decompress_file(out_arch, direct_dst)
    assert direct_dst.read_bytes() == single_file.read_bytes()


@pytest.mark.parametrize("fmt_str", ["tar.gz", "tar.zst", "zip"])
def test_deterministic_reproducible_archives(tmp_path: Path, sample_tree: Path, fmt_str: str) -> None:
    out_1 = tmp_path / f"det1.{fmt_str}"
    out_2 = tmp_path / f"det2.{fmt_str}"
    compress(sample_tree, out_1, format=fmt_str, deterministic=True)
    compress(sample_tree, out_2, format=fmt_str, deterministic=True)
    assert out_1.read_bytes() == out_2.read_bytes()


def test_include_exclude_filtering_and_single_file_zip(tmp_path: Path, sample_tree: Path) -> None:
    # Filter to only .py and .md files
    filtered_zip = tmp_path / "filtered.zip"
    compress(sample_tree, filtered_zip, include=["*.py", "*.md"], exclude=["docs/*"])
    names = [m.name for m in list_members(filtered_zip)]
    assert sorted(names) == ["README.md", "src/pkg/__init__.py"]

    # ZIP also accepts a single file directly
    single_zip = tmp_path / "one.zip"
    compress_file(sample_tree / "README.md", single_zip)
    assert read_member(single_zip, "README.md") == (sample_tree / "README.md").read_bytes()


@pytest.mark.parametrize("fmt", [Format.ZIP, Format.TAR_ZST, Format.SEVENZ])
def test_add_remove_read_iter_and_oo_archive(tmp_path: Path, fmt: Format) -> None:
    arch_path = tmp_path / f"mutable{fmt.ext}"
    pack_file(arch_path, {"a.txt": b"alpha", "b.txt": b"beta"}, fmt=fmt)

    add_member(arch_path, "dir/c.txt", b"gamma")
    with pytest.raises(ArchiveError):
        add_member(arch_path, "a.txt", b"duplicate")

    assert read_member(arch_path, "dir/c.txt") == b"gamma"
    remove_member(arch_path, "b.txt")
    with pytest.raises(ArchiveError):
        remove_member(arch_path, "missing.txt")

    assert read_members(arch_path) == {"a.txt": b"alpha", "dir/c.txt": b"gamma"}
    assert {m.name: m.data for m in iter_members(arch_path)} == {
        "a.txt": b"alpha",
        "dir/c.txt": b"gamma",
    }

    # Test OO Archive wrapper (file-backed & bytes-backed & engine mode)
    with open_archive(arch_path) as handle:
        assert len(handle) == 2
        assert handle.validate() is True
        assert handle.read("a.txt") == b"alpha"
        as_tar_gz = handle.convert(Format.TAR_GZ)

    mem_arch = Archive(as_tar_gz)
    assert mem_arch.format is Format.TAR_GZ
    assert mem_arch.read_all() == {"a.txt": b"alpha", "dir/c.txt": b"gamma"}

    engine = Archive(compression_level=3, threads=2)
    src_f = tmp_path / "plain.txt"
    src_f.write_bytes(b"engine test " * 100)
    eng_arch = tmp_path / "eng.tar.zst"
    engine.compress(src_f, eng_arch)
    eng_out = tmp_path / "eng_out"
    engine.decompress(eng_arch, eng_out, validate_first=True)
    assert (eng_out / "plain.txt").read_bytes() == src_f.read_bytes()


def test_cross_format_conversions_and_archive_diff(tmp_path: Path, sample_tree: Path) -> None:
    zip_p = tmp_path / "v1.zip"
    tar_zst_p = tmp_path / "v1.tar.zst"
    tar_xz_p = tmp_path / "v1.tar.xz"
    sevenz_p = tmp_path / "v1.7z"

    compress(sample_tree, zip_p)
    convert(zip_p, tar_zst_p, threads=2)
    convert_file(tar_zst_p, tar_xz_p, level=3)
    convert_file(tar_xz_p, sevenz_p)

    # All four converted containers must have identical member contents!
    assert diff_archives(zip_p, tar_zst_p).identical is True
    assert diff_archives(tar_zst_p, tar_xz_p).identical is True
    assert diff_archives(tar_xz_p, sevenz_p).identical is True

    # Modify v2 and verify diff_archives detects added, removed, modified
    v2_zip = tmp_path / "v2.zip"
    v1_map = read_members(zip_p)
    del v1_map["docs/notes.txt"]
    v1_map["README.md"] = b"Modified README\n"
    v1_map["NEW.txt"] = b"brand new"
    pack_file(v2_zip, v1_map)

    diff = diff_archives(tar_zst_p, v2_zip)
    assert diff.identical is False
    assert diff.added == ("NEW.txt",)
    assert diff.removed == ("docs/notes.txt",)
    assert diff.modified == ("README.md",)
    assert "src/pkg/data.bin" in diff.unchanged

    # Stream <-> Stream and Single-member Container <-> Stream conversions
    gz_p = tmp_path / "readme.md.gz"
    zst_p = tmp_path / "readme.md.zst"
    one_zip = tmp_path / "readme_only.zip"
    compress(sample_tree / "README.md", gz_p)
    convert(gz_p, zst_p)
    convert(zst_p, one_zip)
    back_br = tmp_path / "readme.md.br"
    convert(one_zip, back_br)
    assert decompress(back_br.read_bytes(), format="br") == (sample_tree / "README.md").read_bytes()

    # Multi-file container -> single stream must raise ValueError
    with pytest.raises(ValueError, match="exactly one"):
        convert(zip_p, tmp_path / "bad.gz")


def test_corrupted_archive_validation_and_verify_raises(tmp_path: Path) -> None:
    good_zst = tmp_path / "good.tar.zst"
    pack_file(good_zst, {"file.txt": b"hello world " * 500})
    raw = bytearray(good_zst.read_bytes())
    # Corrupt middle of compressed Zstd frame (checksum enabled!)
    raw[len(raw) // 2] ^= 0xFF
    bad_zst = tmp_path / "bad.tar.zst"
    bad_zst.write_bytes(bytes(raw))

    rep = validate(bad_zst)
    assert rep.healthy is False
    assert bool(rep) is False
    assert len(rep.errors) >= 1

    with pytest.raises(CorruptedArchiveError):
        verify_archive(bad_zst, "tar.zst")

    # Selective extraction test
    multi = tmp_path / "multi.tar.gz"
    pack_file(multi, {"keep.txt": b"1", "skip.txt": b"2"})
    out_sel = tmp_path / "sel"
    written = extract(multi, out_sel, members=["keep.txt"])
    assert written == [out_sel / "keep.txt"]
    assert not (out_sel / "skip.txt").exists()
