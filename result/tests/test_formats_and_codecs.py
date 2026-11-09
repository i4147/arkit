"""Tests for ``arkit.formats``, ``arkit.codecs``, custom plugins, and ``ZstdTarFile``."""

from __future__ import annotations

import io
import shutil
import tarfile
from pathlib import Path

import pytest

from arkit import (
    BrotliStreamReader,
    BrotliStreamWriter,
    CorruptedArchiveError,
    Format,
    SnappyStreamReader,
    SnappyStreamWriter,
    UnknownFormatError,
    UnsupportedFormatError,
    ZstdTarFile,
    codec_available,
    compress,
    compress_stream,
    decompress_bytes,
    decompress_stream,
    detect,
    detect_archive_type,
    detect_file,
    detect_format,
    find_archive_format,
    get_compression_type,
    guess_format,
    level_bounds,
    missing_codecs,
    open_stream_reader,
    open_stream_writer,
    pack,
    preferred_suffix,
    register_codec,
    register_format,
    resolve_level,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("tar.gz", Format.TAR_GZ),
        (".tgz", Format.TAR_GZ),
        ("TAR_ZST", Format.TAR_ZST),
        (".tzst", Format.TAR_ZST),
        ("tar+brotli", Format.TAR_BR),
        ("txz", Format.TAR_XZ),
        ("tbz2", Format.TAR_BZ2),
        ("tlz4", Format.TAR_LZ4),
        ("tar.snappy", Format.TAR_SNAPPY),
        ("tar.7z", Format.TAR_SEVENZ),
        ("wheel", Format.WHL),
        (".whl", Format.WHL),
        ("7zip", Format.SEVENZ),
        ("zstandard", Format.ZST),
        ("gzip", Format.GZ),
        ("pbrt", Format.PBRT),
    ],
)
def test_format_parse_aliases(raw: str, expected: Format) -> None:
    assert Format.parse(raw) is expected
    assert Format.parse(expected) is expected


def test_format_errors_and_suffixes() -> None:
    with pytest.raises(UnknownFormatError):
        Format.parse("")
    with pytest.raises(UnknownFormatError):
        Format.parse("nonexistent_format")
    assert preferred_suffix("tar.zst") == ".tar.zst"
    assert preferred_suffix(Format.TAR_GZ) == ".tar.gz"


@pytest.mark.parametrize(
    "fmt",
    [
        Format.GZ,
        Format.BZ2,
        Format.XZ,
        Format.ZST,
        Format.LZ4,
        Format.SNAPPY,
        Format.PBRT,
        Format.ZIP,
        Format.SEVENZ,
        Format.TAR,
        Format.TAR_GZ,
        Format.TAR_BZ2,
        Format.TAR_XZ,
        Format.TAR_ZST,
        Format.TAR_LZ4,
        Format.TAR_SNAPPY,
        Format.TAR_SEVENZ,
    ],
)
def test_deep_magic_detection_bytes_and_extensionless_files(tmp_path: Path, fmt: Format) -> None:
    payload = {"alpha/hello.txt": b"magic detection test " * 100}
    if fmt.is_archive:
        blob = pack(payload, fmt)
    else:
        blob = compress(b"single stream payload " * 100, fmt)

    assert detect(blob, deep=True) is fmt

    # Write without ANY file extension and verify detect_file & detect_format fallback!
    mystery_path = tmp_path / f"mystery_{fmt.name.lower()}"
    mystery_path.write_bytes(blob)
    assert guess_format(mystery_path) is None
    assert detect_file(mystery_path, deep=True) is fmt
    af = detect_format(mystery_path)
    assert af.format_enum is fmt


@pytest.mark.parametrize("codec", ["gz", "bz2", "xz", "zst", "br", "lz4", "snappy"])
def test_stream_codecs_roundtrip_and_standalone_openers(tmp_path: Path, codec: str) -> None:
    assert codec_available(codec)
    original = (b"streaming-codec-" + codec.encode() + b"-") * 8000  # > 128 KiB (multiple Snappy 64K blocks)

    # Stream to stream
    compressed_buf = io.BytesIO()
    written_in = compress_stream(io.BytesIO(original), compressed_buf, codec)
    assert written_in == len(original)

    compressed_buf.seek(0)
    decompressed_buf = io.BytesIO()
    written_out = decompress_stream(compressed_buf, decompressed_buf, codec)
    assert written_out == len(original)
    assert decompressed_buf.getvalue() == original

    # Standalone file-like open_stream_writer / open_stream_reader
    f_path = tmp_path / f"sample.{codec}"
    with open_stream_writer(f_path, codec) as writer:
        writer.write(original[:50000])
        writer.write(original[50000:])
    with open_stream_reader(f_path, codec) as reader:
        head = reader.read(128)
        rest = reader.read()
    assert head + rest == original


def test_brotli_and_snappy_truncation_detection() -> None:
    data = b"integrity check payload " * 5000

    # Truncated Brotli stream must raise CorruptedArchiveError
    br_buf = io.BytesIO()
    with BrotliStreamWriter(br_buf, quality=4) as bw:
        bw.write(data)
    truncated_br = br_buf.getvalue()[:-5]
    with pytest.raises(CorruptedArchiveError):
        BrotliStreamReader(io.BytesIO(truncated_br)).read()

    # Truncated / headerless Snappy framed stream must raise CorruptedArchiveError
    sn_buf = io.BytesIO()
    with SnappyStreamWriter(sn_buf) as sw:
        sw.write(data)
    valid_sn = sn_buf.getvalue()
    assert SnappyStreamReader(io.BytesIO(valid_sn)).read() == data

    with pytest.raises(CorruptedArchiveError):
        SnappyStreamReader(io.BytesIO(valid_sn[:-6])).read()
    with pytest.raises(CorruptedArchiveError):
        SnappyStreamReader(io.BytesIO(b"\x00\x05\x00\x00hello")).read()


def test_custom_codec_and_format_plugin_registration(tmp_path: Path) -> None:
    # Register a toy XOR-0x5A stream codec and format
    def xor_reader(src: io.BytesIO) -> io.BytesIO:
        raw = src.read()
        return io.BytesIO(bytes(b ^ 0x5A for b in raw))

    def xor_writer(dst: io.BytesIO, _level: int | None) -> io.BytesIO:
        class _XorWriter(io.RawIOBase):
            def writable(self) -> bool:
                return True

            def write(self, b: bytes) -> int:  # type: ignore[override]
                dst.write(bytes(x ^ 0x5A for x in bytes(b)))
                return len(b)

        return _XorWriter()  # type: ignore[return-value]

    register_codec("xor5a", xor_reader, xor_writer)  # type: ignore[arg-type]
    fmt_obj = register_format("xor-stream", suffixes=(".x5a",), kind="single", codec="xor5a")
    assert fmt_obj.kind == "stream"
    assert codec_available("xor5a")
    assert find_archive_format("secret.x5a") is fmt_obj

    buf = io.BytesIO()
    compress_stream(io.BytesIO(b"plugin works!"), buf, "xor5a")
    buf.seek(0)
    out = io.BytesIO()
    decompress_stream(buf, out, "xor5a")
    assert out.getvalue() == b"plugin works!"


def test_zstd_tarfile_direct_subclass(tmp_path: Path, sample_tree: Path) -> None:
    archive_path = tmp_path / "direct.tar.zst"
    with ZstdTarFile(archive_path, "w", level=4, threads=2) as ztf:
        ztf.add(sample_tree / "README.md", arcname="README.md")
        info_obj = tarfile.TarInfo("inline.txt")
        payload = b"inline zstd tar content"
        info_obj.size = len(payload)
        ztf.addfile(info_obj, io.BytesIO(payload))

    with ZstdTarFile(archive_path, "r") as ztf_in:
        names = ztf_in.getnames()
        assert names == ["README.md", "inline.txt"]
        ef = ztf_in.extractfile("inline.txt")
        assert ef is not None
        assert ef.read() == b"inline zstd tar content"


def test_level_validation_and_helpers(tmp_path: Path) -> None:
    assert missing_codecs() == []
    assert level_bounds("zst") == (1, 22, 3)
    assert resolve_level("zst", None) == 3
    with pytest.raises(ValueError):
        resolve_level("zst", 99)
    with pytest.raises(UnsupportedFormatError):
        resolve_level("no_such_codec", 1)

    p = tmp_path / "sample.tar.gz"
    shutil.copyfile(__file__, p)
    assert detect_archive_type(p) == "gz"
    assert get_compression_type(p) == "tar.gz"
    assert decompress_bytes(compress(b"abc", "lz4")) == b"abc"
