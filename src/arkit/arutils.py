import bz2
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import gzip
import io
import lzma
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import time
from typing import Any, BinaryIO, Protocol
import zipfile
import zlib

try:
    import cramjam
except ModuleNotFoundError:
    cramjam = None
try:
    import py7zr
except ModuleNotFoundError:
    py7zr = None
__version__ = "1.0.0"
type StrPath = str | os.PathLike[str]
type MembersInput = StrPath | Mapping[str, bytes] | Iterable[Member]
__all__ = [
    "Archive",
    "ArchiveError",
    "ArchiveInfo",
    "Compression",
    "Container",
    "Format",
    "MembersInput",
    "Member",
    "MemberInfo",
    "StrPath",
    "UnknownFormatError",
    "add_member",
    "compress",
    "compress_file",
    "compress_stream",
    "convert",
    "convert_file",
    "decompress",
    "decompress_file",
    "decompress_stream",
    "detect",
    "detect_file",
    "extract",
    "guess_format",
    "info",
    "iter_members",
    "list_members",
    "merge",
    "merge_archive",
    "merge_files",
    "open_archive",
    "pack",
    "pack_file",
    "read_member",
    "read_members",
    "remove_member",
    "split",
    "split_archive",
    "split_file",
    "validate",
    "validate_file",
]


class ArchiveError(Exception):
    pass


class UnknownFormatError(ArchiveError):
    pass


class Compression(Enum):
    BR = "br"
    GZ = "gz"
    XZ = "xz"
    BZ2 = "bz2"
    LZ4 = "lz4"
    ZST = "zst"
    SNAPPY = "snappy"
    SEVENZ = "7z"

    @property
    def magic(self) -> bytes:
        return _MAGIC.get(self, b"")


class Container(Enum):
    TAR = "tar"
    ZIP = "zip"
    SEVENZ = "7z"

    @property
    def magic(self) -> bytes:
        match self:
            case Container.ZIP:
                return b"PK\x03\x04"
            case Container.SEVENZ:
                return _MAGIC7Z
        return b""


class Format(Enum):
    BR = (None, Compression.BR)
    GZ = (None, Compression.GZ)
    XZ = (None, Compression.XZ)
    BZ2 = (None, Compression.BZ2)
    LZ4 = (None, Compression.LZ4)
    ZST = (None, Compression.ZST)
    SNAPPY = (None, Compression.SNAPPY)
    ZIP = (Container.ZIP, None)
    SEVENZ = (Container.SEVENZ, None)
    TAR = (Container.TAR, None)
    TAR_BR = (Container.TAR, Compression.BR)
    TAR_GZ = (Container.TAR, Compression.GZ)
    TAR_XZ = (Container.TAR, Compression.XZ)
    TAR_BZ2 = (Container.TAR, Compression.BZ2)
    TAR_LZ4 = (Container.TAR, Compression.LZ4)
    TAR_ZST = (Container.TAR, Compression.ZST)
    TAR_SNAPPY = (Container.TAR, Compression.SNAPPY)
    TAR_SEVENZ = (Container.TAR, Compression.SEVENZ)

    def __init__(self, container: Container | None, compression: Compression | None) -> None:
        self.container = container
        self.compression = compression

    def __str__(self) -> str:
        return self.ext

    @property
    def ext(self) -> str:
        comp = self.compression
        suffix = f".{comp.value}" if comp is not None else ""
        if self.container is None:
            return suffix
        return f".{self.container.value}{suffix}"

    @property
    def magic(self) -> bytes:
        if self.compression is not None:
            return self.compression.magic
        if self.container is not None:
            return self.container.magic
        return b""

    @property
    def is_archive(self) -> bool:
        return self.container is not None


_MAGIC7Z: bytes = b"7z\xbc\xaf\x27\x1c"
_MAGIC: Mapping[Compression, bytes] = {
    Compression.GZ: b"\x1f\x8b",
    Compression.XZ: b"\xfd7zXZ\x00",
    Compression.BZ2: b"BZh",
    Compression.LZ4: b"\x04\x22\x4d\x18",
    Compression.ZST: b"\x28\xb5\x2f\xfd",
    Compression.SNAPPY: b"\xff\x06\x00\x00sNaPpY",
    Compression.SEVENZ: _MAGIC7Z,
    Compression.BR: b"",
}
_EXT_ALIASES: Mapping[str, Format] = {
    ".tgz": Format.TAR_GZ,
    ".taz": Format.TAR_GZ,
    ".txz": Format.TAR_XZ,
    ".tbz2": Format.TAR_BZ2,
    ".tbz": Format.TAR_BZ2,
    ".tzst": Format.TAR_ZST,
    ".tbr": Format.TAR_BR,
    ".tlz4": Format.TAR_LZ4,
}


@dataclass(frozen=True, slots=True)
class MemberInfo:
    name: str
    size: int
    compressed_size: int | None = None
    mtime: datetime | None = None


@dataclass(frozen=True, slots=True)
class Member:
    info: MemberInfo
    data: bytes

    @property
    def name(self) -> str:
        return self.info.name


@dataclass(frozen=True, slots=True)
class ArchiveInfo:
    path: Path | None
    format: Format
    size: int
    payload_size: int
    member_count: int
    ratio: float
    members: tuple[MemberInfo, ...]


_CHUNK: int = 1 << 20
_SPOOL: int = 64 << 20
_SNIFF: int = 1 << 16
_PAYLOAD_NAME: str = "data"


class _Incremental(Protocol):
    def feed(self, data: bytes) -> bytes: ...
    def finish(self) -> bytes: ...


class _Codec(Protocol):
    def cbytes(self, data: bytes, level: int) -> bytes: ...
    def dbytes(self, data: bytes) -> bytes: ...
    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int: ...
    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int: ...
    def dincr(self) -> _Incremental: ...


@dataclass(slots=True)
class _StdIncr:
    obj: Any

    def feed(self, data: bytes) -> bytes:
        if self.obj.eof:
            return b""
        return self.obj.decompress(data)

    def finish(self) -> bytes:
        return b""


@dataclass(slots=True)
class _CramIncr:
    obj: Any

    def feed(self, data: bytes) -> bytes:
        return bytes(self.obj.decompress(data))

    def finish(self) -> bytes:
        return bytes(self.obj.finish())


@dataclass(slots=True)
class _SevenZIncr:
    buf: bytearray = field(default_factory=bytearray)
    out: bytes = b""
    done: bool = False

    def feed(self, data: bytes) -> bytes:
        self.buf += data
        return b""

    def finish(self) -> bytes:
        if not self.done:
            self.done = True
            self.out = _sevenz_single(bytes(self.buf))
        return self.out


def _require(mod: Any, name: str) -> Any:
    if mod is None:
        raise ArchiveError(f"optional dependency not installed: pip install {name}")
    return mod


def _pump_in(src: BinaryIO, dst: BinaryIO, chunk: int, wrap: Callable[[Any], Any]) -> int:
    total = 0
    stream = wrap(dst)
    try:
        while block := src.read(chunk):
            stream.write(block)
            total += len(block)
        stream.flush()
    finally:
        stream.close()
    return total


def _pump_out(src: BinaryIO, dst: BinaryIO, chunk: int, wrap: Callable[[Any], Any]) -> int:
    total = 0
    stream = wrap(src)
    try:
        while block := stream.read(chunk):
            dst.write(block)
            total += len(block)
    finally:
        stream.close()
    return total


def _copy(src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
    total = 0
    while block := src.read(chunk):
        dst.write(block)
        total += len(block)
    return total


class _GzipCodec:
    def cbytes(self, data: bytes, level: int) -> bytes:
        return gzip.compress(data, compresslevel=level, mtime=0)

    def dbytes(self, data: bytes) -> bytes:
        return gzip.decompress(data)

    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
        return _pump_in(src, dst, chunk, lambda f: gzip.GzipFile(fileobj=f, mode="wb", compresslevel=level, mtime=0))

    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
        return _pump_out(src, dst, chunk, lambda f: gzip.GzipFile(fileobj=f, mode="rb"))

    def dincr(self) -> _Incremental:
        return _StdIncr(zlib.decompressobj(47))


class _XzCodec:
    def cbytes(self, data: bytes, level: int) -> bytes:
        return lzma.compress(data, format=lzma.FORMAT_XZ, preset=level)

    def dbytes(self, data: bytes) -> bytes:
        return lzma.decompress(data)

    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
        return _pump_in(
            src, dst, chunk, lambda f: lzma.LZMAFile(fileobj=f, mode="wb", format=lzma.FORMAT_XZ, preset=level)
        )

    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
        return _pump_out(src, dst, chunk, lambda f: lzma.LZMAFile(fileobj=f, mode="rb"))

    def dincr(self) -> _Incremental:
        return _StdIncr(lzma.LZMADecompressor())


class _Bz2Codec:
    def cbytes(self, data: bytes, level: int) -> bytes:
        return bz2.compress(data, compresslevel=level)

    def dbytes(self, data: bytes) -> bytes:
        return bz2.decompress(data)

    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
        return _pump_in(src, dst, chunk, lambda f: bz2.BZ2File(fileobj=f, mode="wb", compresslevel=level))

    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
        return _pump_out(src, dst, chunk, lambda f: bz2.BZ2File(fileobj=f, mode="rb"))

    def dincr(self) -> _Incremental:
        return _StdIncr(bz2.BZ2Decompressor())


class _CramCodec:
    def __init__(self, mod: Any, level_kw: bool) -> None:
        self._mod: Any = mod
        self._level_kw: bool = level_kw

    def _lib(self) -> Any:
        return _require(self._mod, "cramjam")

    def cbytes(self, data: bytes, level: int) -> bytes:
        lib = self._lib()
        if self._level_kw:
            return bytes(lib.compress(data, level=level))
        return bytes(lib.compress(data))

    def dbytes(self, data: bytes) -> bytes:
        return bytes(self._lib().decompress(data))

    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
        lib = self._lib()
        comp = lib.Compressor(level=level) if self._level_kw else lib.Compressor()
        total = 0
        while block := src.read(chunk):
            dst.write(bytes(comp.compress(block)))
            total += len(block)
        dst.write(bytes(comp.finish()))
        return total

    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
        dec = self._lib().Decompressor()
        total = 0
        while block := src.read(chunk):
            out = dec.decompress(block)
            if out:
                dst.write(bytes(out))
                total += len(out)
        tail = dec.finish()
        if tail:
            dst.write(bytes(tail))
            total += len(tail)
        return total

    def dincr(self) -> _Incremental:
        return _CramIncr(self._lib().Decompressor())


class _SevenZCodec:
    def cbytes(self, data: bytes, level: int) -> bytes:
        return _sevenz_write([_mk_member(data)], level)

    def dbytes(self, data: bytes) -> bytes:
        return _sevenz_single(data)

    def cstream(self, src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
        payload = src.read()
        dst.write(self.cbytes(payload, level))
        return len(payload)

    def dstream(self, src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
        out = self.dbytes(src.read())
        dst.write(out)
        return len(out)

    def dincr(self) -> _Incremental:
        return _SevenZIncr()


_LEVELS: Mapping[Compression, tuple[int, int, int]] = {
    Compression.BR: (0, 11, 11),
    Compression.GZ: (0, 9, 9),
    Compression.XZ: (0, 9, 6),
    Compression.BZ2: (1, 9, 9),
    Compression.LZ4: (0, 0, 0),
    Compression.ZST: (1, 22, 3),
    Compression.SNAPPY: (0, 0, 0),
    Compression.SEVENZ: (0, 9, 5),
}
_CODECS: Mapping[Compression, _Codec] = {
    Compression.GZ: _GzipCodec(),
    Compression.XZ: _XzCodec(),
    Compression.BZ2: _Bz2Codec(),
    Compression.BR: _CramCodec(cramjam, True),
    Compression.LZ4: _CramCodec(cramjam, False),
    Compression.ZST: _CramCodec(cramjam, True),
    Compression.SNAPPY: _CramCodec(cramjam, False),
    Compression.SEVENZ: _SevenZCodec(),
}


def _mk_member(data: bytes) -> Member:
    return Member(MemberInfo(_PAYLOAD_NAME, len(data), None, None), data)


def _single_payload(members: Sequence[Member]) -> bytes:
    if len(members) != 1:
        raise ArchiveError(f"expected exactly one member, found {len(members)}")
    return members[0].data


def _check_names(members: Sequence[Member]) -> None:
    names = [m.info.name for m in members]
    if len(set(names)) != len(names):
        raise ArchiveError("duplicate member names")


def _is_tar(data: bytes) -> bool:
    return len(data) >= 262 and data[257:262] == b"ustar"


def _detect_shallow(data: bytes) -> Format | None:
    if _is_tar(data):
        return Format.TAR
    if data.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return Format.ZIP
    if data.startswith(_MAGIC7Z):
        return Format.SEVENZ
    for comp in (Compression.GZ, Compression.XZ, Compression.BZ2, Compression.LZ4, Compression.ZST, Compression.SNAPPY):
        if data.startswith(_MAGIC[comp]):
            return Format[comp.name]
    return None


def _magic_ok(fmt: Format, data: bytes) -> bool:
    if fmt.container is Container.TAR and fmt.compression is None:
        return _is_tar(data)
    if fmt.container is Container.ZIP:
        return data.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"))
    magic = fmt.magic
    if not magic:
        return True
    return data.startswith(magic)


def _payload_bytes(fmt: Format, data: bytes) -> bytes:
    comp = fmt.compression
    if comp is None:
        return data
    return _CODECS[comp].dbytes(data)


def _encode_payload(fmt: Format, payload: bytes, level: int) -> bytes:
    comp = fmt.compression
    if comp is None:
        return payload
    return _CODECS[comp].cbytes(payload, level)


def _level_bounds(fmt: Format) -> tuple[int, int, int]:
    comp = fmt.compression
    if comp is not None:
        return _LEVELS[comp]
    match fmt.container:
        case Container.ZIP:
            return (0, 9, 6)
        case Container.SEVENZ:
            return (0, 9, 5)
    return (0, 0, 0)


def _resolve_level(fmt: Format, level: int | None) -> int:
    lo, hi, default = _level_bounds(fmt)
    if level is None:
        return default
    if not lo <= level <= hi:
        raise ValueError(f"level {level} outside [{lo}, {hi}] for {fmt.ext}")
    return level


def _zip_time(mt: datetime | None) -> tuple[int, int, int, int, int, int]:
    t = (mt if mt is not None else datetime.now()).timetuple()
    return (max(t.tm_year, 1980), t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)


def _tar_time(ti: tarfile.TarInfo) -> datetime:
    try:
        return datetime.fromtimestamp(int(ti.mtime), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return datetime.fromtimestamp(0, tz=timezone.utc)


def _tar_add(tf: tarfile.TarFile, name: str, data: bytes, mtime: datetime | None) -> None:
    ti = tarfile.TarInfo(name)
    ti.size = len(data)
    ti.mode = 0o644
    ti.mtime = int(mtime.timestamp()) if mtime is not None else int(time.time())
    tf.addfile(ti, io.BytesIO(data))


def _tar_write(members: Sequence[Member]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for m in members:
            _tar_add(tf, m.info.name, m.data, m.info.mtime)
    return buf.getvalue()


def _tar_read(data: bytes) -> list[Member]:
    out: list[Member] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as tf:
        for ti in tf:
            if not ti.isfile():
                continue
            ef = tf.extractfile(ti)
            out.append(Member(MemberInfo(ti.name, ti.size, None, _tar_time(ti)), ef.read() if ef is not None else b""))
    return out


def _tar_stream_write(dst: BinaryIO, source: Path | Mapping[str, bytes] | Iterable[Member]) -> None:
    with tarfile.open(fileobj=dst, mode="w|", format=tarfile.PAX_FORMAT) as tf:
        if isinstance(source, os.PathLike):
            p = Path(source)
            if p.is_dir():
                for fp in sorted(q for q in p.rglob("*") if q.is_file()):
                    tf.add(fp, arcname=fp.relative_to(p).as_posix(), recursive=False)
            else:
                tf.add(p, arcname=p.name, recursive=False)
        elif isinstance(source, Mapping):
            for name, data in source.items():
                _tar_add(tf, str(name), data, None)
        else:
            for m in source:
                _tar_add(tf, m.info.name, m.data, m.info.mtime)


def _zip_pack(out: BinaryIO, source: Path | Mapping[str, bytes] | Iterable[Member], level: int) -> None:
    comp = zipfile.ZIP_STORED if level <= 0 else zipfile.ZIP_DEFLATED
    cl = level if comp == zipfile.ZIP_DEFLATED else None
    with zipfile.ZipFile(out, "w", compression=comp, compresslevel=cl) as zf:
        if isinstance(source, os.PathLike):
            p = Path(source)
            if p.is_dir():
                for fp in sorted(q for q in p.rglob("*") if q.is_file()):
                    zf.write(fp, arcname=fp.relative_to(p).as_posix())
            else:
                zf.write(p, arcname=p.name)
        elif isinstance(source, Mapping):
            for name, data in source.items():
                zf.writestr(str(name), data, compress_type=comp, compresslevel=cl)
        else:
            for m in source:
                zf.writestr(
                    zipfile.ZipInfo(m.info.name, date_time=_zip_time(m.info.mtime)),
                    m.data,
                    compress_type=comp,
                    compresslevel=cl,
                )


def _zip_write(members: Sequence[Member], level: int) -> bytes:
    _check_names(members)
    buf = io.BytesIO()
    _zip_pack(buf, iter(members), level)
    return buf.getvalue()


def _zip_read(data: bytes) -> list[Member]:
    out: list[Member] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for zi in zf.infolist():
            if zi.is_dir():
                continue
            out.append(
                Member(MemberInfo(zi.filename, zi.file_size, zi.compress_size, datetime(*zi.date_time)), zf.read(zi))
            )
    return out


def _zip_stream_into(out: BinaryIO, src: BinaryIO, chunk: int, level: int) -> int:
    total = 0
    comp = zipfile.ZIP_STORED if level <= 0 else zipfile.ZIP_DEFLATED
    cl = level if comp == zipfile.ZIP_DEFLATED else None
    with zipfile.ZipFile(out, "w", compression=comp, compresslevel=cl) as zf:
        with zf.open(zipfile.ZipInfo(_PAYLOAD_NAME, date_time=_zip_time(None)), "w", force_zip64=True) as fh:
            while block := src.read(chunk):
                fh.write(block)
                total += len(block)
    return total


def _zip_stream_compress(src: BinaryIO, dst: BinaryIO, chunk: int, level: int) -> int:
    if dst.seekable():
        return _zip_stream_into(dst, src, chunk, level)
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL)
    try:
        total = _zip_stream_into(spool, src, chunk, level)
        spool.seek(0)
        shutil.copyfileobj(spool, dst, chunk)
        return total
    finally:
        spool.close()


def _ensure_seekable(src: BinaryIO) -> BinaryIO:
    if src.seekable():
        return src
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL)
    shutil.copyfileobj(src, spool, _CHUNK)
    spool.seek(0)
    return spool


def _zip_stream_decompress(src: BinaryIO, dst: BinaryIO, chunk: int) -> int:
    total = 0
    seekable = _ensure_seekable(src)
    owned = seekable is not src
    try:
        seekable.seek(0)
        with zipfile.ZipFile(seekable) as zf:
            infos = [zi for zi in zf.infolist() if not zi.is_dir()]
            if len(infos) != 1:
                raise ArchiveError(f"expected exactly one member, found {len(infos)}")
            with zf.open(infos[0]) as fh:
                while block := fh.read(chunk):
                    dst.write(block)
                    total += len(block)
    finally:
        if owned:
            seekable.close()
    return total


def _sevenz_write(members: Sequence[Member], level: int) -> bytes:
    lib = _require(py7zr, "py7zr")
    _check_names(members)
    buf = io.BytesIO()
    with tempfile.TemporaryDirectory() as td:
        with lib.SevenZipFile(buf, "w", filters=[{"id": lib.FILTER_LZMA2, "preset": level}]) as zf:
            for i, m in enumerate(members):
                fp = Path(td) / f"m{i:06d}"
                fp.write_bytes(m.data)
                zf.write(fp, arcname=m.info.name)
    return buf.getvalue()


def _sevenz_read(data: bytes) -> list[Member]:
    lib = _require(py7zr, "py7zr")
    with lib.SevenZipFile(io.BytesIO(data), "r") as zf:
        infos = {fi.filename: fi for fi in zf.list()}
        files = zf.readall()
    out: list[Member] = []
    for name, bio in files.items():
        fi = infos.get(name)
        if fi is not None and fi.is_directory:
            continue
        payload = bio.read()
        csize = getattr(fi, "compressed", None) if fi is not None else None
        mt = getattr(fi, "creationtime", None) if fi is not None else None
        out.append(
            Member(
                MemberInfo(
                    name,
                    len(payload),
                    csize if isinstance(csize, int) else None,
                    mt if isinstance(mt, datetime) else None,
                ),
                payload,
            )
        )
    return out


def _sevenz_single(data: bytes) -> bytes:
    return _single_payload(_sevenz_read(data))


def _norm_members(source: Path | Mapping[str, bytes] | Iterable[Member]) -> list[Member]:
    if isinstance(source, os.PathLike):
        p = Path(source)
        if p.is_dir():
            out: list[Member] = []
            for fp in sorted(q for q in p.rglob("*") if q.is_file()):
                st = fp.stat()
                out.append(
                    Member(
                        MemberInfo(
                            fp.relative_to(p).as_posix(),
                            st.st_size,
                            None,
                            datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                        ),
                        fp.read_bytes(),
                    )
                )
            return out
        st = p.stat()
        return [
            Member(
                MemberInfo(p.name, st.st_size, None, datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)),
                p.read_bytes(),
            )
        ]
    if isinstance(source, Mapping):
        return [Member(MemberInfo(str(name), len(data), None, None), data) for name, data in source.items()]
    return list(source)


def _members_to_bytes(fmt: Format, members: Sequence[Member], level: int) -> bytes:
    cont = fmt.container
    if cont is None:
        if len(members) != 1:
            raise ArchiveError("raw compression formats hold a single payload")
        comp = fmt.compression
        if comp is None:
            raise UnknownFormatError(str(fmt))
        return _CODECS[comp].cbytes(members[0].data, level)
    _check_names(members)
    match cont:
        case Container.TAR:
            return _encode_payload(fmt, _tar_write(members), level)
        case Container.ZIP:
            return _zip_write(members, level)
        case Container.SEVENZ:
            return _sevenz_write(members, level)
    raise UnknownFormatError(str(fmt))


def _rewrite(p: Path, fmt: Format, members: list[Member], level: int | None) -> Path:
    blob = _members_to_bytes(fmt, members, _resolve_level(fmt, level))
    tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(blob)
        os.replace(tmp, p)
    finally:
        tmp.unlink(missing_ok=True)
    return p


def _safe_join(base: Path, name: str) -> Path:
    rel = Path(name)
    if rel.is_absolute() or rel.drive or ".." in rel.parts:
        raise ArchiveError(f"unsafe member path: {name}")
    return base.joinpath(rel)


def _load_bytes(source: StrPath | bytes) -> bytes:
    if isinstance(source, bytes):
        return source
    return Path(source).read_bytes()


def _resolve_source_fmt(source: StrPath | bytes, fmt: Format | None) -> Format:
    if fmt is not None:
        return fmt
    if isinstance(source, bytes):
        f = detect(source, deep=True)
    else:
        p = Path(source)
        f = guess_format(p)
        if f is not None and f.container is None:
            deeper = detect_file(p, deep=True)
            if deeper is not None and deeper.container is not None:
                f = deeper
        elif f is None:
            f = detect_file(p, deep=True)
    if f is None:
        raise UnknownFormatError("unable to determine format")
    return f


def _strip_ext(name: str, fmt: Format) -> str:
    low = name.lower()
    if fmt.ext and low.endswith(fmt.ext):
        return name[: -len(fmt.ext)]
    return name


class _Sink:
    def __init__(self) -> None:
        self.size = 0

    def write(self, data: bytes) -> int:
        self.size += len(data)
        return len(data)


class _IncrementalReader(io.BufferedIOBase):
    def __init__(self, src: BinaryIO, codec: _Codec, chunk: int) -> None:
        self._src = src
        self._chunk = chunk
        self._inc = codec.dincr()
        self._buf = bytearray()
        self._eof = False

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            while not self._eof:
                self._pull()
            out = bytes(self._buf)
            self._buf.clear()
            return out
        while len(self._buf) < size and not self._eof:
            self._pull()
        out = bytes(self._buf[:size])
        del self._buf[:size]
        return out

    def _pull(self) -> None:
        if self._eof:
            return
        block = self._src.read(self._chunk)
        if block:
            out = self._inc.feed(block)
            if out:
                self._buf += out
        else:
            self._eof = True
            tail = self._inc.finish()
            if tail:
                self._buf += tail


def _payload_reader(src: BinaryIO, fmt: Format, chunk: int) -> BinaryIO:
    comp = fmt.compression
    if comp is None:
        return src
    return _IncrementalReader(src, _CODECS[comp], chunk)


def _read_members(fmt: Format, data: bytes) -> list[Member]:
    cont = fmt.container
    if cont is None:
        comp = fmt.compression
        if comp is None:
            raise UnknownFormatError(str(fmt))
        payload = _CODECS[comp].dbytes(data)
        return [Member(MemberInfo(_PAYLOAD_NAME, len(payload), len(data), None), payload)]
    match cont:
        case Container.TAR:
            return _tar_read(_payload_bytes(fmt, data))
        case Container.ZIP:
            return _zip_read(data)
        case Container.SEVENZ:
            return _sevenz_read(data)
    raise UnknownFormatError(str(fmt))


def detect(data: bytes, deep: bool = False) -> Format | None:
    f = _detect_shallow(data)
    if f is None or not deep:
        return f
    if f is Format.SEVENZ:
        try:
            members = _sevenz_read(data)
        except Exception:
            return f
        if len(members) == 1 and _is_tar(members[0].data):
            return Format.TAR_SEVENZ
        return f
    comp = f.compression
    if f.container is not None or comp is None:
        return f
    try:
        inc = _CODECS[comp].dincr()
        out = bytearray()
        for i in range(0, len(data), _CHUNK):
            out += inc.feed(data[i : i + _CHUNK])
            if len(out) >= 262:
                break
        if len(out) < 262:
            out += inc.finish()
    except Exception:
        return f
    if _is_tar(bytes(out)):
        return Format[f"TAR_{comp.name}"]
    return f


def detect_file(path: StrPath, deep: bool = False) -> Format | None:
    p = Path(path)
    with p.open("rb") as fi:
        head = fi.read(_SNIFF)
    if not deep:
        return _detect_shallow(head)
    if head.startswith(_MAGIC7Z):
        return detect(p.read_bytes(), deep=True)
    return detect(head, deep=True)


def guess_format(path: StrPath) -> Format | None:
    name = Path(path).name.lower()
    best: Format | None = None
    for f in Format:
        if f.ext and name.endswith(f.ext) and (best is None or len(f.ext) > len(best.ext)):
            best = f
    if best is not None:
        return best
    for ext, f in _EXT_ALIASES.items():
        if name.endswith(ext):
            return f
    return None


def compress(data: bytes, fmt: Format, level: int | None = None) -> bytes:
    lvl = _resolve_level(fmt, level)
    cont = fmt.container
    if cont is None:
        comp = fmt.compression
        if comp is None:
            raise UnknownFormatError(str(fmt))
        return _CODECS[comp].cbytes(data, lvl)
    match cont:
        case Container.TAR:
            return _encode_payload(fmt, data, lvl)
        case Container.ZIP:
            return _zip_write([_mk_member(data)], lvl)
        case Container.SEVENZ:
            return _sevenz_write([_mk_member(data)], lvl)
    raise UnknownFormatError(str(fmt))


def decompress(data: bytes, fmt: Format | None = None) -> bytes:
    f = fmt if fmt is not None else _detect_shallow(data)
    if f is None:
        raise UnknownFormatError("unable to determine format")
    cont = f.container
    if cont is None:
        comp = f.compression
        if comp is None:
            raise UnknownFormatError(str(f))
        return _CODECS[comp].dbytes(data)
    match cont:
        case Container.TAR:
            return _payload_bytes(f, data)
        case Container.ZIP:
            return _single_payload(_zip_read(data))
        case Container.SEVENZ:
            return _single_payload(_sevenz_read(data))
    raise UnknownFormatError(str(f))


def compress_stream(src: BinaryIO, dst: BinaryIO, fmt: Format, level: int | None = None, chunk: int = _CHUNK) -> int:
    lvl = _resolve_level(fmt, level)
    cont = fmt.container
    if cont is None:
        comp = fmt.compression
        if comp is None:
            raise UnknownFormatError(str(fmt))
        return _CODECS[comp].cstream(src, dst, chunk, lvl)
    if cont is Container.TAR:
        comp = fmt.compression
        if comp is None:
            return _copy(src, dst, chunk)
        return _CODECS[comp].cstream(src, dst, chunk, lvl)
    if cont is Container.ZIP:
        return _zip_stream_compress(src, dst, chunk, lvl)
    if cont is Container.SEVENZ:
        payload = src.read()
        dst.write(_sevenz_write([_mk_member(payload)], lvl))
        return len(payload)
    raise UnknownFormatError(str(fmt))


def decompress_stream(src: BinaryIO, dst: BinaryIO, fmt: Format | None = None, chunk: int = _CHUNK) -> int:
    if fmt is None:
        src = _ensure_seekable(src)
        pos = src.tell()
        head = src.read(_SNIFF)
        src.seek(pos)
        fmt = _detect_shallow(head)
        if fmt is None:
            raise UnknownFormatError("unable to determine format from stream")
    cont = fmt.container
    if cont is None:
        comp = fmt.compression
        if comp is None:
            raise UnknownFormatError(str(fmt))
        return _CODECS[comp].dstream(src, dst, chunk)
    if cont is Container.TAR:
        comp = fmt.compression
        if comp is None:
            return _copy(src, dst, chunk)
        return _CODECS[comp].dstream(src, dst, chunk)
    if cont is Container.ZIP:
        return _zip_stream_decompress(src, dst, chunk)
    if cont is Container.SEVENZ:
        payload = _sevenz_single(src.read())
        dst.write(payload)
        return len(payload)
    raise UnknownFormatError(str(fmt))


def compress_file(
    src: StrPath, dst: StrPath, fmt: Format | None = None, level: int | None = None, chunk: int = _CHUNK
) -> Path:
    sp = Path(src)
    dp = Path(dst)
    f = fmt if fmt is not None else guess_format(dp)
    if f is None:
        raise UnknownFormatError("cannot determine target format; pass fmt")
    if f.container is not None:
        pack_file(dp, sp, f, level, chunk)
        return dp
    with sp.open("rb") as fi, dp.open("wb") as fo:
        compress_stream(fi, fo, f, level, chunk)
    return dp


def decompress_file(src: StrPath, dst: StrPath, fmt: Format | None = None, chunk: int = _CHUNK) -> Path:
    sp = Path(src)
    dp = Path(dst)
    f = _resolve_source_fmt(sp, fmt)
    with sp.open("rb") as fi, dp.open("wb") as fo:
        decompress_stream(fi, fo, f, chunk)
    return dp


def pack(source: MembersInput, fmt: Format, level: int | None = None) -> bytes:
    buf = io.BytesIO()
    pack_file(buf, source, fmt, level)
    return buf.getvalue()


def pack_file(
    dst: StrPath | BinaryIO,
    source: MembersInput,
    fmt: Format | None = None,
    level: int | None = None,
    chunk: int = _CHUNK,
) -> Path | None:
    dst_path = Path(dst) if isinstance(dst, (str, os.PathLike)) else None
    out: BinaryIO = dst_path.open("wb") if dst_path is not None else dst
    try:
        f = fmt if fmt is not None else (guess_format(dst_path) if dst_path is not None else None)
        if f is None:
            raise UnknownFormatError("target format required when dst is a stream")
        lvl = _resolve_level(f, level)
        src: Path | Mapping[str, bytes] | Iterable[Member] = (
            Path(source) if isinstance(source, (str, os.PathLike)) else source
        )
        cont = f.container
        if cont is None:
            comp = f.compression
            if comp is None:
                raise UnknownFormatError(str(f))
            if isinstance(src, os.PathLike):
                with Path(src).open("rb") as fi:
                    _CODECS[comp].cstream(fi, out, chunk, lvl)
            elif isinstance(src, Mapping):
                if len(src) != 1:
                    raise ArchiveError("raw compression formats hold a single payload")
                (payload,) = src.values()
                with io.BytesIO(payload) as fi:
                    _CODECS[comp].cstream(fi, out, chunk, lvl)
            else:
                members = list(src)
                if len(members) != 1:
                    raise ArchiveError("raw compression formats hold a single payload")
                with io.BytesIO(members[0].data) as fi:
                    _CODECS[comp].cstream(fi, out, chunk, lvl)
            return dst_path
        if cont is Container.TAR:
            spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL)
            try:
                _tar_stream_write(spool, src)
                spool.seek(0)
                comp = f.compression
                if comp is None:
                    shutil.copyfileobj(spool, out, chunk)
                else:
                    _CODECS[comp].cstream(spool, out, chunk, lvl)
            finally:
                spool.close()
            return dst_path
        if cont is Container.ZIP:
            _zip_pack(out, src, lvl)
            return dst_path
        if cont is Container.SEVENZ:
            out.write(_sevenz_write(_norm_members(src), lvl))
            return dst_path
        raise UnknownFormatError(str(f))
    finally:
        if dst_path is not None:
            out.close()


def list_members(source: StrPath | bytes, fmt: Format | None = None) -> list[MemberInfo]:
    f = _resolve_source_fmt(source, fmt)
    return [m.info for m in _read_members(f, _load_bytes(source))]


def read_members(source: StrPath | bytes, fmt: Format | None = None) -> dict[str, bytes]:
    f = _resolve_source_fmt(source, fmt)
    return {m.info.name: m.data for m in _read_members(f, _load_bytes(source))}


def read_member(source: StrPath | bytes, name: str, fmt: Format | None = None) -> bytes:
    f = _resolve_source_fmt(source, fmt)
    for m in _read_members(f, _load_bytes(source)):
        if m.info.name == name:
            return m.data
    raise ArchiveError(f"member not found: {name}")


def iter_members(source: StrPath | bytes, fmt: Format | None = None, chunk: int = _CHUNK) -> Iterator[Member]:
    if isinstance(source, bytes):
        yield from _read_members(_resolve_source_fmt(source, fmt), source)
        return
    p = Path(source)
    f = _resolve_source_fmt(p, fmt)
    cont = f.container
    if cont is None:
        comp = f.compression
        if comp is None:
            raise UnknownFormatError(str(f))
        with p.open("rb") as fi:
            payload = _IncrementalReader(fi, _CODECS[comp], chunk).read(-1)
        yield Member(MemberInfo(_PAYLOAD_NAME, len(payload), None, None), payload)
        return
    if cont is Container.TAR:
        with p.open("rb") as fi:
            reader = _payload_reader(fi, f, chunk)
            with tarfile.open(fileobj=reader, mode="r|") as tf:
                for ti in tf:
                    if not ti.isfile():
                        continue
                    ef = tf.extractfile(ti)
                    yield Member(
                        MemberInfo(ti.name, ti.size, None, _tar_time(ti)), ef.read() if ef is not None else b""
                    )
        return
    yield from _read_members(f, p.read_bytes())


def extract(
    source: StrPath | bytes,
    dest: StrPath,
    fmt: Format | None = None,
    members: Iterable[str] | None = None,
) -> list[Path]:
    want = set(members) if members is not None else None
    base = Path(dest)
    base.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for m in iter_members(source, fmt):
        if want is not None and m.info.name not in want:
            continue
        target = _safe_join(base, m.info.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(m.data)
        written.append(target)
    return written


def add_member(path: StrPath, name: str, data: bytes, fmt: Format | None = None, level: int | None = None) -> Path:
    p = Path(path)
    f = _resolve_source_fmt(p, fmt)
    if f.container is None:
        raise ArchiveError("raw compression formats do not support named members")
    members = _read_members(f, p.read_bytes())
    if any(m.info.name == name for m in members):
        raise ArchiveError(f"member already exists: {name}")
    members.append(Member(MemberInfo(name, len(data), None, datetime.now(timezone.utc)), data))
    return _rewrite(p, f, members, level)


def remove_member(path: StrPath, name: str, fmt: Format | None = None, level: int | None = None) -> Path:
    p = Path(path)
    f = _resolve_source_fmt(p, fmt)
    if f.container is None:
        raise ArchiveError("raw compression formats do not support named members")
    original = _read_members(f, p.read_bytes())
    members = [m for m in original if m.info.name != name]
    if len(members) == len(original):
        raise ArchiveError(f"member not found: {name}")
    return _rewrite(p, f, members, level)


def info(source: StrPath | bytes, fmt: Format | None = None) -> ArchiveInfo:
    if isinstance(source, bytes):
        data = source
        p: Path | None = None
    else:
        p = Path(source)
        data = p.read_bytes()
    f = _resolve_source_fmt(source, fmt)
    members = _read_members(f, data)
    payload = sum(m.info.size for m in members)
    ratio = payload / len(data) if data else 0.0
    return ArchiveInfo(p, f, len(data), payload, len(members), round(ratio, 6), tuple(m.info for m in members))


def validate(data: bytes, fmt: Format | None = None) -> bool:
    try:
        f = fmt if fmt is not None else _detect_shallow(data)
        if f is None or not _magic_ok(f, data):
            return False
        cont = f.container
        if cont is None:
            comp = f.compression
            if comp is None:
                return False
            _CODECS[comp].dbytes(data)
            return True
        if cont is Container.TAR:
            with tarfile.open(fileobj=io.BytesIO(_payload_bytes(f, data)), mode="r:") as tf:
                for _ in tf:
                    pass
            return True
        if cont is Container.ZIP:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                return zf.testzip() is None
        if cont is Container.SEVENZ:
            lib = _require(py7zr, "py7zr")
            with lib.SevenZipFile(io.BytesIO(data), "r") as zf:
                result = zf.test()
            return True if result is None else bool(result)
        return False
    except Exception:
        return False


def validate_file(path: StrPath, fmt: Format | None = None, chunk: int = _CHUNK) -> bool:
    try:
        p = Path(path)
        f = fmt if fmt is not None else _resolve_source_fmt(p, None)
        cont = f.container
        if cont is None:
            comp = f.compression
            if comp is None:
                return False
            with p.open("rb") as src:
                _CODECS[comp].dstream(src, _Sink(), chunk)
            return True
        if cont is Container.TAR:
            with p.open("rb") as src:
                reader = _payload_reader(src, f, chunk)
                with tarfile.open(fileobj=reader, mode="r|") as tf:
                    for _ in tf:
                        pass
            return True
        return validate(p.read_bytes(), f)
    except Exception:
        return False


def convert(data: bytes, fmt: Format, level: int | None = None, src_fmt: Format | None = None) -> bytes:
    sf = src_fmt if src_fmt is not None else detect(data, deep=True)
    if sf is None:
        raise UnknownFormatError("unable to determine source format")
    if sf == fmt:
        return data
    lvl = _resolve_level(fmt, level)
    if sf.container is not None and fmt.container is not None:
        return _members_to_bytes(fmt, _read_members(sf, data), lvl)
    return compress(decompress(data, sf), fmt, lvl)


def convert_file(src: StrPath, dst: StrPath, fmt: Format | None = None, level: int | None = None) -> Path:
    sp = Path(src)
    dp = Path(dst)
    sf = _resolve_source_fmt(sp, None)
    tf = fmt if fmt is not None else guess_format(dp)
    if tf is None:
        raise UnknownFormatError("cannot determine target format; pass fmt")
    if sf == tf:
        if sp.resolve() != dp.resolve():
            shutil.copyfile(sp, dp)
        return dp
    lvl = _resolve_level(tf, level)
    if sf.container is not None and tf.container is not None:
        dp.write_bytes(_members_to_bytes(tf, _read_members(sf, sp.read_bytes()), lvl))
        return dp
    if sf.container is None and tf.container is not None:
        payload = decompress(sp.read_bytes(), sf)
        name = _strip_ext(sp.name, sf) or _PAYLOAD_NAME
        dp.write_bytes(_members_to_bytes(tf, [Member(MemberInfo(name, len(payload), None, None), payload)], lvl))
        return dp
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL)
    try:
        with sp.open("rb") as fi:
            decompress_stream(fi, spool, sf)
        spool.seek(0)
        with dp.open("wb") as fo:
            compress_stream(spool, fo, tf, lvl)
    finally:
        spool.close()
    return dp


def split(data: bytes, part_size: int) -> list[bytes]:
    if part_size <= 0:
        raise ValueError("part_size must be positive")
    return [data[i : i + part_size] for i in range(0, len(data), part_size)]


def split_file(src: StrPath, part_size: int, out_dir: StrPath | None = None, prefix: str | None = None) -> list[Path]:
    if part_size <= 0:
        raise ValueError("part_size must be positive")
    p = Path(src)
    directory = Path(out_dir) if out_dir is not None else p.parent
    stem = prefix if prefix is not None else p.name
    directory.mkdir(parents=True, exist_ok=True)
    parts: list[Path] = []
    index = 0
    with p.open("rb") as fi:
        while block := fi.read(part_size):
            part = directory / f"{stem}.{index:03d}"
            with part.open("wb") as fo:
                fo.write(block)
            parts.append(part)
            index += 1
    return parts


def merge(parts: Iterable[bytes]) -> bytes:
    return b"".join(parts)


def merge_files(parts: Iterable[StrPath], dst: StrPath, sort: bool = False) -> Path:
    dp = Path(dst)
    ordered = sorted((Path(x) for x in parts), key=lambda q: q.name) if sort else [Path(x) for x in parts]
    dp.parent.mkdir(parents=True, exist_ok=True)
    with dp.open("wb") as fo:
        for part in ordered:
            with part.open("rb") as fi:
                shutil.copyfileobj(fi, fo, _CHUNK)
    return dp


def split_archive(data: bytes, fmt: Format, part_size: int, level: int | None = None) -> list[bytes]:
    return split(compress(data, fmt, level), part_size)


def merge_archive(parts: Iterable[bytes], fmt: Format | None = None) -> bytes:
    blob = merge(parts)
    f = fmt if fmt is not None else detect(blob, deep=True)
    if f is None:
        raise UnknownFormatError("unable to determine format")
    return decompress(blob, f)


class Archive:
    def __init__(self, source: StrPath | bytes, fmt: Format | None = None) -> None:
        self._fmt: Format = _resolve_source_fmt(source, fmt)
        self._path: Path | None = None if isinstance(source, bytes) else Path(source)
        self._blob: bytes | None = source if isinstance(source, bytes) else None

    @property
    def format(self) -> Format:
        return self._fmt

    @property
    def path(self) -> Path | None:
        return self._path

    def _load(self) -> bytes:
        if self._blob is not None:
            return self._blob
        if self._path is not None:
            return self._path.read_bytes()
        raise ArchiveError("archive has no source")

    def members(self) -> list[MemberInfo]:
        return [m.info for m in _read_members(self._fmt, self._load())]

    def read(self, name: str) -> bytes:
        for m in _read_members(self._fmt, self._load()):
            if m.info.name == name:
                return m.data
        raise ArchiveError(f"member not found: {name}")

    def read_all(self) -> dict[str, bytes]:
        return {m.info.name: m.data for m in _read_members(self._fmt, self._load())}

    def extract(self, dest: StrPath, members: Iterable[str] | None = None) -> list[Path]:
        source = self._path if self._path is not None else self._blob or b""
        return extract(source, dest, self._fmt, members)

    def add(self, name: str, data: bytes, level: int | None = None) -> None:
        if self._path is None:
            raise ArchiveError("bytes-backed archives are immutable")
        add_member(self._path, name, data, self._fmt, level)

    def remove(self, name: str, level: int | None = None) -> None:
        if self._path is None:
            raise ArchiveError("bytes-backed archives are immutable")
        remove_member(self._path, name, self._fmt, level)

    def payload(self) -> bytes:
        return decompress(self._load(), self._fmt)

    def info(self) -> ArchiveInfo:
        source = self._path if self._path is not None else self._blob or b""
        return info(source, self._fmt)

    def validate(self) -> bool:
        return validate(self._load(), self._fmt)

    def convert(self, fmt: Format, level: int | None = None) -> bytes:
        return convert(self._load(), fmt, level, self._fmt)

    def convert_to(self, dst: StrPath, fmt: Format, level: int | None = None) -> Path:
        target = Path(dst)
        if self._path is None:
            target.write_bytes(self.convert(fmt, level))
        else:
            convert_file(self._path, target, fmt, level)
        return target

    def split(self, part_size: int, out_dir: StrPath | None = None) -> list[Path] | list[bytes]:
        if self._path is not None:
            return split_file(self._path, part_size, out_dir)
        return split(self._blob or b"", part_size)

    def __iter__(self) -> Iterator[Member]:
        if self._path is None:
            yield from _read_members(self._fmt, self._blob or b"")
        else:
            yield from iter_members(self._path, self._fmt)

    def __len__(self) -> int:
        return len(self.members())

    def __repr__(self) -> str:
        origin = str(self._path) if self._path is not None else f"<bytes:{len(self._blob or b'')}>"
        return f"Archive(format={self._fmt.ext}, source={origin})"


@contextmanager
def open_archive(source: StrPath | bytes, fmt: Format | None = None) -> Iterator[Archive]:
    yield Archive(source, fmt)
