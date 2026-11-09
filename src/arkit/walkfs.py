"""
walkfs -- generator-style filesystem walking that also walks *into* archives.

``walkfs.walk()`` recursively visits every entry under a root directory (or a
single file) and transparently descends into archives -- including archives
nested inside other archives.  It is a pure generator: nothing is read,
decompressed or held open until you actually ask for content.

Each visited entry is yielded as a :class:`VirtualPath`, a pathlib-flavoured
path whose string form interleaves archive names, e.g.::

    dist/pkg.whl/mypkg/core.py          (a member of a wheel)
    backup.tar.gz/inner.zip/hello.txt   (a zip inside a tar.gz)

Supported archive formats
-------------------------====================================================

=========  =========================  =============================
container  single-file compression   compressed tar ("tarred" form)
=========  =========================  =============================
.zip       .gz  (gzip)               .tar.gz   (also .tgz)
.whl       .bz2 (bzip2)              .tar.bz2  (also .tbz2, .tbz)
.tar       .xz  (xz/lzma)            .tar.xz   (also .txz)
.7z        .lz4 (lz4 frame)          .tar.lz4
           .zst (zstandard)          .tar.zst   (also .tzst)
           .br  (brotli)             .tar.br
           .7z  (7-Zip container)    .tar.7z
=========  =========================  =============================

A single-file compressed archive (e.g. ``notes.txt.gz``) is treated as a
one-member archive: walking it yields ``notes.txt.gz/notes.txt`` whose
``.open()`` returns the decompressed stream.  If that inner name is itself an
archive (``bundle.zip.gz``), the walk keeps descending (subject to
``max_depth``).

Quick start
-----------

>>> from walkfs import walk
>>> for vp in walk("dist"):
...     print(f"{'  ' * vp.in_archive}{vp}  ({vp.size or '?'} bytes)")
dist/readme.txt  (14 bytes)
dist/pkg.whl  (12045 bytes)
  dist/pkg.whl/mypkg/__init__.py  (31 bytes)
  dist/pkg.whl/mypkg/core.py  (72 bytes)

>>> vp.read_bytes()  # lazy: decompresses on demand
b'CORE = 1\n'
>>> with vp.open() as fh:  # context-managed binary stream
...     head = fh.read(16)

Design notes
------------
* **pathlib-first**: the real filesystem is walked with ``pathlib.Path``;
  yielded values quack like ``PurePosixPath`` (``.name``, ``.suffix``,
  ``.parts``, ``.parent``, ``/``, ``.match()``, ...).
* **Lazy content**: every file entry carries an opener.  Calling ``.open()``
  re-opens the *chain* of archives that contain it, so nothing stays open
  during the walk itself and no decompression happens until you ask.
  Exception: compound tars (``.tar.gz`` ... ``.tar.7z``) and archives nested
  inside other archives are decompressed *once* per walk and kept in memory
  while any of their paths are alive, so member access is cheap and random.
* **Errors are logged, never raised out of ``walk()``**: unreadable
  directories, corrupt or password-protected archives, missing codecs, etc.
  are reported through `loguru <https://github.com/Delgan/loguru>`_ and the
  offending entry is simply yielded as a plain file (or skipped).
* **Depth guard**: nested archive descent is capped by ``max_depth``
  (default 3) to defuse archive bombs.
* ``VirtualPath`` deliberately has **no ``__fspath__``**: a path inside an
  archive is not a real file, so ``open(vp)`` must fail loudly.  Use
  ``vp.open()`` / ``vp.read_bytes()`` instead.

Dependencies
------------
* ``loguru`` (required) -- ``pip install loguru``
* optional codecs, enabled automatically when importable:

  - ``py7zr``      for ``.7z`` / ``.tar.7z``     (``pip install py7zr``)
  - ``zstandard``  for ``.zst`` / ``.tar.zst``   (``pip install zstandard``)
  - ``lz4``        for ``.lz4`` / ``.tar.lz4``  (``pip install lz4``)
  - ``brotli``     for ``.br`` / ``.tar.br``     (``pip install brotli``)

  Missing codecs produce a single loguru warning; the affected files are
  yielded as plain files.  You can also plug in your own codecs with
  :func:`register_codec` + :func:`register_format`.

Requires Python 3.8+.  Single file, zero fuss: drop ``walkfs.py`` anywhere.
"""

from __future__ import annotations

import bz2
import gzip
import importlib
import importlib.util
import io
import lzma
import os
import stat as _stat
import tarfile
import zipfile
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path, PurePosixPath
from types import MappingProxyType, ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from loguru import logger

__version__ = "0.1.0"
__all__ = [
    "walk",
    "VirtualPath",
    "ArchiveFormat",
    "detect_format",
    "register_format",
    "register_codec",
    "codec_available",
    "missing_codecs",
    "FORMATS",
    "logger",
]

if TYPE_CHECKING:  # pragma: no cover
    CodecFactory = Callable[[BinaryIO], BinaryIO]

#: Re-exported so ``from walkfs import logger`` gives you the same loguru
#: logger the library logs to (configure sinks/levels on it).
logger = logger


# ---------------------------------------------------------------------------
# Codecs
# ---------------------------------------------------------------------------

_CODEC_IMPORT: Dict[str, str] = {
    "gz": "gzip",
    "bz2": "bz2",
    "xz": "lzma",
    "lz4": "lz4.frame",
    "zst": "zstandard",
    "br": "brotli",
    "7z": "py7zr",
}
_CODEC_PIP: Dict[str, str] = {
    "lz4": "lz4",
    "zst": "zstandard",
    "br": "brotli",
    "7z": "py7zr",
}

_codec_modules: Dict[str, Optional[ModuleType]] = {}
_codec_warned: set = set()
_custom_codecs: Dict[str, "CodecFactory"] = {}


def codec_available(codec: str) -> bool:
    """Return True if *codec* is importable (or user-registered).

    Stdlib codecs (``gz``/``bz2``/``xz``) are always available.  This function
    never emits a warning; use :func:`missing_codecs` for a quick report.
    """
    if codec in _custom_codecs:
        return True
    if codec in _codec_modules:
        return _codec_modules[codec] is not None
    try:
        return importlib.util.find_spec(_CODEC_IMPORT[codec]) is not None
    except (KeyError, ImportError, ValueError):
        return False


def missing_codecs() -> List[str]:
    """List optional codecs that are not currently installed."""
    return [c for c in ("lz4", "zst", "br", "7z") if not codec_available(c)]


def register_codec(codec: str, factory: "CodecFactory") -> None:
    """Register a custom decompression codec.

    *factory* takes a readable binary stream of compressed bytes and returns
    a readable binary stream of decompressed bytes (it may read the input
    fully and return a ``io.BytesIO``).  Combine with :func:`register_format`::

        import io, lzma

        walkfs.register_codec("lzma-alone", lambda f: io.BytesIO(lzma.decompress(f.read(), format=lzma.FORMAT_ALONE)))
        walkfs.register_format("tar+lzma-alone", suffixes=(".tar.lzma",), kind="tar", codec="lzma-alone")
    """
    _custom_codecs[codec] = factory


def _codec_module(codec: str) -> Optional[ModuleType]:
    """Import (and cache) the module backing *codec*; None if unavailable."""
    if codec not in _codec_modules:
        try:
            _codec_modules[codec] = importlib.import_module(_CODEC_IMPORT[codec])
        except ImportError:
            _codec_modules[codec] = None
            if codec not in _codec_warned:
                _codec_warned.add(codec)
                logger.warning(
                    "Codec {!r} is not installed (pip install {}) -- files using it will be yielded as plain files",
                    codec,
                    _CODEC_PIP.get(codec, codec),
                )
    return _codec_modules[codec]


def _codec_ready(codec: Optional[str]) -> bool:
    if codec is None:
        return True
    if codec in _custom_codecs:
        return True
    return _codec_module(codec) is not None


def _codec_stream(codec: str, fileobj: BinaryIO) -> BinaryIO:
    """Wrap *fileobj* in a decompressing reader for *codec*.

    Callers check availability with :func:`_codec_ready` first; this raises
    only for genuinely unknown codecs.
    """
    custom = _custom_codecs.get(codec)
    if custom is not None:
        return custom(fileobj)
    if codec == "gz":
        return gzip.GzipFile(fileobj=fileobj, mode="rb")
    if codec == "bz2":
        return bz2.BZ2File(fileobj)
    if codec == "xz":
        return lzma.LZMAFile(fileobj)
    if codec == "lz4":
        lz4frame = importlib.import_module("lz4.frame")
        return io.BytesIO(lz4frame.decompress(fileobj.read()))
    if codec == "zst":
        zstandard = importlib.import_module("zstandard")
        dctx = zstandard.ZstdDecompressor()
        try:
            reader = dctx.stream_reader(fileobj, read_across_frames=True)
        except TypeError:  # older zstandard without the kwarg
            reader = dctx.stream_reader(fileobj)
        return io.BufferedReader(reader)
    if codec == "br":
        brotli = importlib.import_module("brotli")
        return io.BytesIO(brotli.decompress(fileobj.read()))
    raise ValueError(f"unknown codec {codec!r}")


@contextmanager
def _decode(codec: str, fileobj: BinaryIO) -> Iterator[BinaryIO]:
    """Context manager wrapping *fileobj* in a codec stream that always closes."""
    stream = _codec_stream(codec, fileobj)
    try:
        yield stream
    finally:
        closer = getattr(stream, "close", None)
        if closer is not None:
            try:
                closer()
            except Exception:  # pragma: no cover - best effort cleanup
                pass


# ---------------------------------------------------------------------------
# py7zr compatibility (0.x exposes read()/readall(); 1.x removed them in
# favour of extract()/extractall() with a WriterFactory -- we support both).
# ---------------------------------------------------------------------------

#: Per-member cap for in-memory 7z extraction (py7zr >= 1.0).
_7Z_MEM_LIMIT = 1 << 30


def _7z_readall(sz: Any) -> Dict[str, Any]:
    """Return {name: stream} for every member of an open SevenZipFile."""
    readall = getattr(sz, "readall", None)
    if readall is not None:  # py7zr 0.x
        return readall() or {}
    pio = importlib.import_module("py7zr.io")
    factory = pio.BytesIOFactory(limit=_7Z_MEM_LIMIT)
    sz.extractall(factory=factory)
    return dict(factory.products)


def _7z_read_member(sz: Any, name: str) -> Optional[Any]:
    """Return a readable stream for one member of an open SevenZipFile."""
    read = getattr(sz, "read", None)
    if read is not None:  # py7zr 0.x
        payload = read([name]) or {}
        return payload.get(name)
    pio = importlib.import_module("py7zr.io")
    factory = pio.BytesIOFactory(limit=_7Z_MEM_LIMIT)
    sz.extract(targets=[name], factory=factory)
    return factory.products.get(name)


# ---------------------------------------------------------------------------
# Formats
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArchiveFormat:
    """Description of a recognisable archive format.

    Attributes:
        name:     human-readable identifier, e.g. ``"tar+zstd"``.
        kind:     how to read it -- ``"zip"`` (zip/wheel), ``"tar"``,
                  ``"7z"`` (7-Zip container) or ``"single"`` (one-member
                  compressed file).
        codec:    codec id for compressed variants (``"gz"``, ``"zst"``,
                  ``"7z"``, ... or a custom one), or None.
        suffixes: filename suffixes (with dot) that select this format.
                  Longest suffix wins, so ``x.tar.gz`` matches ``.tar.gz``
                  before ``.gz``.
    """

    name: str
    kind: str
    codec: Optional[str] = None
    suffixes: Tuple[str, ...] = ()


_FORMATS: Dict[str, ArchiveFormat] = {}
_SUFFIX_TO_FORMAT: Dict[str, ArchiveFormat] = {}
_SUFFIX_ORDER: List[str] = []


def _rebuild_suffix_map() -> None:
    _SUFFIX_TO_FORMAT.clear()
    for fmt in _FORMATS.values():
        for suffix in fmt.suffixes:
            _SUFFIX_TO_FORMAT[suffix] = fmt
    _SUFFIX_ORDER.clear()
    _SUFFIX_ORDER.extend(sorted(_SUFFIX_TO_FORMAT, key=len, reverse=True))


def register_format(
    name: str,
    *,
    suffixes: Tuple[str, ...],
    kind: str,
    codec: Optional[str] = None,
) -> ArchiveFormat:
    """Register (or replace) an archive format.

    Example -- teach walkfs about Java archives::

        walkfs.register_format("jar", suffixes=(".jar",), kind="zip")

    Re-registering the same *name* replaces its suffixes.  Matching is
    case-insensitive and longest-suffix-first.
    """
    if kind not in ("zip", "tar", "7z", "single"):
        raise ValueError(f"kind must be one of zip/tar/7z/single, got {kind!r}")
    normalised = tuple(s if s.startswith(".") else "." + s for s in suffixes)
    if not normalised:
        raise ValueError("at least one suffix is required")
    fmt = ArchiveFormat(name=name, kind=kind, codec=codec, suffixes=normalised)
    _FORMATS[name] = fmt
    _rebuild_suffix_map()
    return fmt


def _register_defaults() -> None:
    register_format("zip", suffixes=(".zip",), kind="zip")
    register_format("wheel", suffixes=(".whl",), kind="zip")
    register_format("tar", suffixes=(".tar",), kind="tar")
    register_format("7z", suffixes=(".7z",), kind="7z")
    register_format("tar+gzip", suffixes=(".tar.gz", ".tgz"), kind="tar", codec="gz")
    register_format("tar+bzip2", suffixes=(".tar.bz2", ".tbz2", ".tbz"), kind="tar", codec="bz2")
    register_format("tar+xz", suffixes=(".tar.xz", ".txz"), kind="tar", codec="xz")
    register_format("tar+lz4", suffixes=(".tar.lz4",), kind="tar", codec="lz4")
    register_format("tar+zstd", suffixes=(".tar.zst", ".tzst"), kind="tar", codec="zst")
    register_format("tar+brotli", suffixes=(".tar.br",), kind="tar", codec="br")
    register_format("tar+7z", suffixes=(".tar.7z",), kind="tar", codec="7z")
    register_format("gzip", suffixes=(".gz",), kind="single", codec="gz")
    register_format("bzip2", suffixes=(".bz2",), kind="single", codec="bz2")
    register_format("xz", suffixes=(".xz",), kind="single", codec="xz")
    register_format("lz4", suffixes=(".lz4",), kind="single", codec="lz4")
    register_format("zstd", suffixes=(".zst",), kind="single", codec="zst")
    register_format("brotli", suffixes=(".br",), kind="single", codec="br")


_register_defaults()

#: Live, read-only view of the registered formats (name -> ArchiveFormat).
FORMATS: Mapping[str, ArchiveFormat] = MappingProxyType(_FORMATS)


def detect_format(name: Union[str, "os.PathLike[str]"]) -> Optional[ArchiveFormat]:
    """Return the :class:`ArchiveFormat` matching a filename, or None.

    Matching is case-insensitive and longest-suffix-first, so
    ``data.tar.gz`` resolves to ``tar+gzip`` rather than ``gzip``.
    """
    if isinstance(name, os.PathLike):
        name = os.fspath(name)
    lowered = str(name).lower()
    for suffix in _SUFFIX_ORDER:
        if lowered.endswith(suffix):
            return _SUFFIX_TO_FORMAT[suffix]
    return None


# ---------------------------------------------------------------------------
# Sources and members
# ---------------------------------------------------------------------------


class _FileSource:
    """A real file on disk; every ``open()`` returns a fresh handle."""

    __slots__ = ("path",)

    def __init__(self, path: Path) -> None:
        self.path = path

    def open(self) -> BinaryIO:
        return open(self.path, "rb")


class _BytesSource:
    """Materialised bytes (e.g. a nested archive); ``open()`` is cheap."""

    __slots__ = ("data",)

    def __init__(self, data: bytes) -> None:
        self.data = data

    def open(self) -> BinaryIO:
        return io.BytesIO(self.data)


@dataclass(frozen=True)
class _Member:
    """One entry inside an archive."""

    name: str  # normalised display name ('pkg/lib.py')
    raw: str  # original name as stored (for lookups)
    is_dir: bool
    is_symlink: bool = False
    size: Optional[int] = None


def _norm_name(name: str) -> Optional[str]:
    """Normalise an archive member name; None for degenerate names."""
    name = name.replace("\\", "/")
    parts = [p for p in name.split("/") if p not in ("", ".")]
    return "/".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Archive readers
#
# Each reader exposes:
#   members()               -> List[_Member]     (opens + closes its source)
#   open_member(member)     -> context manager yielding a binary stream
#
# open_member() is *reopenable*: every call re-opens the source, so streams
# from the same archive never share state (safe to interleave, and usable
# after the walk has finished).
# ---------------------------------------------------------------------------


class _ZipReader:
    """zip / wheel archives (random access, never materialised)."""

    __slots__ = ("_source",)

    def __init__(self, source: Union[_FileSource, _BytesSource]) -> None:
        self._source = source

    def members(self) -> List[_Member]:
        out: List[_Member] = []
        seen: set = set()
        encrypted = False
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            zf = stack.enter_context(zipfile.ZipFile(fh))
            for zi in zf.infolist():
                name = _norm_name(zi.filename)
                if name is None:
                    continue
                mode = (zi.external_attr >> 16) & 0o170000
                is_dir = zi.is_dir() or mode == 0o040000
                is_symlink = mode == 0o120000
                if zi.flag_bits & 0x1:
                    encrypted = True
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _Member(
                        name=name,
                        raw=zi.filename,
                        is_dir=is_dir,
                        is_symlink=is_symlink,
                        size=None if is_dir else zi.file_size,
                    )
                )
        if encrypted:
            logger.warning(
                "Archive {} contains password-protected members; .open() will raise for those members",
                self._source.path if isinstance(self._source, _FileSource) else "archive",
            )
        return out

    @contextmanager
    def open_member(self, member: _Member) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            zf = stack.enter_context(zipfile.ZipFile(fh))
            try:
                stream = stack.enter_context(zf.open(member.raw))
            except RuntimeError as exc:  # encrypted member, unsupported
                raise RuntimeError(
                    f"cannot open {member.raw!r}: {exc} (password-protected members are not supported)"
                ) from exc
            yield stream


class _TarReader:
    """tar archives; compound tars are decompressed once and kept in memory."""

    __slots__ = ("_source",)

    def __init__(
        self,
        source: Union[_FileSource, _BytesSource],
        fmt: ArchiveFormat,
    ) -> None:
        if fmt.codec is None:
            self._source = source
        elif fmt.codec == "7z":
            self._source = _BytesSource(_tar7z_payload(source))
        else:
            with ExitStack() as stack:
                fh = stack.enter_context(source.open())
                cs = stack.enter_context(_decode(fmt.codec, fh))
                self._source = _BytesSource(cs.read())

    def members(self) -> List[_Member]:
        out: List[_Member] = []
        seen: set = set()
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            tf = stack.enter_context(tarfile.open(fileobj=fh, mode="r:"))
            for m in tf.getmembers():
                name = _norm_name(m.name)
                if name is None:
                    continue
                is_dir, is_symlink = m.isdir(), m.issym()
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _Member(
                        name=name,
                        raw=m.name,
                        is_dir=is_dir,
                        is_symlink=is_symlink,
                        size=None if is_dir else m.size,
                    )
                )
        return out

    @contextmanager
    def open_member(self, member: _Member) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            tf = stack.enter_context(tarfile.open(fileobj=fh, mode="r:"))
            try:
                m = tf.getmember(member.raw)
            except KeyError:
                raise KeyError(f"member {member.raw!r} not found") from None
            fobj = tf.extractfile(m)
            if fobj is None:
                raise OSError(f"member {member.name!r} has no file data (symlink / special tar entry)")
            stack.enter_context(closing(fobj))
            yield fobj


def _tar7z_payload(source: Union[_FileSource, _BytesSource]) -> bytes:
    """Extract the single tar member of a .tar.7z archive to bytes."""
    py7zr = _codec_module("7z")
    if py7zr is None:  # pragma: no cover - guarded by _codec_ready
        raise RuntimeError("py7zr is required for .tar.7z archives")
    with ExitStack() as stack:
        fh = stack.enter_context(source.open())
        sz = stack.enter_context(py7zr.SevenZipFile(fh, "r"))
        payload = _7z_readall(sz)
    files = {k: v for k, v in payload.items() if v is not None}
    if len(files) != 1:
        raise ValueError(f".tar.7z archive must contain exactly one member, found {len(files)}")
    return next(iter(files.values())).read()


class _SevenZipReader:
    """7-Zip container archives (requires py7zr)."""

    __slots__ = ("_source", "_py7zr")

    def __init__(self, source: Union[_FileSource, _BytesSource]) -> None:
        self._source = source
        self._py7zr = _codec_module("7z")
        if self._py7zr is None:  # pragma: no cover - guarded by _codec_ready
            raise RuntimeError("py7zr is required for .7z archives")

    @staticmethod
    def _info_get(info: Any, key: str, default: Any = None) -> Any:
        """Read a field from a py7zr FileInfo regardless of its shape."""
        value = getattr(info, key, None)
        if value is None and isinstance(info, dict):
            value = info.get(key)
        return default if value is None else value

    def members(self) -> List[_Member]:
        out: List[_Member] = []
        seen: set = set()
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            sz = stack.enter_context(self._py7zr.SevenZipFile(fh, "r"))
            for info in sz.list():
                raw = self._info_get(info, "filename")
                if not raw:
                    continue
                name = _norm_name(raw)
                if name is None:
                    continue
                is_dir = bool(self._info_get(info, "is_directory", False))
                size = self._info_get(info, "uncompressed")
                key = (name, is_dir)
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    _Member(
                        name=name,
                        raw=raw,
                        is_dir=is_dir,
                        is_symlink=False,  # py7zr's listing carries no link flag
                        size=None if is_dir else (int(size) if size is not None else None),
                    )
                )
        return out

    @contextmanager
    def open_member(self, member: _Member) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            sz = stack.enter_context(self._py7zr.SevenZipFile(fh, "r"))
            bio = _7z_read_member(sz, member.raw)
            if bio is None:  # empty file / directory entry
                bio = io.BytesIO()
            yield bio


class _SingleCodecReader:
    """A bare compressed file (e.g. notes.txt.gz) -- exactly one member."""

    __slots__ = ("_source", "_codec", "_inner")

    def __init__(
        self,
        source: Union[_FileSource, _BytesSource],
        fmt: ArchiveFormat,
        base_name: str,
    ) -> None:
        self._source = source
        self._codec = fmt.codec
        inner = base_name
        for suffix in fmt.suffixes:
            if inner.lower().endswith(suffix):
                inner = inner[: -len(suffix)]
                break
        self._inner = inner or (base_name + ".decoded")

    def members(self) -> List[_Member]:
        return [_Member(name=self._inner, raw=self._inner, is_dir=False, is_symlink=False, size=None)]

    @contextmanager
    def open_member(self, member: _Member) -> Iterator[BinaryIO]:
        with ExitStack() as stack:
            fh = stack.enter_context(self._source.open())
            stream = stack.enter_context(_decode(self._codec, fh))
            yield stream


def _make_reader(
    source: Union[_FileSource, _BytesSource],
    fmt: ArchiveFormat,
    base_name: str,
) -> object:
    if fmt.kind == "zip":
        return _ZipReader(source)
    if fmt.kind == "tar":
        return _TarReader(source, fmt)
    if fmt.kind == "7z":
        return _SevenZipReader(source)
    if fmt.kind == "single":
        return _SingleCodecReader(source, fmt, base_name)
    raise ValueError(f"unknown format kind {fmt.kind!r}")


def _materialize(reader: Any, member: _Member, label: str) -> Optional[_BytesSource]:
    """Read a nested archive member fully into memory (with error logging)."""
    try:
        with reader.open_member(member) as fh:
            return _BytesSource(fh.read())
    except Exception as exc:
        logger.error(
            "Cannot read nested archive {} ({}: {})",
            label,
            type(exc).__name__,
            exc,
        )
        return None


def _vjoin(parts: Tuple[str, ...]) -> str:
    """Join virtual path parts into a posix string (handles the '/' anchor)."""
    return PurePosixPath(*parts).as_posix() if parts else "."


def _real_file_opener(path: Path):
    @contextmanager
    def _open() -> Iterator[BinaryIO]:
        with open(path, "rb") as fh:
            yield fh

    return _open


# ---------------------------------------------------------------------------
# VirtualPath
# ---------------------------------------------------------------------------


class _ManagedStream:
    """A readable binary stream that owns (and closes) its archive handles.

    ``VirtualPath.open()`` returns this wrapper.  Use it directly
    (``f = vp.open(); f.read(); f.close()``) or as a context manager
    (``with vp.open() as f: ...``).  Everything else (``read``, ``seek``,
    ``tell``, ``readinto``, ...) is delegated to the wrapped stream.
    """

    __slots__ = ("_stream", "_stack")

    def __init__(self, stream: BinaryIO, stack: "ExitStack") -> None:
        self._stream = stream
        self._stack = stack

    def __getattr__(self, name: str) -> Any:
        if name in ("_stream", "_stack"):  # guard against recursion pre-init
            raise AttributeError(name)
        return getattr(self._stream, name)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        try:
            return bool(self._stream.seekable())
        except Exception:  # pragma: no cover - exotic streams
            return False

    def close(self) -> None:
        """Release the underlying stream and every archive handle beneath it."""
        self._stack.close()

    def __enter__(self) -> "_ManagedStream":
        return self

    def __exit__(self, *exc_info: Any) -> bool:
        self.close()
        return False

    def __del__(self) -> None:  # pragma: no cover - best effort
        try:
            self._stack.close()
        except Exception:
            pass

    def __iter__(self):
        return iter(self._stream)

    def __repr__(self) -> str:
        return f"<_ManagedStream {self._stream!r}>"


class VirtualPath:
    """A pathlib-flavoured path that may live inside (nested) archives.

    Instances are produced by :func:`walk`; you can also derive new ones with
    ``/``, ``.parent`` or ``.relative_to()`` (derived paths are for matching
    and display only -- they have no content opener).

    Path-ish API: ``parts``, ``name``, ``stem``, ``suffix``, ``suffixes``,
    ``parent``, ``parents``, ``__truediv__``, ``match()``, ``as_posix()``,
    ordering and hashing behave like ``PurePosixPath``.

    Walk-specific API:

    - ``origin``     -- the real ``Path`` passed to ``walk()``.
    - ``real``       -- real on-disk file this entry's bytes come from
                        (the outermost archive for nested members).
    - ``depth``      -- archive nesting level (0 = real filesystem).
    - ``in_archive`` -- ``depth > 0``.
    - ``kind``       -- ``'file'`` | ``'dir'`` | ``'symlink'`` | ``'special'``.
    - ``is_dir()`` / ``is_file()`` / ``is_symlink()`` -- lstat-style flags.
    - ``size``       -- uncompressed size in bytes if known (``None`` for
                        dirs, special files and single-codec members).
    - ``open(mode='rb')`` -- readable binary stream, lazy (usable directly
      or as a context manager; ``close()`` releases the archive handles).
    - ``read_bytes()`` / ``read_text()`` -- convenience wrappers.

    There is intentionally no ``__fspath__``: passing these to ``open(vp)``
    would be a lie for virtual paths.  Note also that ``is_file()`` uses
    lstat semantics: an unfollowed symlink is not a file.
    """

    __slots__ = (
        "_parts",
        "_origin",
        "_real",
        "_depth",
        "_kind",
        "_is_symlink",
        "_size",
        "_opener",
        "_str",
    )

    def __init__(
        self,
        parts,
        *,
        origin: Union[str, Path],
        real: Optional[Path] = None,
        depth: int = 0,
        kind: str = "file",
        is_symlink: bool = False,
        size: Optional[int] = None,
        opener: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._parts = tuple(str(p) for p in parts)
        self._origin = Path(origin)
        self._real = Path(real) if real is not None else None
        self._depth = int(depth)
        self._kind = kind
        self._is_symlink = bool(is_symlink)
        self._size = size
        self._opener = opener
        self._str = str(PurePosixPath(*self._parts)) if self._parts else "."

    # -- path-ish interface -------------------------------------------------

    def as_posix(self) -> str:
        return self._str

    def __str__(self) -> str:
        return self._str

    def __repr__(self) -> str:
        return f"VirtualPath({self._str!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, VirtualPath) and self._origin == other._origin and self._parts == other._parts

    def __hash__(self) -> int:
        return hash((self._origin, self._parts))

    def __lt__(self, other: "VirtualPath") -> bool:
        if not isinstance(other, VirtualPath):
            return NotImplemented
        return self._parts < other._parts

    def __truediv__(self, other: Union[str, "os.PathLike[str]"]) -> "VirtualPath":
        if isinstance(other, VirtualPath):
            raise TypeError("cannot append a VirtualPath; append a str/PathLike instead")
        text = os.fspath(other) if isinstance(other, os.PathLike) else str(other)
        extra = tuple(p for p in text.split("/") if p not in ("", "."))
        return VirtualPath(
            self._parts + extra,
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind="file",
            is_symlink=False,
            size=None,
            opener=None,
        )

    @property
    def parts(self) -> Tuple[str, ...]:
        return self._parts

    @property
    def name(self) -> str:
        return self._parts[-1] if self._parts else ""

    @property
    def stem(self) -> str:
        return PurePosixPath(self.name).stem

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.name).suffix

    @property
    def suffixes(self) -> List[str]:
        return PurePosixPath(self.name).suffixes

    @property
    def parent(self) -> "VirtualPath":
        if self._parts == ("/",):
            return self
        if not self._parts:
            return VirtualPath((), origin=self._origin, real=self._real, depth=self._depth, kind="dir")
        return VirtualPath(
            self._parts[:-1],
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind="dir",
        )

    @property
    def parents(self) -> Iterator["VirtualPath"]:
        current = self
        while len(current._parts) > 1:
            current = current.parent
            yield current

    def match(self, pattern: str) -> bool:
        return PurePosixPath(*self._parts).match(pattern)

    def relative_to(self, other: Union[str, Path, "VirtualPath"]) -> "VirtualPath":
        """Return this path relative to *other*, keeping the content opener.

        The result still supports ``open()``/``read_bytes()``, so
        ``vp.relative_to(walk_root)`` gives you a portable virtual path such
        as ``dist/pkg.whl/mypkg/core.py``.
        """
        if isinstance(other, VirtualPath):
            base = other._parts
        else:
            base = PurePosixPath(Path(other).as_posix()).parts
        if self._parts[: len(base)] != base:
            raise ValueError(f"{self} is not relative to {other}")
        return VirtualPath(
            self._parts[len(base) :],
            origin=self._origin,
            real=self._real,
            depth=self._depth,
            kind=self._kind,
            is_symlink=self._is_symlink,
            size=self._size,
            opener=self._opener,
        )

    # -- walk-specific interface --------------------------------------------

    @property
    def origin(self) -> Path:
        return self._origin

    @property
    def real(self) -> Optional[Path]:
        return self._real

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def in_archive(self) -> bool:
        return self._depth > 0

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def size(self) -> Optional[int]:
        return self._size

    def is_dir(self) -> bool:
        return self._kind == "dir"

    def is_file(self) -> bool:
        return self._kind == "file"

    def is_symlink(self) -> bool:
        return self._is_symlink

    def open(self, mode: str = "rb") -> _ManagedStream:
        """Return a readable binary stream of this entry's content.

        Content is produced lazily: opening re-opens the chain of archives
        that contain this entry (fresh handles, no shared state).  The
        returned stream works standalone (``f.read(); f.close()``) or as a
        context manager (``with vp.open() as f:``).  Only ``'rb'`` / ``'r'``
        are accepted.
        """
        if mode not in ("r", "rb"):
            raise ValueError(f"unsupported mode {mode!r}: only binary reading is supported")
        if self._kind == "dir":
            raise IsADirectoryError(f"{self} is a directory")
        if self._kind == "symlink":
            raise OSError(
                f"{self} is a symlink entry; walkfs never follows symlinks "
                "into archives and unfollowed links have no content here"
            )
        if self._kind == "special":
            raise OSError(f"{self} is a special file (fifo/socket/device)")
        if self._opener is None:
            raise ValueError(f"{self} has no backing content (path built by manipulation, not yielded by walk())")
        stack = ExitStack()
        stream = stack.enter_context(self._opener())
        return _ManagedStream(stream, stack)

    def read_bytes(self) -> bytes:
        with self.open() as fh:
            return fh.read()

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self.read_bytes().decode(encoding, errors)


# ---------------------------------------------------------------------------
# The walker
# ---------------------------------------------------------------------------


def walk(
    root: Union[str, "os.PathLike[str]"],
    *,
    max_depth: int = 3,
    include_dirs: bool = True,
    follow_symlinks: bool = False,
) -> Iterator[VirtualPath]:
    """Walk *root* and every archive found beneath it (generator).

    Parameters:
        root: Directory (traversed recursively, like ``Path.rglob`` -- the
            root itself is not yielded) or a single file (which *is* yielded;
            if it is an archive, its members are walked too).
        max_depth: How many nested archive layers may be entered.
            ``0`` disables archive descent entirely, ``1`` only opens
            archives found on the real filesystem, ``N`` descends up to N
            layers (guards against archive bombs).  Default ``3``.
        include_dirs: Also yield directory entries (default ``True``), like
            ``Path.rglob``.  Implicit directories (a zip that stores
            ``a/b.txt`` with no entry for ``a/``) are never fabricated.
        follow_symlinks: Follow symlinks on the real filesystem (default
            ``False``).  Cycles (a link chain leading back to an ancestor
            directory) are detected and logged; non-cyclic links simply
            cause subtrees to be walked again under the link's path.
            Links *inside* archives are never followed; they are yielded
            as symlink entries.

    Yields:
        VirtualPath: entries in a stable order -- real directories are
        listed sorted by name; every archive is yielded as a plain entry
        first, then its members in archive order
        (``a.zip`` before ``a.zip/x.txt``).

    All errors (unreadable directories, corrupt/encrypted archives, missing
    codecs, stat failures, ...) are logged through loguru and never raised
    out of this generator; the affected entries degrade gracefully (e.g. a
    corrupt archive is simply yielded as a plain file).

    Memory note: compound tars (``.tar.gz`` ... ``.tar.7z``) and archives
    nested inside archives are decompressed once per walk and kept in memory
    while any of their paths remain alive, so member access is random-access
    cheap.  Plain ``.zip``/``.whl``/``.tar``/``.7z`` on disk are never
    materialised.
    """
    root_path = Path(root)

    try:
        if not (root_path.is_symlink() or root_path.exists()):
            logger.error("Walk root does not exist: {}", root_path)
            return
    except OSError as exc:
        logger.error("Cannot access walk root {}: {}", root_path, exc)
        return

    root_parts = PurePosixPath(root_path.as_posix()).parts
    # Resolved paths of directories on the current descent -- used to detect
    # symlink cycles (a link pointing back to its own ancestor).
    ancestry: List[Path] = []

    def real_kind(path: Path) -> Tuple[str, bool, Optional[int]]:
        """Classify a real path as ('dir'|'file'|'symlink'|'special', is_link, size)."""
        try:
            st = path.lstat()
        except OSError as exc:
            logger.warning("Cannot stat {}: {}", path, exc)
            return "file", False, None
        is_link = _stat.S_ISLNK(st.st_mode)
        if is_link and follow_symlinks:
            try:
                target = path.stat()  # follows the link
            except OSError as exc:
                logger.warning("Cannot stat symlink target {}: {}", path, exc)
                return "symlink", True, None
            if _stat.S_ISDIR(target.st_mode):
                return "dir", True, None
            if _stat.S_ISREG(target.st_mode):
                return "file", True, target.st_size
            return "special", True, target.st_size
        if is_link:
            return "symlink", True, st.st_size
        if _stat.S_ISDIR(st.st_mode):
            return "dir", False, None
        if _stat.S_ISREG(st.st_mode):
            return "file", False, st.st_size
        return "special", False, st.st_size

    def walk_real_dir(dir_path: Path, base_parts: Tuple[str, ...]) -> Iterator[VirtualPath]:
        try:
            children = sorted(dir_path.iterdir(), key=lambda p: p.name)
        except OSError as exc:
            logger.error("Cannot list directory {}: {}", dir_path, exc)
            return
        for child in children:
            try:
                yield from walk_real_entry(child, base_parts + (child.name,))
            except Exception:
                logger.exception("Unexpected error while visiting {}", child)

    def walk_real_entry(path: Path, parts: Tuple[str, ...]) -> Iterator[VirtualPath]:
        kind, is_link, size = real_kind(path)
        entry = VirtualPath(
            parts,
            origin=root_path,
            real=path,
            depth=0,
            kind=kind,
            is_symlink=is_link,
            size=size,
            opener=_real_file_opener(path) if kind == "file" else None,
        )

        if kind == "dir":
            resolved = None
            if follow_symlinks:
                try:
                    resolved = path.resolve()
                except OSError:  # pragma: no cover - resolve rarely fails
                    pass
                if resolved is not None and resolved in ancestry:
                    logger.warning("Symlink loop detected at {} -- not descending", path)
                    if include_dirs:
                        yield entry
                    return
            if include_dirs:
                yield entry
            if resolved is not None:
                ancestry.append(resolved)
            yield from walk_real_dir(path, parts)
            if resolved is not None:
                ancestry.pop()
            return

        # file / symlink / special -- yield, then maybe dive into an archive
        yield entry
        if kind != "file":
            return
        fmt = detect_format(path.name)
        if fmt is not None and max_depth >= 1:
            yield from walk_archive(_FileSource(path), fmt, parts, path, 1)

    def walk_archive(
        source: Union[_FileSource, _BytesSource],
        fmt: ArchiveFormat,
        arch_parts: Tuple[str, ...],
        arch_real: Path,
        depth: int,
    ) -> Iterator[VirtualPath]:
        label = _vjoin(arch_parts)
        if not _codec_ready(fmt.codec):
            return  # already warned by _codec_module; entry yielded as file
        try:
            reader = _make_reader(source, fmt, arch_parts[-1])
            members = reader.members()
        except Exception as exc:
            logger.error(
                "Cannot open {} archive {} ({}: {}) -- treating it as a plain file",
                fmt.name,
                label,
                type(exc).__name__,
                exc,
            )
            return
        for member in members:
            mparts = arch_parts + tuple(member.name.split("/"))
            entry = VirtualPath(
                mparts,
                origin=root_path,
                real=arch_real,
                depth=depth,
                kind="dir" if member.is_dir else ("symlink" if member.is_symlink else "file"),
                is_symlink=member.is_symlink,
                size=member.size,
                opener=None if (member.is_dir or member.is_symlink) else partial(reader.open_member, member),
            )
            if member.is_dir:
                if include_dirs:
                    yield entry
                continue
            yield entry
            if member.is_symlink:
                continue
            inner_fmt = detect_format(member.name)
            if inner_fmt is not None and depth + 1 <= max_depth:
                nested = _materialize(reader, member, _vjoin(mparts))
                if nested is not None:
                    yield from walk_archive(nested, inner_fmt, mparts, arch_real, depth + 1)

    # -- dispatch ------------------------------------------------------------
    try:
        root_is_link = root_path.is_symlink()
        root_is_dir = root_path.is_dir()  # follows links
    except OSError:  # pragma: no cover
        root_is_link, root_is_dir = False, False

    if root_is_dir and (follow_symlinks or not root_is_link):
        if follow_symlinks:
            try:
                ancestry.append(root_path.resolve())
            except OSError:  # pragma: no cover
                pass
        yield from walk_real_dir(root_path, root_parts)
    else:
        yield from walk_real_entry(root_path, root_parts)


if __name__ == "__main__":  # pragma: no cover -- tiny demo CLI
    import sys

    target = sys.argv[1] if len(sys.argv) > 1 else "."
    for entry in walk(target):
        size = f"{entry.size:>9}" if entry.size is not None else "        ?"
        print(f"{size}  {entry}")
