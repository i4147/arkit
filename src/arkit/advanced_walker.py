#!/usr/bin/env python3
"""
Advanced two-way filesystem + archive walker.

Yields VirtualFile objects for:
  - regular files on disk
  - members inside ZIP / TAR archives (including .tar.gz, .tgz, .tar.bz2, …)
  - nested archives (archive inside another archive)
  - standalone compressed files (.gz, .bz2, .xz)

VirtualFile supports:
  read_bytes()  → original content
  write_bytes() → record a modification

On walker exit / flush() all pending modifications are written back,
rebuilding archives from the deepest nesting level outward.
"""

import bz2
import gzip
import io
import lzma
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Callable, Iterator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_SKIP_DIRS: set[str] = {
    ".git",
    ".svn",
    ".hg",
    "__pycache__",
    "node_modules",
    ".tox",
    "venv",
    ".venv",
    ".mypy_cache",
    ".pytest_cache",
    ".cache",
    "dist",
    "build",
    ".eggs",
    ".idea",
    ".vscode",
}

ARCHIVE_SUFFIXES: tuple[str, ...] = (
    ".zip",
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".gz",
    ".bz2",
    ".xz",
)


def _is_archive_name(name: str) -> bool:
    lower = name.lower()
    return any(lower.endswith(suf) for suf in ARCHIVE_SUFFIXES)


# ---------------------------------------------------------------------------
# VirtualFile – the object yielded to the caller
# ---------------------------------------------------------------------------
class VirtualFile:
    """Unified handle for a real file or an archive member."""

    def __init__(
        self,
        path: str,
        *,
        read_func: Callable[[], bytes],
        write_func: Callable[[bytes], None] | None = None,
    ) -> None:
        self.path = path
        self._read_func = read_func
        self._write_func = write_func
        self._cache: bytes | None = None

    def read_bytes(self) -> bytes:
        if self._cache is None:
            self._cache = self._read_func()
        return self._cache

    def write_bytes(self, data: bytes) -> None:
        self._cache = data
        if self._write_func is None:
            raise RuntimeError(f"Read-only entry: {self.path}")
        self._write_func(data)

    def __repr__(self) -> str:
        return f"VirtualFile({self.path!r})"


# ---------------------------------------------------------------------------
# Archive handlers (internal)
# ---------------------------------------------------------------------------
class _BaseHandler:
    def __init__(
        self,
        depth: int,
        parent: "_BaseHandler | None" = None,
        parent_member: str | None = None,
    ) -> None:
        self.depth = depth
        self.parent = parent
        self.parent_member = parent_member
        self.updates: dict[str, bytes] = {}
        self.dirty = False

    def update(self, member: str, data: bytes) -> None:
        self.updates[member] = data
        self.dirty = True

    def serialize(self) -> bytes:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover
        pass


class ZipHandler(_BaseHandler):
    def __init__(
        self,
        source: Path | io.BytesIO,
        depth: int,
        parent: _BaseHandler | None = None,
        parent_member: str | None = None,
    ) -> None:
        super().__init__(depth, parent, parent_member)
        self._src = source
        self._zf = zipfile.ZipFile(source, "r")
        self.is_top_level = isinstance(source, Path)
        self.top_path: Path | None = source if self.is_top_level else None

    def reader_for(self, name: str) -> Callable[[], bytes]:
        def _read() -> bytes:
            return self._zf.read(name)

        return _read

    def serialize(self) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as out:
            for info in self._zf.infolist():
                if info.is_dir():
                    continue
                name = info.filename
                data = self.updates.get(name, self._zf.read(name))
                new_info = zipfile.ZipInfo(filename=name, date_time=info.date_time)
                new_info.compress_type = info.compress_type
                new_info.external_attr = info.external_attr
                out.writestr(new_info, data)
        return buf.getvalue()

    def close(self) -> None:
        self._zf.close()


class TarHandler(_BaseHandler):
    def __init__(
        self,
        source: Path | io.BytesIO,
        depth: int,
        parent: _BaseHandler | None = None,
        parent_member: str | None = None,
        mode_hint: str = "r:*",
    ) -> None:
        super().__init__(depth, parent, parent_member)
        self._src = source
        self._tf = tarfile.open(
            fileobj=source if not isinstance(source, Path) else None,
            name=str(source) if isinstance(source, Path) else None,
            mode=mode_hint,
        )
        self.is_top_level = isinstance(source, Path)
        self.top_path: Path | None = source if self.is_top_level else None
        # Remember compression for rewrite
        self._write_mode = (
            "w:gz" if "gz" in mode_hint else "w:bz2" if "bz2" in mode_hint else "w:xz" if "xz" in mode_hint else "w"
        )

    def reader_for(self, name: str) -> Callable[[], bytes]:
        def _read() -> bytes:
            f = self._tf.extractfile(name)
            if f is None:
                return b""
            return f.read()

        return _read

    def serialize(self) -> bytes:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode=self._write_mode) as out:
            for member in self._tf.getmembers():
                if not member.isfile():
                    continue
                name = member.name
                data = self.updates.get(name)
                if data is None:
                    extracted = self._tf.extractfile(member)
                    data = extracted.read() if extracted else b""
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                info.mtime = member.mtime
                info.mode = member.mode
                info.uid = member.uid
                info.gid = member.gid
                out.addfile(info, io.BytesIO(data))
        return buf.getvalue()

    def close(self) -> None:
        self._tf.close()


class SingleFileHandler(_BaseHandler):
    """Handles standalone .gz / .bz2 / .xz (one logical member)."""

    def __init__(
        self,
        source: Path | io.BytesIO,
        depth: int,
        fmt: str,  # "gz" | "bz2" | "xz"
        parent: _BaseHandler | None = None,
        parent_member: str | None = None,
    ) -> None:
        super().__init__(depth, parent, parent_member)
        self._src = source
        self.fmt = fmt
        self.is_top_level = isinstance(source, Path)
        self.top_path: Path | None = source if self.is_top_level else None
        self._member_name = "content"  # logical name

        if isinstance(source, Path):
            raw = source.read_bytes()
        else:
            raw = source.getvalue()

        if fmt == "gz":
            self._original = gzip.decompress(raw)
        elif fmt == "bz2":
            self._original = bz2.decompress(raw)
        else:
            self._original = lzma.decompress(raw)

    def reader_for(self, _name: str = "content") -> Callable[[], bytes]:
        return lambda: self._original

    def update(self, member: str, data: bytes) -> None:
        self.updates[self._member_name] = data
        self.dirty = True

    def serialize(self) -> bytes:
        data = self.updates.get(self._member_name, self._original)
        if self.fmt == "gz":
            return gzip.compress(data)
        if self.fmt == "bz2":
            return bz2.compress(data)
        return lzma.compress(data)

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# The public walker
# ---------------------------------------------------------------------------
class AdvancedWalker:
    """
    Context-manager / iterator that yields VirtualFile objects.

    Usage
    -----
    with AdvancedWalker(".") as walker:
        for entry in walker:
            data = entry.read_bytes()
            new = transform(data)
            if new != data:
                entry.write_bytes(new)
    # all archive rewrites happen automatically on exit
    """

    def __init__(
        self,
        root: str | Path = ".",
        *,
        skip_dirs: set[str] | None = None,
        max_archive_depth: int = 4,
        expand_archives: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        self.skip_dirs = skip_dirs if skip_dirs is not None else DEFAULT_SKIP_DIRS
        self.max_archive_depth = max_archive_depth
        self.expand_archives = expand_archives
        self._handlers: list[_BaseHandler] = []

    def __enter__(self) -> "AdvancedWalker":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.flush()
        finally:
            for h in self._handlers:
                h.close()

    def __iter__(self) -> Iterator[VirtualFile]:
        yield from self._walk_fs(self.root)

    # ------------------------------------------------------------------
    def flush(self) -> None:
        """Rewrite every dirty archive (deepest first)."""
        dirty = [h for h in self._handlers if h.dirty]
        dirty.sort(key=lambda h: -h.depth)  # deepest first

        for h in dirty:
            new_bytes = h.serialize()
            if h.parent is not None and h.parent_member is not None:
                h.parent.update(h.parent_member, new_bytes)
                h.parent.dirty = True
            elif h.is_top_level and h.top_path is not None:
                h.top_path.write_bytes(new_bytes)

    # ------------------------------------------------------------------
    def _walk_fs(self, directory: Path) -> Iterator[VirtualFile]:
        for dirpath, dirnames, filenames in os.walk(directory, followlinks=False):
            # prune
            dirnames[:] = [d for d in dirnames if d not in self.skip_dirs and not (Path(dirpath) / d).is_symlink()]

            for name in filenames:
                path = Path(dirpath) / name
                if path.is_symlink() or not path.is_file():
                    continue

                rel = path.relative_to(self.root).as_posix()

                if self.expand_archives and _is_archive_name(name):
                    yield from self._expand_archive(path, rel, depth=0)
                else:
                    yield VirtualFile(
                        path=rel,
                        read_func=path.read_bytes,
                        write_func=path.write_bytes,
                    )

    def _expand_archive(
        self,
        source: Path | io.BytesIO,
        virtual_prefix: str,
        depth: int,
        parent: _BaseHandler | None = None,
        parent_member: str | None = None,
    ) -> Iterator[VirtualFile]:
        if depth > self.max_archive_depth:
            return

        name_for_type = virtual_prefix.split("!")[-1] if "!" in virtual_prefix else virtual_prefix
        lower = name_for_type.lower()

        handler: _BaseHandler

        try:
            if lower.endswith(".zip"):
                handler = ZipHandler(source, depth, parent, parent_member)
            elif any(
                lower.endswith(s) for s in (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz", ".tbz2", ".tar.xz", ".txz")
            ):
                mode = "r:*"
                handler = TarHandler(source, depth, parent, parent_member, mode)
            elif lower.endswith(".gz") and not lower.endswith((".tar.gz", ".tgz")):
                handler = SingleFileHandler(source, depth, "gz", parent, parent_member)
            elif lower.endswith(".bz2") and not lower.endswith((".tar.bz2", ".tbz", ".tbz2")):
                handler = SingleFileHandler(source, depth, "bz2", parent, parent_member)
            elif lower.endswith(".xz") and not lower.endswith((".tar.xz", ".txz")):
                handler = SingleFileHandler(source, depth, "xz", parent, parent_member)
            else:
                return
        except Exception:
            # unreadable / corrupt archive → skip silently
            return

        self._handlers.append(handler)

        # ---- single-file compressors ---------------------------------
        if isinstance(handler, SingleFileHandler):
            member_path = virtual_prefix  # the .gz itself is the “file”
            yield VirtualFile(
                path=member_path,
                read_func=handler.reader_for(),
                write_func=lambda d: handler.update("content", d),
            )
            return

        # ---- multi-member archives (zip / tar) -----------------------
        members: list[str]
        if isinstance(handler, ZipHandler):
            members = [i.filename for i in handler._zf.infolist() if not i.is_dir()]
        else:  # TarHandler
            members = [m.name for m in handler._tf.getmembers() if m.isfile()]

        for member_name in members:
            vpath = f"{virtual_prefix}!{member_name}"

            if _is_archive_name(member_name) and depth < self.max_archive_depth:
                # nested archive → extract bytes and recurse
                try:
                    if isinstance(handler, ZipHandler):
                        nested_bytes = handler._zf.read(member_name)
                    else:
                        f = handler._tf.extractfile(member_name)
                        nested_bytes = f.read() if f else b""
                    nested_src = io.BytesIO(nested_bytes)
                    yield from self._expand_archive(
                        nested_src,
                        vpath,
                        depth + 1,
                        parent=handler,
                        parent_member=member_name,
                    )
                except Exception:
                    continue
            else:
                yield VirtualFile(
                    path=vpath,
                    read_func=handler.reader_for(member_name),
                    write_func=lambda d, n=member_name: handler.update(n, d),
                )
