"""Crash-safe atomic file writing utilities (both buffered and streaming).

Provides:
- :func:`atomic_open`: Streaming context manager writing to a unique sibling
  temporary file and atomically replacing the destination on clean exit.
- :func:`atomic_write`: High-level one-shot atomic writer supporting raw bytes,
  strings, and JSON-serializable structures (dataclasses, sets, paths, dates)
  with backup, permission preservation, lock context, and ``fsync`` durability.
"""

from __future__ import annotations

import codecs
import contextlib
import dataclasses
import json
import os
import secrets
import shutil
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import IO, Any, Literal, overload

from .models import StrPath

__all__ = [
    "atomic_open",
    "atomic_replace",
    "atomic_save",
    "atomic_write",
    "atomic_write_file",
    "atomic_write_json",
    "atomic_write_text",
    "write_atomic",
]


def _json_default(obj: Any) -> Any:
    """Default JSON serializer for dataclasses, sets, PathLike, and date/time objects."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if isinstance(obj, os.PathLike):
        return os.fspath(obj)
    isoformat = getattr(obj, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    raise TypeError(f"Cannot serialize {type(obj).__name__}")


def _serialize(
    data: Any,
    *,
    as_json: bool | None,
    encoding: str,
    errors: str,
    newline: str | None,
    indent: int | None,
    sort_keys: bool,
    ensure_ascii: bool,
    trailing_newline: bool,
) -> bytes:
    """Serialize bytes, text, or arbitrary JSON-able objects into raw bytes."""
    raw_types = (bytes, bytearray, memoryview)
    if as_json is None:
        as_json = not isinstance(data, (*raw_types, str))
    if as_json:
        text = json.dumps(
            data,
            indent=indent,
            sort_keys=sort_keys,
            ensure_ascii=ensure_ascii,
            default=_json_default,
        )
        if trailing_newline:
            text += "\n"
    elif isinstance(data, raw_types):
        return bytes(data)
    elif isinstance(data, str):
        text = data
    else:
        raise TypeError(f"Cannot write {type(data).__name__} as text; use as_json=True")

    if newline is not None:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        if newline != "\n":
            text = text.replace("\n", newline)
    return text.encode(encoding, errors)


def _is_encoding(name: str) -> bool:
    try:
        codecs.lookup(name)
        return True
    except LookupError:
        return False


def _fsync_dir(directory: Path) -> None:
    """Best-effort POSIX directory fsync to make rename metadata durable."""
    if os.name != "posix":
        return
    with contextlib.suppress(OSError):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@overload
def atomic_open(
    path: StrPath,
    mode: Literal["wb"] = "wb",
    *,
    encoding: None = None,
    chmod_mode: int | None = None,
    mode_from: StrPath | None = None,
    mkdir: bool = True,
    fsync: bool = False,
    follow_symlinks: bool = True,
) -> AbstractContextManager[IO[bytes]]: ...


@overload
def atomic_open(
    path: StrPath,
    mode: Literal["w"],
    *,
    encoding: str | None = None,
    chmod_mode: int | None = None,
    mode_from: StrPath | None = None,
    mkdir: bool = True,
    fsync: bool = False,
    follow_symlinks: bool = True,
) -> AbstractContextManager[IO[str]]: ...


@overload
def atomic_open(
    path: StrPath,
    mode: str = "wb",
    *,
    encoding: str | None = None,
    chmod_mode: int | None = None,
    mode_from: StrPath | None = None,
    mkdir: bool = True,
    fsync: bool = False,
    follow_symlinks: bool = True,
) -> AbstractContextManager[IO[Any]]: ...


@contextmanager
def atomic_open(
    path: StrPath,
    mode: str = "wb",
    *,
    encoding: str | None = None,
    chmod_mode: int | None = None,
    mode_from: StrPath | None = None,
    mkdir: bool = True,
    fsync: bool = False,
    follow_symlinks: bool = True,
) -> Iterator[IO[Any]]:
    """Open a sibling temporary file for streaming writes and atomically commit on exit.

    Unlike buffering an entire archive in RAM before writing, :func:`atomic_open`
    lets callers stream arbitrarily large archives with constant memory while
    guaranteeing that readers never observe partial or truncated files. If an
    exception occurs inside the ``with`` block, the temporary file is unlinked
    and the original target remains untouched.
    """
    if "r" in mode or "a" in mode or "+" in mode:
        raise ValueError(f"atomic_open only supports write modes ('w' or 'wb'), got {mode!r}")

    target = Path(os.path.realpath(path)) if follow_symlinks else Path(path)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)

    exists = target.is_file()
    tmp = target.with_name(f".{target.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    fd = os.open(tmp, flags, 0o666)
    committed = False
    try:
        open_kwargs: dict[str, Any] = {}
        if "b" not in mode:
            open_kwargs["encoding"] = encoding or "utf-8"
        with os.fdopen(fd, mode, **open_kwargs) as handle:
            yield handle
            handle.flush()
            if fsync:
                with contextlib.suppress(OSError):
                    os.fsync(handle.fileno())

        with contextlib.suppress(OSError, NotImplementedError):
            if chmod_mode is not None:
                os.chmod(tmp, chmod_mode)
            elif mode_from is not None and Path(mode_from).exists():
                shutil.copymode(mode_from, tmp)
            elif exists:
                shutil.copymode(target, tmp)

        os.replace(tmp, target)
        committed = True
        if fsync:
            _fsync_dir(target.parent)
    finally:
        if not committed:
            with contextlib.suppress(OSError):
                os.unlink(tmp)


def _replace_atomically(
    target: Path,
    payload: bytes,
    exists: bool,
    mode: int | None,
    mode_from: StrPath | None,
    fsync: bool,
) -> None:
    with atomic_open(
        target,
        "wb",
        chmod_mode=mode,
        mode_from=mode_from,
        mkdir=False,
        fsync=fsync,
        follow_symlinks=False,
    ) as handle:
        # Preserve legacy behavior: if `exists` was False and neither mode nor mode_from was given,
        # umask-adjusted permissions from 0o666 remain intact.
        _ = exists
        handle.write(payload)


def atomic_write(
    path: StrPath,
    data: Any,
    opt: bool | int | str | os.PathLike[str] | AbstractContextManager[Any] | None = None,
    *,
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: str | None = None,
    mode: int | None = None,
    mode_from: StrPath | None = None,
    backup: bool | StrPath = False,
    backup_suffix: str = ".bak",
    as_json: bool | None = None,
    indent: int | None = 2,
    sort_keys: bool = False,
    ensure_ascii: bool = False,
    trailing_newline: bool = True,
    mkdir: bool = True,
    fsync: bool = True,
    follow_symlinks: bool = True,
    skip_unchanged: bool = False,
    lock: AbstractContextManager[Any] | None = None,
    suppress_errors: bool = False,
    fallback: bool = True,
) -> bool:
    """Atomically write bytes, text, or JSON-serializable objects to ``path``.

    Supports polymorphic ``opt`` shorthand (``bool`` -> ``backup``, ``int`` -> ``mode``,
    valid encoding ``str`` -> ``encoding``, path ``str``/``PathLike`` -> ``mode_from``,
    context manager -> ``lock``) for full compatibility with legacy ``atomicio``.
    """
    if opt is not None:
        if isinstance(opt, bool):
            backup = opt
        elif isinstance(opt, int):
            mode = opt
        elif isinstance(opt, str):
            if _is_encoding(opt):
                encoding = opt
            else:
                mode_from = opt
        elif isinstance(opt, os.PathLike):
            mode_from = opt
        elif hasattr(opt, "__enter__") and hasattr(opt, "__exit__"):
            lock = opt

    target = Path(os.path.realpath(path)) if follow_symlinks else Path(path)
    payload = _serialize(
        data,
        as_json=as_json,
        encoding=encoding,
        errors=errors,
        newline=newline,
        indent=indent,
        sort_keys=sort_keys,
        ensure_ascii=ensure_ascii,
        trailing_newline=trailing_newline,
    )
    try:
        ctx: AbstractContextManager[Any] = lock if lock is not None else contextlib.nullcontext()
        with ctx:
            exists = target.is_file()
            if skip_unchanged and exists:
                with contextlib.suppress(OSError):
                    if target.read_bytes() == payload:
                        return True
            if mkdir:
                target.parent.mkdir(parents=True, exist_ok=True)
            if backup and exists:
                bak = target.with_name(target.name + backup_suffix) if backup is True else Path(backup)
                try:
                    shutil.copy2(target, bak)
                except OSError:
                    shutil.copyfile(target, bak)
            try:
                _replace_atomically(target, payload, exists, mode, mode_from, fsync)
            except OSError:
                if not fallback:
                    raise
                target.write_bytes(payload)
                if mode is not None:
                    with contextlib.suppress(OSError, NotImplementedError):
                        os.chmod(target, mode)
            return True
    except OSError:
        if suppress_errors:
            return False
        raise


write_atomic = atomic_write
atomic_save = atomic_write
atomic_replace = atomic_write
atomic_write_text = atomic_write
atomic_write_json = atomic_write
atomic_write_file = atomic_write
_atomic_write = atomic_write
_atomic_save = atomic_write
