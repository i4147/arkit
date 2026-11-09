import codecs
import contextlib
import dataclasses
import json
import os
import secrets
import shutil
from pathlib import Path
from typing import Any

__all__ = ["atomic_write"]


def _json_default(o: Any) -> Any:
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return dataclasses.asdict(o)
    if isinstance(o, (set, frozenset)):
        return list(o)
    if isinstance(o, os.PathLike):
        return os.fspath(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(f"Cannot serialize {type(o).__name__}")


def _serialize(data, *, as_json, encoding, errors, newline, indent, sort_keys, ensure_ascii, trailing_newline) -> bytes:
    raw = (bytes, bytearray, memoryview)
    if as_json is None:
        as_json = not isinstance(data, raw + (str,))
    if as_json:
        text = json.dumps(data, indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii, default=_json_default)
        if trailing_newline:
            text += "\n"
    elif isinstance(data, raw):
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
    if os.name != "posix":
        return
    with contextlib.suppress(OSError):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _replace_atomically(target: Path, payload: bytes, exists: bool, mode, mode_from, fsync: bool) -> None:
    tmp = target.with_name(f".{target.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    fd = os.open(tmp, flags, 0o666)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            if fsync:
                with contextlib.suppress(OSError):
                    os.fsync(f.fileno())
        with contextlib.suppress(OSError, NotImplementedError):
            if mode is not None:
                os.chmod(tmp, mode)
            elif mode_from is not None and Path(mode_from).exists():
                shutil.copymode(mode_from, tmp)
            elif exists:
                shutil.copymode(target, tmp)
        os.replace(tmp, target)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
    if fsync:
        _fsync_dir(target.parent)


def atomic_write(
    path,
    data,
    opt=None,
    *,
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: str | None = None,
    mode: int | None = None,
    mode_from=None,
    backup: bool | str | os.PathLike = False,
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
    lock=None,
    suppress_errors: bool = False,
    fallback: bool = True,
) -> bool:
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
        with lock if lock is not None else contextlib.nullcontext():
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
