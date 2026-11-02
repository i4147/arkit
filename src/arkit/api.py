from dataclasses import dataclass
import os
from pathlib import Path
import tarfile
import tempfile
import zipfile
from .codecs import CHUNK_SIZE, compress_stream, decompress_stream
from .formats import detect_format
from .safety import ensure_parent, safe_destination


@dataclass(frozen=True, slots=True)
class CompressionStats:
    source_bytes: int
    result_bytes: int
    ratio: float
    space_freed: int
    space_change_percent: float


@dataclass(frozen=True, slots=True)
class HealthReport:
    healthy: bool
    format: str
    errors: tuple[str, ...] = ()


def compression_stats(source: str | Path, result: str | Path) -> CompressionStats:
    source_bytes = Path(source).stat().st_size
    result_bytes = Path(result).stat().st_size
    ratio = result_bytes / source_bytes if source_bytes else 0.0
    space_freed = source_bytes - result_bytes
    space_change_percent = (1.0 - ratio) * 100 if source_bytes else 0.0
    return CompressionStats(
        source_bytes=source_bytes,
        result_bytes=result_bytes,
        ratio=ratio,
        space_freed=space_freed,
        space_change_percent=space_change_percent,
    )


def _iter_files(root: Path):
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def compress(
    source: str | Path,
    output: str | Path,
    *,
    format: str | None = None,
    level: int | None = None,
    threads: int | None = None,
) -> CompressionStats:
    source = Path(source)
    output = Path(output)
    format_name = format or detect_format(output).name
    output.parent.mkdir(parents=True, exist_ok=True)
    if format_name in {"zip", "whl"}:
        if not source.is_dir():
            raise ValueError("ZIP and WHL creation requires a source directory.")
        compression = zipfile.ZIP_DEFLATED
        compresslevel = level if level is not None else 6
        with zipfile.ZipFile(
            output,
            "w",
            compression=compression,
            compresslevel=compresslevel,
            allowZip64=True,
        ) as archive:
            for file_path in _iter_files(source):
                archive.write(file_path, file_path.relative_to(source))
    elif format_name in {"tar", "tar.gz", "tar.bz2", "tar.xz"}:
        mode = {
            "tar": "w",
            "tar.gz": "w:gz",
            "tar.bz2": "w:bz2",
            "tar.xz": "w:xz",
        }[format_name]
        with tarfile.open(output, mode, compresslevel=level) as archive:
            archive.add(source, arcname=source.name, recursive=True)
    elif format_name in {"tar.zst", "tar.br"}:
        codec = "zst" if format_name == "tar.zst" else "br"
        with output.open("wb") as raw:
            with tempfile.TemporaryFile(mode="w+b") as tar_buffer:
                with tarfile.open(fileobj=tar_buffer, mode="w") as archive:
                    archive.add(source, arcname=source.name, recursive=True)
                tar_buffer.seek(0)
                compress_stream(
                    tar_buffer,
                    raw,
                    codec,
                    level=level,
                    threads=threads,
                )
    elif format_name == "7z":
        try:
            import py7zr
        except ImportError as exc:
            raise ImportError("7z support requires: pip install archivekit[sevenzip]") from exc
        with py7zr.SevenZipFile(output, "w") as archive:
            if source.is_dir():
                archive.writeall(source, arcname=source.name)
            else:
                archive.write(source, arcname=source.name)
    elif format_name in {"gz", "bz2", "xz", "br", "zst", "lz4", "snappy"}:
        if not source.is_file():
            raise ValueError(f"{format_name} is a single-file stream format; source must be a file.")
        with source.open("rb") as reader, output.open("wb") as writer:
            compress_stream(
                reader,
                writer,
                format_name,
                level=level,
                threads=threads,
            )
    else:
        raise ValueError(f"Unsupported output format: {format_name}")
    return compression_stats(source, output)


def decompress(
    archive_path: str | Path,
    destination: str | Path,
    *,
    output_name: str | None = None,
) -> Path:
    archive_path = Path(archive_path)
    destination = Path(destination)
    fmt = detect_format(archive_path)
    destination.mkdir(parents=True, exist_ok=True)
    if fmt.kind == "zip":
        with zipfile.ZipFile(archive_path) as archive:
            for info in archive.infolist():
                target = safe_destination(destination, info.filename)
                if info.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                ensure_parent(target)
                with archive.open(info, "r") as reader, target.open("wb") as writer:
                    while chunk := reader.read(CHUNK_SIZE):
                        writer.write(chunk)
        return destination
    if fmt.name in {"tar", "tar.gz", "tar.bz2", "tar.xz"}:
        with tarfile.open(archive_path, "r:*") as archive:
            for member in archive:
                target = safe_destination(destination, member.name)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                if not member.isfile():
                    continue
                ensure_parent(target)
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                with extracted, target.open("wb") as writer:
                    while chunk := extracted.read(CHUNK_SIZE):
                        writer.write(chunk)
        return destination
    if fmt.name in {"tar.zst", "tar.br"}:
        codec = "zst" if fmt.name == "tar.zst" else "br"
        with tempfile.TemporaryFile(mode="w+b") as tar_buffer:
            with archive_path.open("rb") as reader:
                decompress_stream(reader, tar_buffer, codec)
            tar_buffer.seek(0)
            with tarfile.open(fileobj=tar_buffer, mode="r:") as archive:
                for member in archive:
                    target = safe_destination(destination, member.name)
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        continue
                    if not member.isfile():
                        continue
                    ensure_parent(target)
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    with extracted, target.open("wb") as writer:
                        while chunk := extracted.read(CHUNK_SIZE):
                            writer.write(chunk)
        return destination
    if fmt.name == "7z":
        try:
            import py7zr
        except ImportError as exc:
            raise ImportError("7z support requires: pip install archivekit[sevenzip]") from exc
        with py7zr.SevenZipFile(archive_path, "r") as archive:
            archive.extractall(path=destination)
        return destination
    if fmt.kind == "stream":
        if output_name is None:
            suffix = archive_path.suffix
            output_name = archive_path.name.removesuffix(suffix) or "output"
        target = safe_destination(destination, output_name)
        ensure_parent(target)
        with archive_path.open("rb") as reader, target.open("wb") as writer:
            decompress_stream(reader, writer, fmt.name)
        return target
    raise ValueError(f"Unsupported input format: {fmt.name}")


def validate(archive_path: str | Path) -> HealthReport:
    archive_path = Path(archive_path)
    try:
        fmt = detect_format(archive_path)
        if fmt.kind == "zip":
            with zipfile.ZipFile(archive_path) as archive:
                bad_member = archive.testzip()
                if bad_member is not None:
                    return HealthReport(
                        False,
                        fmt.name,
                        (f"CRC failure in ZIP member: {bad_member}",),
                    )
        elif fmt.name in {"tar", "tar.gz", "tar.bz2", "tar.xz"}:
            with tarfile.open(archive_path, "r:*") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    reader = archive.extractfile(member)
                    if reader is None:
                        continue
                    with reader:
                        while reader.read(CHUNK_SIZE):
                            pass
        elif fmt.name in {"tar.zst", "tar.br"}:
            with tempfile.TemporaryFile(mode="w+b") as tar_buffer:
                codec = "zst" if fmt.name == "tar.zst" else "br"
                with archive_path.open("rb") as reader:
                    decompress_stream(reader, tar_buffer, codec)
                tar_buffer.seek(0)
                with tarfile.open(fileobj=tar_buffer, mode="r:") as archive:
                    for member in archive:
                        if member.isfile():
                            reader = archive.extractfile(member)
                            if reader is not None:
                                with reader:
                                    while reader.read(CHUNK_SIZE):
                                        pass
        elif fmt.kind == "stream":
            with archive_path.open("rb") as reader, os.fdopen(os.open(os.devnull, os.O_WRONLY), "wb") as sink:
                decompress_stream(reader, sink, fmt.name)
        elif fmt.name == "7z":
            try:
                import py7zr
            except ImportError as exc:
                raise ImportError("7z support requires: pip install archivekit[sevenzip]") from exc
            with py7zr.SevenZipFile(archive_path, "r") as archive:
                archive.test()
        return HealthReport(True, fmt.name)
    except Exception as exc:
        return HealthReport(False, "unknown", (f"{type(exc).__name__}: {exc}",))


def convert(
    source: str | Path,
    output: str | Path,
    *,
    output_format: str | None = None,
    level: int | None = None,
    threads: int | None = None,
) -> CompressionStats:
    source = Path(source)
    output = Path(output)
    target_format = output_format or detect_format(output).name
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir)
        extracted = workspace / "extracted"
        decompress(source, extracted)
        if target_format in {"gz", "bz2", "xz", "br", "zst", "lz4", "snappy"}:
            files = [item for item in extracted.rglob("*") if item.is_file()]
            if len(files) != 1:
                raise ValueError("Converting to a stream format requires exactly one extracted file.")
            return compress(
                files[0],
                output,
                format=target_format,
                level=level,
                threads=threads,
            )
        entries = list(extracted.iterdir())
        compression_source = entries[0] if len(entries) == 1 else extracted
        return compress(
            compression_source,
            output,
            format=target_format,
            level=level,
            threads=threads,
        )
