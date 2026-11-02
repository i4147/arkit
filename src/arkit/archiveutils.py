import bz2
import gzip
import importlib
import io
import lzma
import os
import shutil
import tarfile
import tempfile
import zipfile
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Union

ProcessorFunc = Callable[[bytes, str], bytes]
FilterFunc = Callable[[str], bool]
PathLike = Union[str, os.PathLike]


class ArchiveInPlaceError(Exception):
    pass


class UnsupportedFormatError(ArchiveInPlaceError):
    pass


class ArchiveProcessor:
    COMPRESSION_EXTENSIONS = {
        ".gz": "gzip",
        ".bz2": "bzip2",
        ".xz": "lzma",
        ".zst": "zstandard",
        ".br": "brotli",
        ".bz3": "bzip3",
        ".snappy": "snappy",
        ".lz4": "lz4",
    }
    ARCHIVE_EXTENSIONS = {
        ".zip": "zip",
        ".whl": "zip",
        ".tar": "tar",
        ".7z": "7zip",
    }
    TAR_COMPRESSION = {
        ".tar.gz": "gz",
        ".tgz": "gz",
        ".tar.xz": "xz",
        ".txz": "xz",
        ".tar.zst": "zst",
        ".tar.7z": "7z",
        ".tar.br": "br",
        ".tar.bz2": "bz2",
        ".tbz2": "bz2",
        ".tar.bz3": "bz3",
        ".tar.snappy": "snappy",
        ".tar.lz4": "lz4",
    }

    def __init__(self, archive_path: PathLike) -> None:
        self.archive_path: Path = Path(archive_path)
        if not self.archive_path.exists():
            msg = f"Archive not found: {archive_path}"
            raise FileNotFoundError(msg)
        self.format_type: str = ""
        self.compression: str | None = None
        self._detect_format()

    def _detect_format(self) -> None:
        filename = self.archive_path.name.lower()
        for ext, compression in self.TAR_COMPRESSION.items():
            if filename.endswith(ext):
                self.format_type = "tar"
                self.compression = compression
                return
        for ext, archive_type in self.ARCHIVE_EXTENSIONS.items():
            if filename.endswith(ext):
                self.format_type = archive_type
                self.compression = None
                return
        for ext, compression in self.COMPRESSION_EXTENSIONS.items():
            if filename.endswith(ext):
                self.format_type = "single"
                self.compression = compression
                return
        msg = f"Unsupported archive format: {filename}"
        raise UnsupportedFormatError(msg)

    @contextmanager
    def _open_archive(self, mode: Literal["r", "w"] = "r") -> Any:
        if self.format_type == "zip":
            if mode == "r":
                archive = zipfile.ZipFile(self.archive_path, "r")
            else:
                archive = zipfile.ZipFile(self.archive_path, "w")
        elif self.format_type == "tar":
            if mode == "r":
                if self.compression is None:
                    archive = tarfile.open(self.archive_path, "r")
                elif self.compression == "gz":
                    archive = tarfile.open(self.archive_path, "r:gz")
                elif self.compression == "bz2":
                    archive = tarfile.open(self.archive_path, "r:bz2")
                elif self.compression == "xz":
                    archive = tarfile.open(self.archive_path, "r:xz")
                else:
                    archive = tarfile.open(self.archive_path, "r:*")
            elif self.compression is None:
                archive = tarfile.open(self.archive_path, "w")
            elif self.compression == "gz":
                archive = tarfile.open(self.archive_path, "w:gz")
            elif self.compression == "bz2":
                archive = tarfile.open(self.archive_path, "w:bz2")
            elif self.compression == "xz":
                archive = tarfile.open(self.archive_path, "w:xz")
            else:
                msg = f"Unsupported tar compression for writing: {self.compression}"
                raise UnsupportedFormatError(msg)
        elif self.format_type == "7zip":
            import py7zr

            archive = py7zr.SevenZipFile(self.archive_path, mode)
        else:
            msg = f"Unsupported archive type: {self.format_type}"
            raise UnsupportedFormatError(msg)
        try:
            yield archive
        finally:
            archive.close()

    def _get_compression_handler(self, compression: str) -> Any | None:
        handlers = {
            "gzip": gzip,
            "bzip2": bz2,
            "lzma": lzma,
            "zstandard": importlib.import_module("zstandard"),
            "brotli": importlib.import_module("brotli"),
            "bzip3": importlib.import_module("bz3"),
            "snappy": importlib.import_module("cramjam"),
            "lz4": importlib.import_module("lz4.frame"),
        }
        return handlers.get(compression)

    def _decompress_data(self, data: bytes, compression: str | None) -> bytes:
        if compression is None:
            return data
        handler = self._get_compression_handler(compression)
        if handler is None:
            msg = f"Unsupported compression: {compression}"
            raise UnsupportedFormatError(msg)
        try:
            if compression == "gzip":
                return gzip.decompress(data)
            elif compression == "bzip2":
                return bz2.decompress(data)
            elif compression == "lzma":
                return lzma.decompress(data)
            elif compression == "zstandard":
                return handler.ZstdDecompressor().decompress(data)
            elif compression == "brotli":
                return handler.decompress(data)
            elif compression == "bzip3":
                return handler.decompress(data)
            elif compression == "snappy":
                return handler.decompress(data)
            elif compression == "lz4":
                return handler.decompress(data)
            else:
                msg = f"Unsupported compression: {compression}"
                raise UnsupportedFormatError(msg)
        except Exception as e:
            msg = f"Failed to decompress {compression} data: {e}"
            raise ArchiveInPlaceError(msg) from e

    def _compress_data(self, data: bytes, compression: str | None) -> bytes:
        if compression is None:
            return data
        handler = self._get_compression_handler(compression)
        if handler is None:
            msg = f"Unsupported compression: {compression}"
            raise UnsupportedFormatError(msg)
        try:
            if compression == "gzip":
                return gzip.compress(data)
            elif compression == "bzip2":
                return bz2.compress(data)
            elif compression == "lzma":
                return lzma.compress(data)
            elif compression == "zstandard":
                return handler.ZstdCompressor().compress(data)
            elif compression == "brotli":
                return handler.compress(data)
            elif compression == "bzip3":
                return handler.compress(data)
            elif compression == "snappy":
                return handler.compress(data)
            elif compression == "lz4":
                return handler.compress(data)
            else:
                msg = f"Unsupported compression: {compression}"
                raise UnsupportedFormatError(msg)
        except Exception as e:
            msg = f"Failed to compress {compression} data: {e}"
            raise ArchiveInPlaceError(msg) from e

    def list_files(self) -> list[str]:
        if self.format_type == "single":
            return [self.archive_path.stem]
        with self._open_archive("r") as archive:
            if self.format_type == "zip":
                if isinstance(archive, zipfile.ZipFile):
                    return archive.namelist()
                else:
                    msg = "Expected ZipFile"
                    raise ArchiveInPlaceError(msg)
            elif self.format_type == "tar":
                if isinstance(archive, tarfile.TarFile):
                    return [member.name for member in archive.getmembers() if member.isfile()]
                else:
                    msg = "Expected TarFile"
                    raise ArchiveInPlaceError(msg)
            elif self.format_type == "7zip":
                if hasattr(archive, "getnames"):
                    return archive.getnames()
                else:
                    msg = "Expected SevenZipFile"
                    raise ArchiveInPlaceError(msg)
        return []

    def process_files(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None = None,
        preserve_timestamps: bool = True,
    ) -> int:
        processed_count = 0
        if self.format_type == "single":
            data = self._read_single_file()
            filename = self.archive_path.stem
            if filter_func is None or filter_func(filename):
                processed_data = processor_func(data, filename)
                self._write_single_file(processed_data)
                processed_count = 1
        else:
            temp_dir = tempfile.mkdtemp(prefix="archive_processing_")
            try:
                processed_count = self._process_archive_files(
                    processor_func, filter_func, temp_dir, preserve_timestamps
                )
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
        return processed_count

    def _read_single_file(self) -> bytes:
        with open(self.archive_path, "rb") as f:
            compressed_data = f.read()
        return self._decompress_data(compressed_data, self.compression)

    def _write_single_file(self, data: bytes) -> None:
        compressed_data = self._compress_data(data, self.compression)
        temp_path = self.archive_path.with_suffix(self.archive_path.suffix + ".tmp")
        with open(temp_path, "wb") as f:
            f.write(compressed_data)
        os.replace(temp_path, self.archive_path)

    def _process_archive_files(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None,
        temp_dir: str,
        preserve_timestamps: bool,
    ) -> int:
        processed_count = 0
        if self.format_type == "zip":
            processed_count = self._process_zip(processor_func, filter_func, temp_dir, preserve_timestamps)
        elif self.format_type == "tar":
            processed_count = self._process_tar(processor_func, filter_func, temp_dir, preserve_timestamps)
        elif self.format_type == "7zip":
            processed_count = self._process_7zip(processor_func, filter_func, temp_dir, preserve_timestamps)
        return processed_count

    def _process_zip(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None,
        temp_dir: str,
        preserve_timestamps: bool,
    ) -> int:
        processed_count = 0
        temp_archive_path = os.path.join(temp_dir, "archive.zip")
        with zipfile.ZipFile(self.archive_path, "r") as src_zip:
            with zipfile.ZipFile(temp_archive_path, "w") as dst_zip:
                for item in src_zip.infolist():
                    data = src_zip.read(item.filename)
                    filename = item.filename
                    if filter_func is None or filter_func(filename):
                        try:
                            data = processor_func(data, filename)
                            processed_count += 1
                        except Exception as e:
                            print(f"Warning: Failed to process {filename}: {e}")
                    if preserve_timestamps:
                        dst_zip.writestr(item, data)
                    else:
                        dst_zip.writestr(filename, data)
        os.replace(temp_archive_path, self.archive_path)
        return processed_count

    def _process_tar(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None,
        temp_dir: str,
        preserve_timestamps: bool,
    ) -> int:
        processed_count = 0
        temp_archive_path = os.path.join(temp_dir, "archive.tar")
        write_mode: Literal["w", "w:gz", "w:bz2", "w:xz"] = "w"
        if self.compression == "gz":
            write_mode = "w:gz"
        elif self.compression == "bz2":
            write_mode = "w:bz2"
        elif self.compression == "xz":
            write_mode = "w:xz"
        with tarfile.open(self.archive_path, "r") as src_tar:
            with tarfile.open(temp_archive_path, write_mode) as dst_tar:
                for member in src_tar.getmembers():
                    if member.isfile():
                        extracted = src_tar.extractfile(member)
                        if extracted is None:
                            data = b""
                        else:
                            data = extracted.read()
                        if filter_func is None or filter_func(member.name):
                            try:
                                data = processor_func(data, member.name)
                                processed_count += 1
                            except Exception as e:
                                print(f"Warning: Failed to process {member.name}: {e}")
                        member.size = len(data)
                        if preserve_timestamps:
                            dst_tar.addfile(member, io.BytesIO(data))
                        else:
                            new_member = tarfile.TarInfo(name=member.name)
                            new_member.size = len(data)
                            new_member.mode = member.mode
                            dst_tar.addfile(new_member, io.BytesIO(data))
                    else:
                        dst_tar.addfile(member)
        os.replace(temp_archive_path, self.archive_path)
        return processed_count

    def _process_7zip(
        self,
        processor_func: ProcessorFunc,
        filter_func: FilterFunc | None,
        temp_dir: str,
        preserve_timestamps: bool,
    ) -> int:
        import py7zr

        processed_count = 0
        temp_archive_path = os.path.join(temp_dir, "archive.7z")
        with py7zr.SevenZipFile(self.archive_path, "r") as src_7z:
            all_files = src_7z.getnames()
            src_7z.extractall(path=temp_dir)
            for filename in all_files:
                file_path = os.path.join(temp_dir, filename)
                if os.path.isfile(file_path):
                    with open(file_path, "rb") as f:
                        data = f.read()
                    if filter_func is None or filter_func(filename):
                        try:
                            data = processor_func(data, filename)
                            processed_count += 1
                            with open(file_path, "wb") as f:
                                f.write(data)
                        except Exception as e:
                            print(f"Warning: Failed to process {filename}: {e}")
            with py7zr.SevenZipFile(temp_archive_path, "w") as dst_7z:
                for filename in all_files:
                    file_path = os.path.join(temp_dir, filename)
                    if os.path.isfile(file_path):
                        dst_7z.write(file_path, filename)
        os.replace(temp_archive_path, self.archive_path)
        return processed_count


def process_archive(
    archive_path: PathLike,
    processor_func: ProcessorFunc,
    filter_func: FilterFunc | None = None,
    preserve_timestamps: bool = True,
) -> int:
    processor = ArchiveProcessor(archive_path)
    return processor.process_files(processor_func, filter_func, preserve_timestamps)
