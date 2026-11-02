import bz2
import gzip
import lzma
from pathlib import Path
import tarfile
import zipfile

try:
    import zstandard as zstd
except ImportError:
    zstd = None
try:
    import brotli
except ImportError:
    brotli = None
try:
    import py7zr
except ImportError:
    py7zr = None
try:
    import lz4.frame
except ImportError:
    lz4 = None
try:
    import cramjam
except ImportError:
    cramjam = None


class Archive:
    CHUNK_SIZE = 2**20
    SUPPORTED_FORMATS = {
        ".zip",
        ".whl",
        ".tar",
        ".tar.gz",
        ".tar.bz2",
        ".tar.xz",
        ".tar.zst",
        ".tar.br",
        ".tar.7z",
        ".tar.lz4",
        ".tar.snappy",
        ".gz",
        ".bz2",
        ".xz",
        ".zst",
        ".br",
        ".7z",
        ".lz4",
        ".snappy",
        ".x",
    }

    def __init__(self, compression_level: int = 6, threads: int = 1):
        self.compression_level = max(1, min(compression_level, 22))
        self.threads = max(1, threads)

    def _get_format(self, path: Path) -> str:
        suffix = str(path).lower()
        for fmt in [".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst", ".tar.br", ".tar.7z", ".tar.lz4", ".tar.snappy"]:
            if suffix.endswith(fmt):
                return fmt
        return path.suffix.lower()

    def decompress(self, archive_path: Path, output_dir: Path, validate: bool = False) -> None:
        archive_path = Path(archive_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        fmt = self._get_format(archive_path)
        if fmt == ".zip" or fmt == ".whl":
            self._decompress_zip(archive_path, output_dir)
        elif fmt.startswith(".tar"):
            self._decompress_tar(archive_path, output_dir, fmt)
        elif fmt in {".gz", ".bz2", ".xz", ".zst", ".br", ".7z", ".lz4", ".snappy", ".x"}:
            self._decompress_single(archive_path, output_dir, fmt)
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        if validate:
            from archive_pkg.validate import validate_archive

            validate_archive(archive_path)

    def compress(self, source_path: Path, archive_path: Path, format_type: str = ".tar.gz") -> None:
        source_path = Path(source_path)
        archive_path = Path(archive_path)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        if format_type == ".zip" or format_type == ".whl":
            self._compress_zip(source_path, archive_path)
        elif format_type.startswith(".tar"):
            self._compress_tar(source_path, archive_path, format_type)
        elif format_type in {".gz", ".bz2", ".xz", ".zst", ".br", ".7z", ".lz4", ".snappy"}:
            if source_path.is_dir():
                raise ValueError(f"Format {format_type} requires file input, not directory")
            self._compress_single(source_path, archive_path, format_type)
        else:
            raise ValueError(f"Unsupported format: {format_type}")

    def _decompress_zip(self, archive_path: Path, output_dir: Path) -> None:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    (output_dir / member.filename).mkdir(parents=True, exist_ok=True)
                else:
                    target = output_dir / member.filename
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(member) as source, open(target, "wb") as dest:
                        while chunk := source.read(self.CHUNK_SIZE):
                            dest.write(chunk)

    def _compress_zip(self, source_path: Path, archive_path: Path) -> None:
        def add_files(zf: zipfile.ZipFile, base_path: Path, prefix: str = "") -> None:
            for item in sorted(base_path.iterdir()):
                arcname = f"{prefix}{item.name}"
                if item.is_dir():
                    add_files(zf, item, f"{arcname}/")
                else:
                    zf.write(item, arcname, compress_type=zipfile.ZIP_DEFLATED)

        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
            if source_path.is_file():
                zf.write(source_path, source_path.name)
            else:
                add_files(zf, source_path)

    def _decompress_tar(self, archive_path: Path, output_dir: Path, fmt: str) -> None:
        mode_map = {
            ".tar": "r",
            ".tar.gz": "r:gz",
            ".tar.bz2": "r:bz2",
            ".tar.xz": "r:xz",
            ".tar.zst": "r|",
            ".tar.br": "r|",
            ".tar.7z": "r|",
            ".tar.lz4": "r|",
            ".tar.snappy": "r|",
        }
        mode = mode_map.get(fmt, "r")
        if fmt in {".tar.zst", ".tar.br", ".tar.7z", ".tar.lz4", ".tar.snappy"}:
            stream = self._decompress_stream(archive_path, fmt)
            with tarfile.open(fileobj=stream, mode="r|") as tf:
                tf.extractall(output_dir)
        else:
            with tarfile.open(archive_path, mode) as tf:
                tf.extractall(output_dir)

    def _compress_tar(self, source_path: Path, archive_path: Path, fmt: str) -> None:
        if fmt == ".tar":
            with tarfile.open(archive_path, "w") as tf:
                self._add_to_tar(tf, source_path)
        elif fmt == ".tar.gz":
            with tarfile.open(archive_path, "w:gz") as tf:
                self._add_to_tar(tf, source_path)
        elif fmt == ".tar.bz2":
            with tarfile.open(archive_path, "w:bz2") as tf:
                self._add_to_tar(tf, source_path)
        elif fmt == ".tar.xz":
            with tarfile.open(archive_path, "w:xz") as tf:
                self._add_to_tar(tf, source_path)
        elif fmt == ".tar.zst":
            with self._compress_stream_zst(archive_path) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tf:
                    self._add_to_tar(tf, source_path)
        elif fmt == ".tar.br":
            with self._compress_stream_br(archive_path) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tf:
                    self._add_to_tar(tf, source_path)
        elif fmt == ".tar.7z":
            if source_path.is_file():
                with py7zr.SevenZipFile(archive_path, "w") as szf:
                    szf.write(source_path, source_path.name)
            else:
                with py7zr.SevenZipFile(archive_path, "w") as szf:
                    for item in source_path.rglob("*"):
                        if item.is_file():
                            szf.write(item, item.relative_to(source_path.parent))
        elif fmt == ".tar.lz4":
            with self._compress_stream_lz4(archive_path) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tf:
                    self._add_to_tar(tf, source_path)
        elif fmt == ".tar.snappy":
            with self._compress_stream_snappy(archive_path) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tf:
                    self._add_to_tar(tf, source_path)

    def _add_to_tar(self, tar: tarfile.TarFile, path: Path) -> None:
        if path.is_file():
            tar.add(path, arcname=path.name)
        else:
            for item in sorted(path.rglob("*")):
                tar.add(item, arcname=item.relative_to(path.parent))

    def _decompress_single(self, archive_path: Path, output_dir: Path, fmt: str) -> None:
        output_file = output_dir / archive_path.stem
        if fmt == ".gz":
            self._decompress_gzip(archive_path, output_file)
        elif fmt == ".bz2":
            self._decompress_bz2(archive_path, output_file)
        elif fmt == ".xz" or fmt == ".x":
            self._decompress_xz(archive_path, output_file)
        elif fmt == ".zst":
            self._decompress_zst(archive_path, output_file)
        elif fmt == ".br":
            self._decompress_br(archive_path, output_file)
        elif fmt == ".7z":
            self._decompress_7z(archive_path, output_dir)
        elif fmt == ".lz4":
            self._decompress_lz4(archive_path, output_file)
        elif fmt == ".snappy":
            self._decompress_snappy(archive_path, output_file)

    def _compress_single(self, source_path: Path, archive_path: Path, fmt: str) -> None:
        if fmt == ".gz":
            self._compress_gzip(source_path, archive_path)
        elif fmt == ".bz2":
            self._compress_bz2(source_path, archive_path)
        elif fmt == ".xz":
            self._compress_xz(source_path, archive_path)
        elif fmt == ".zst":
            self._compress_zst(source_path, archive_path)
        elif fmt == ".br":
            self._compress_br(source_path, archive_path)
        elif fmt == ".7z":
            self._compress_7z(source_path, archive_path)
        elif fmt == ".lz4":
            self._compress_lz4(source_path, archive_path)
        elif fmt == ".snappy":
            self._compress_snappy(source_path, archive_path)

    def _decompress_gzip(self, archive_path: Path, output_file: Path) -> None:
        with gzip.open(archive_path, "rb") as src:
            with open(output_file, "wb") as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _compress_gzip(self, source_path: Path, archive_path: Path) -> None:
        with open(source_path, "rb") as src:
            with gzip.open(archive_path, "wb", compresslevel=self.compression_level) as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _decompress_bz2(self, archive_path: Path, output_file: Path) -> None:
        with bz2.open(archive_path, "rb") as src:
            with open(output_file, "wb") as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _compress_bz2(self, source_path: Path, archive_path: Path) -> None:
        with open(source_path, "rb") as src:
            with bz2.open(archive_path, "wb", compresslevel=self.compression_level) as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _decompress_xz(self, archive_path: Path, output_file: Path) -> None:
        with lzma.open(archive_path, "rb") as src:
            with open(output_file, "wb") as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _compress_xz(self, source_path: Path, archive_path: Path) -> None:
        with open(source_path, "rb") as src:
            with lzma.open(archive_path, "wb", preset=self.compression_level) as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(chunk)

    def _decompress_zst(self, archive_path: Path, output_file: Path) -> None:
        if not zstd:
            raise ImportError("zstandard library required")
        dctx = zstd.ZstdDecompressor()
        with open(archive_path, "rb") as src:
            with open(output_file, "wb") as dest:
                while chunk := src.read(self.CHUNK_SIZE):
                    dest.write(dctx.decompress(chunk))

    def _compress_zst(self, source_path: Path, archive_path: Path) -> None:
        if not zstd:
            raise ImportError("zstandard library required")
        cctx = zstd.ZstdCompressor(level=self.compression_level, threads=self.threads)
        with open(source_path, "rb") as src:
            with open(archive_path, "wb") as dest:
                with cctx.stream_writer(dest) as writer:
                    while chunk := src.read(self.CHUNK_SIZE):
                        writer.write(chunk)

    def _compress_stream_zst(self, archive_path: Path):
        if not zstd:
            raise ImportError("zstandard library required")
        cctx = zstd.ZstdCompressor(level=self.compression_level, threads=self.threads)
        return cctx.stream_writer(open(archive_path, "wb"))

    def _decompress_br(self, archive_path: Path, output_file: Path) -> None:
        if not brotli:
            raise ImportError("brotli library required")
        with open(archive_path, "rb") as src:
            with open(output_file, "wb") as dest:
                data = src.read()
                dest.write(brotli.decompress(data))

    def _compress_br(self, source_path: Path, archive_path: Path) -> None:
        if not brotli:
            raise ImportError("brotli library required")
        with open(source_path, "rb") as src:
            with open(archive_path, "wb") as dest:
                data = src.read()
                dest.write(brotli.compress(data, quality=self.compression_level))

    def _compress_stream_br(self, archive_path: Path):
        if not brotli:
            raise ImportError("brotli library required")
        file_obj = open(archive_path, "wb")
        return _BrotliStreamWrapper
