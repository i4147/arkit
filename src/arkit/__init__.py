from .api import (
    CompressionStats,
    HealthReport,
    compress,
    compression_stats,
    convert,
    decompress,
    validate,
)

__all__ = [
    "CompressionStats",
    "HealthReport",
    "compress",
    "compression_stats",
    "convert",
    "decompress",
    "validate",
]
from .tar_archive import process_tar_archive
from .zip_archive import process_zip_archive


def process_archive(src_path: str, process_func, dst_path: str = None):
    ext = _detect_format(src_path)
    if ext in ("tar", "tar.gz", "tar.xz", "tar.bz2", "tar.lz4", "tar.zst", "tar.br"):
        return process_tar_archive(src_path, process_func, dst_path)
    elif ext in ("zip", "whl"):
        return process_zip_archive(src_path, process_func, dst_path)
    else:
        raise ValueError("process_archive only supports tar.*, zip, and whl")
