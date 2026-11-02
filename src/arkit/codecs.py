import bz2
import gzip
import lzma
from typing import IO

BinaryIO = IO[bytes]
CHUNK_SIZE = 1024 * 1024


def copy_stream(source: BinaryIO, destination: BinaryIO, chunk_size: int = CHUNK_SIZE) -> int:
    total = 0
    while chunk := source.read(chunk_size):
        destination.write(chunk)
        total += len(chunk)
    return total


def compress_stream(
    source: BinaryIO,
    destination: BinaryIO,
    codec: str,
    *,
    level: int | None = None,
    threads: int | None = None,
    chunk_size: int = CHUNK_SIZE,
) -> int:
    match codec:
        case "gz":
            with gzip.GzipFile(
                fileobj=destination,
                mode="wb",
                compresslevel=level if level is not None else 9,
            ) as writer:
                return copy_stream(source, writer, chunk_size)
        case "bz2":
            compressor = bz2.BZ2Compressor(level if level is not None else 9)
            total = 0
            while chunk := source.read(chunk_size):
                destination.write(compressor.compress(chunk))
                total += len(chunk)
            destination.write(compressor.flush())
            return total
        case "xz":
            compressor = lzma.LZMACompressor(
                format=lzma.FORMAT_XZ,
                preset=level if level is not None else 6,
            )
            total = 0
            while chunk := source.read(chunk_size):
                destination.write(compressor.compress(chunk))
                total += len(chunk)
            destination.write(compressor.flush())
            return total
        case "zst":
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise ImportError("Zstandard support requires: pip install archivekit[zstd]") from exc
            compressor = zstd.ZstdCompressor(
                level=level if level is not None else 3,
                threads=threads if threads is not None else 0,
            )
            with compressor.stream_writer(destination) as writer:
                return copy_stream(source, writer, chunk_size)
        case "br":
            try:
                import brotli
            except ImportError as exc:
                raise ImportError("Brotli support requires: pip install archivekit[brotli]") from exc
            compressor = brotli.Compressor(quality=level if level is not None else 11)
            total = 0
            while chunk := source.read(chunk_size):
                destination.write(compressor.process(chunk))
                total += len(chunk)
            destination.write(compressor.finish())
            return total
        case "lz4":
            try:
                import lz4.frame
            except ImportError as exc:
                raise ImportError("LZ4 support requires: pip install archivekit[lz4]") from exc
            with lz4.frame.open(
                destination,
                mode="wb",
                compression_level=level if level is not None else 0,
            ) as writer:
                return copy_stream(source, writer, chunk_size)
        case "snappy":
            try:
                from cramjam import snappy
            except ImportError as exc:
                raise ImportError("Snappy support requires: pip install archivekit[snappy]") from exc
            data = bytearray()
            while chunk := source.read(chunk_size):
                data.extend(chunk)
            destination.write(snappy.compress(bytes(data)))
            return len(data)
        case _:
            raise ValueError(f"Unsupported stream codec: {codec}")


def decompress_stream(
    source: BinaryIO,
    destination: BinaryIO,
    codec: str,
    *,
    chunk_size: int = CHUNK_SIZE,
) -> int:
    match codec:
        case "gz":
            with gzip.GzipFile(fileobj=source, mode="rb") as reader:
                return copy_stream(reader, destination, chunk_size)
        case "bz2":
            decompressor = bz2.BZ2Decompressor()
            total = 0
            while chunk := source.read(chunk_size):
                output = decompressor.decompress(chunk)
                destination.write(output)
                total += len(output)
            return total
        case "xz":
            decompressor = lzma.LZMADecompressor()
            total = 0
            while chunk := source.read(chunk_size):
                output = decompressor.decompress(chunk)
                destination.write(output)
                total += len(output)
            return total
        case "zst":
            try:
                import zstandard as zstd
            except ImportError as exc:
                raise ImportError("Zstandard support requires: pip install archivekit[zstd]") from exc
            decompressor = zstd.ZstdDecompressor()
            with decompressor.stream_reader(source) as reader:
                return copy_stream(reader, destination, chunk_size)
        case "br":
            try:
                import brotli
            except ImportError as exc:
                raise ImportError("Brotli support requires: pip install archivekit[brotli]") from exc
            decompressor = brotli.Decompressor()
            total = 0
            while chunk := source.read(chunk_size):
                output = decompressor.process(chunk)
                destination.write(output)
                total += len(output)
            return total
        case "lz4":
            try:
                import lz4.frame
            except ImportError as exc:
                raise ImportError("LZ4 support requires: pip install archivekit[lz4]") from exc
            with lz4.frame.open(source, mode="rb") as reader:
                return copy_stream(reader, destination, chunk_size)
        case "snappy":
            try:
                from cramjam import snappy
            except ImportError as exc:
                raise ImportError("Snappy support requires: pip install archivekit[snappy]") from exc
            data = bytearray()
            while chunk := source.read(chunk_size):
                data.extend(chunk)
            output = snappy.decompress(bytes(data))
            destination.write(output)
            return len(output)
        case _:
            raise ValueError(f"Unsupported stream codec: {codec}")
