import os
import tarfile
import tempfile
from zipfile import ZipFile

CHUNK_SIZE = 1024 * 1024

class CorruptedContent(Exception):
    pass

def _verify_tar(fileobj):
    fileobj.seek(0)

    with tarfile.open(fileobj=fileobj, mode="r:") as tf:
        for member in tf:
            if not member.isfile():
                continue

        content = tf.extractfile(member)
        if content is None:
            continue

        with content:
            while content.read(CHUNK_SIZE):
                pass


def _decompress_brotli(source, destination):
    import brotli

    dec = brotli.Decompressor()

    while chunk := source.read(CHUNK_SIZE):
        data = dec.process(chunk)
        if data:
            destination.write(data)

    if not dec.is_finished():
        raise EOFError("Incomplete Brotli stream")


def _decompress_zstd(source, destination):
    import zstandard

    dctx = zstandard.ZstdDecompressor()
    with dctx.stream_reader(source,read_across_frames=True,closefd=False) as reader:
        while chunk := reader.read(CHUNK_SIZE):
            destination.write(chunk)

def _decompress_lz4(source, destination):
    import lz4.frame


    with lz4.frame.open(source, mode="rb") as reader:
        while chunk := reader.read(CHUNK_SIZE):
            destination.write(chunk)


def verify_archive(archive_file, type):
    archive_file = os.fspath(archive_file)

    fmt = str(type).lower()
    filename = archive_file.lower()

    try:
        if fmt.endswith("zip") or filename.endswith(".zip"):
            with ZipFile(archive_file) as zf:
                error = zf.testzip()

                if error:
                    raise ValueError(f"Corrupt ZIP member: {error}")

            return True

        if fmt.endswith("7z") or filename.endswith(".7z"):
            import py7zr

            with py7zr.SevenZipFile(archive_file, mode="r") as zf:
                error = zf.testzip()

                if error:
                    raise ValueError(f"Corrupt 7z member: {error}")

                result = zf.test()
                if result is False:
                    raise ValueError("7z CRC verification failed")

            return True

        decompressors = {
            "br": _decompress_brotli,
            "zst": _decompress_zstd,
            "lz4": _decompress_lz4,
        }

        extension = next(
            (ext for ext in decompressors if
             fmt.endswith("tar." + ext) or
             filename.endswith(".tar." + ext)),
            None
        )

        if extension:
            with open(archive_file, "rb") as source:
                with tempfile.TemporaryFile() as tmp:
                    decompressors[extension](source, tmp)
                    _verify_tar(tmp)

            return True

        if fmt.endswith("tar") or filename.endswith(
            (".tar", ".tar.gz", ".tgz", ".tar.bz2",
             ".tbz2", ".tar.xz", ".txz")
        ):
            with tarfile.open(archive_file, mode="r:*") as tf:
                for member in tf:
                    if not member.isfile():
                        continue

                    content = tf.extractfile(member)
                    if content:
                        with content:
                            while content.read(CHUNK_SIZE):
                                pass

            return True

        raise ValueError(f"Unsupported archive format: {type}")

    except CorruptedContent:
        raise

    except Exception as ex:
        raise CorruptedContent(
            'Failed to verify file "{}"'.format(archive_file)
        ) from ex


https://py7zr.readthedocs.io/en/latest/api.html?utm_source=chatgpt.com
https://python-zstandard.readthedocs.io/en/latest/decompressor.html?utm_source=chatgpt.com
