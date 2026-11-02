import tarfile, io
from .compression import open_stream_reader, open_stream_writer  # similar for writing


def process_tar_archive(src_path: str, process_func, dst_path: str = None):
    """Iterate over members of a tar archive (compressed or not), apply process_func to each file's content, write new archive."""
    if dst_path is None:
        dst_path = src_path + ".processed"

    # Detect compression from extension
    ext = _get_compression_ext(src_path)
    if ext:
        src_stream = open_stream_reader(src_path, ext)
    else:
        src_stream = open(src_path, "rb")

    # For writing, we need to wrap with compression if dst has extension
    dst_ext = _get_compression_ext(dst_path)
    if dst_ext:
        dst_stream = open_stream_writer(dst_path, dst_ext)
    else:
        dst_stream = open(dst_path, "wb")

    try:
        with tarfile.open(fileobj=src_stream, mode="r|") as tar_in:
            with tarfile.open(fileobj=dst_stream, mode="w|") as tar_out:
                for member in tar_in:
                    if member.isfile():
                        # Read original content (may be large, but tarfile reads in chunks)
                        content = tar_in.extractfile(member).read()
                        new_content = process_func(content)
                        # Create new TarInfo with same metadata
                        new_info = tarfile.TarInfo(name=member.name)
                        new_info.size = len(new_content)
                        new_info.mtime = member.mtime
                        new_info.mode = member.mode
                        tar_out.addfile(new_info, io.BytesIO(new_content))
                    else:
                        # Directories, links, etc. – copy as is
                        tar_out.addfile(member)
    finally:
        src_stream.close()
        dst_stream.close()
    return dst_path
