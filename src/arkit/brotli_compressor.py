import os
import struct
import tempfile
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Callable, Optional
import brotli


class ParallelBrotliCompressor:
    MAGIC = b"PBRT0001"

    def __init__(
        self,
        quality: int = 1,
        chunk_size: int = 4 * 1024 * 1024,
        workers: Optional[int] = None,
        mode: int = brotli.MODE_GENERIC,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ):
        if not 0 <= quality <= 11:
            msg = "quality must be between 0 and 11"
            raise ValueError(msg)
        if chunk_size < 64 * 1024:
            msg = "chunk_size should be at least 64KB"
            raise ValueError(msg)
        self.quality = quality
        self.chunk_size = chunk_size
        self.workers = workers or cpu_count()
        self.mode = mode
        self.progress_callback = progress_callback

    def compress_file(self, input_path: str, output_path: str) -> None:
        input_path = Path(input_path)
        output_path = Path(output_path)
        total_size = input_path.stat().st_size
        tasks = self._iter_chunk_tasks(input_path)
        bytes_done = 0
        with open(output_path, "wb") as f_out:
            f_out.write(self.MAGIC)
            count_pos = f_out.tell()
            f_out.write(struct.pack("<I", 0))
            chunk_count = 0
            with Pool(processes=self.workers) as pool:
                for chunk_index, compressed, orig_len in pool.imap(
                    _compress_worker,
                    tasks,
                    chunksize=1,
                ):
                    f_out.write(struct.pack("<I", len(compressed)))
                    f_out.write(struct.pack("<I", orig_len))
                    f_out.write(compressed)
                    chunk_count += 1
                    bytes_done += orig_len
                    if self.progress_callback:
                        self.progress_callback(bytes_done, total_size)
            end_pos = f_out.tell()
            f_out.seek(count_pos)
            f_out.write(struct.pack("<I", chunk_count))
            f_out.seek(end_pos)

    @staticmethod
    def decompress_file(input_path: str, output_path: str) -> None:
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            magic = f_in.read(len(ParallelBrotliCompressor.MAGIC))
            if magic != ParallelBrotliCompressor.MAGIC:
                msg = "Not a ParallelBrotli container"
                raise ValueError(msg)
            (chunk_count,) = struct.unpack("<I", f_in.read(4))
            for _ in range(chunk_count):
                (comp_len,) = struct.unpack("<I", f_in.read(4))
                (_orig_len,) = struct.unpack("<I", f_in.read(4))
                payload = f_in.read(comp_len)
                if len(payload) != comp_len:
                    msg = "Truncated chunk payload"
                    raise IOError(msg)
                f_out.write(brotli.decompress(payload))

    def compress_bytes(self, data: bytes) -> bytes:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_in:
            tmp_in.write(data)
            tmp_in_path = tmp_in.name
        tmp_out_fd, tmp_out_path = tempfile.mkstemp()
        os.close(tmp_out_fd)
        try:
            self.compress_file(tmp_in_path, tmp_out_path)
            with open(tmp_out_path, "rb") as f:
                return f.read()
        finally:
            os.unlink(tmp_in_path)
            os.unlink(tmp_out_path)

    def _iter_chunk_tasks(self, input_path: Path):
        index = 0
        with open(input_path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                yield (index, chunk, self.quality, self.mode)
                index += 1


def _compress_worker(task):
    index, chunk, quality, mode = task
    compressed = brotli.compress(chunk, quality=quality, mode=mode)
    return index, compressed, len(chunk)
