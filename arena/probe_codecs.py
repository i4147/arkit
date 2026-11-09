#!/usr/bin/env python3
"""Helper script to probe installed optional codec APIs and verify streaming behavior."""

from __future__ import annotations

import io

import brotli
import cramjam
import lz4.frame
import py7zr
import py7zr.io
import zstandard


def main() -> None:
    sample = b"arkit streaming test payload " * 1000

    # 1. cramjam submodules
    cram_attrs = [a for a in dir(cramjam) if not a.startswith("_")]
    print("cramjam attrs:", cram_attrs)
    snappy_framed = bytes(cramjam.snappy.compress(sample))
    assert bytes(cramjam.snappy.decompress(snappy_framed)) == sample
    print("cramjam.snappy framed magic:", snappy_framed[:10])
    print("cramjam.snappy attrs:", [a for a in dir(cramjam.snappy) if not a.startswith("_")])

    # Test cramjam snappy true O(1)-memory framed streaming reader/writer!
    SNAPPY_HEADER = b"\xff\x06\x00\x00sNaPpY"
    comp = cramjam.snappy.Compressor()
    comp.compress(sample[:500])
    c1 = bytes(comp.flush())
    comp.compress(sample[500:])
    c2 = bytes(comp.finish())
    framed_stream = io.BytesIO(c1 + c2)
    out_chunks: list[bytes] = []
    while True:
        hdr = framed_stream.read(4)
        if not hdr:
            break
        assert len(hdr) == 4
        ctype = hdr[0]
        clen = int.from_bytes(hdr[1:4], "little")
        body = framed_stream.read(clen)
        assert len(body) == clen
        if ctype == 0xFF:
            assert body == b"sNaPpY"
        elif ctype in (0x00, 0x01):
            out_chunks.append(bytes(cramjam.snappy.decompress(SNAPPY_HEADER + hdr + body)))
    assert b"".join(out_chunks) == sample
    print("cramjam.snappy true O(64KB) framed streaming OK! Compressed len:", len(c1 + c2))

    # 2. py7zr 1.1.4 API check
    buf = io.BytesIO()
    with py7zr.SevenZipFile(buf, "w") as sz:
        sz.writestr(sample, "dir/hello.txt")
    buf.seek(0)
    with py7zr.SevenZipFile(buf, "r") as sz:
        print("py7zr list():", [(f.filename, f.uncompressed, f.compressed, f.is_directory) for f in sz.list()])
        print("py7zr has readall:", hasattr(sz, "readall"), "has test:", hasattr(sz, "test"), "has testzip:", hasattr(sz, "testzip"))
        res_test = sz.test()
        print("sz.test() returned:", repr(res_test))
        sz.reset()
        res_testzip = sz.testzip()
        print("sz.testzip() returned:", repr(res_testzip))
        sz.reset()
        factory = py7zr.io.BytesIOFactory(limit=10 * 1024 * 1024)
        sz.extract(targets=["dir/hello.txt"], factory=factory)
        assert factory.products["dir/hello.txt"].read() == sample
    print("py7zr 1.1.4 writestr + BytesIOFactory OK")

    # 3. brotli streaming Compressor / Decompressor
    b_comp = brotli.Compressor(quality=4)
    b_data = b_comp.process(sample[:1000]) + b_comp.process(sample[1000:]) + b_comp.finish()
    b_dec = brotli.Decompressor()
    b_out = b_dec.process(b_data[:20]) + b_dec.process(b_data[20:])
    assert b_dec.is_finished() and b_out == sample
    print("brotli streaming OK")

    # 4. zstandard streaming reader/writer with threads
    z_buf = io.BytesIO()
    cctx = zstandard.ZstdCompressor(level=3, threads=2)
    with cctx.stream_writer(z_buf, closefd=False) as writer:
        writer.write(sample)
    z_buf.seek(0)
    dctx = zstandard.ZstdDecompressor()
    with dctx.stream_reader(z_buf, read_across_frames=True, closefd=False) as reader:
        assert reader.read() == sample
    print("zstandard multi-threaded streaming OK")

    # 5. lz4.frame streaming
    l_buf = io.BytesIO()
    with lz4.frame.open(l_buf, mode="wb", compression_level=3) as lw:
        lw.write(sample)
    l_buf.seek(0)
    with lz4.frame.open(l_buf, mode="rb") as lr:
        assert lr.read() == sample
    print("lz4.frame streaming OK")


if __name__ == "__main__":
    main()
