from dh2 import PBCompressor


def on_progress(done, total):
    pct = 100 * done / total if total else 0
    print(f"\r{pct:5.1f}%  ({don88e:,}/{total:,} bytes)", end="")


compressor = PBCompressor(
    quality=3,
    chunk_size=1024 * 1024,
    workers=4,
    progress_callback=on_progress,
)
compressor.compress_file("bigfile.bin", "bigfile.pbrt")
print()
PBCompressor.decompress_file("bigfile.pbrt", "bigfile.out")
compressor = PBCompressor(
    quality=3,
    chunk_size=2 * 1024 * 1024,
    mode=brotli.MODE_TEXT,
)
compressor.compress_file("huge.json", "huge.json.pbrt")
