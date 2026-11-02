from pathlib import Path
from archivekit import compress, convert, decompress, validate

stats = compress(
    Path("project"),
    Path("project.tar.zst"),
    format="tar.zst",
    level=12,
    threads=4,
)
print(f"Input: {stats.source_bytes} bytes")
print(f"Output: {stats.result_bytes} bytes")
print(f"Ratio: {stats.ratio:.3f}")
print(f"Space freed: {stats.space_freed} bytes")
print(f"Reduction: {stats.space_change_percent:.2f}%")
decompress(Path("project.tar.zst"), Path("restored-project"))
report = validate(Path("project.tar.zst"))
print(report.healthy, report.errors)
convert(
    Path("project.zip"),
    Path("project.tar.xz"),
    output_format="tar.xz",
    level=9,
)
