from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Format:
    name: str
    suffixes: tuple[str, ...]
    kind: str


FORMATS: tuple[Format, ...] = (
    Format("tar.xz", (".tar.xz", ".txz"), "tar"),
    Format("tar.gz", (".tar.gz", ".tgz"), "tar"),
    Format("tar.bz2", (".tar.bz2", ".tbz2", ".tbz"), "tar"),
    Format("tar.br", (".tar.br",), "tar"),
    Format("tar.zst", (".tar.zst", ".tzst"), "tar"),
    Format("tar", (".tar",), "tar"),
    Format("whl", (".whl",), "zip"),
    Format("zip", (".zip",), "zip"),
    Format("7z", (".7z",), "7z"),
    Format("xz", (".xz",), "stream"),
    Format("gz", (".gz",), "stream"),
    Format("bz2", (".bz2",), "stream"),
    Format("br", (".br",), "stream"),
    Format("zst", (".zst",), "stream"),
    Format("lz4", (".lz4",), "stream"),
    Format("snappy", (".snappy",), "stream"),
)


def detect_format(path: str | Path) -> Format:
    path = Path(path)
    name = path.name.lower()
    for fmt in FORMATS:
        if any(name.endswith(suffix) for suffix in fmt.suffixes):
            return fmt
    raise ValueError(f"Unsupported archive format: {path}")


def preferred_suffix(format_name: str) -> str:
    for fmt in FORMATS:
        if fmt.name == format_name:
            return fmt.suffixes[0]
    raise ValueError(f"Unsupported output format: {format_name}")
