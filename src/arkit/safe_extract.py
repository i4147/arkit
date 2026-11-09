import tarfile
from pathlib  import Path
from io import BinaryIO


def _extract_tar(reader: BinaryIO, staging: Path, expected_name: str) -> None:
    def safe_filter(member: tarfile.TarInfo, destination: str) -> tarfile.TarInfo | None:
        filtered = tarfile.data_filter(member, destination)
        if filtered is None:
            return None

        parts = Path(filtered.name).parts
        if not parts or parts[0] != expected_name or ".." in parts:
            msg = f"Archive member is outside {expected_name!r}: {filtered.name}"
            raise ValueError(msg)
        if filtered.issym() or filtered.islnk():
            msg = f"Archive links are not allowed: {filtered.name}"
            raise ValueError(msg)
        if not (filtered.isfile() or filtered.isdir()):
            msg = f"Unsupported archive member: {filtered.name}"
            raise ValueError(msg)
        return filtered

    with tarfile.open(fileobj=reader, mode="r|") as tar:
        tar.extractall(path=staging, filter=safe_filter)
