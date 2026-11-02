from pathlib import Path


class UnsafeArchivePathError(ValueError):
    pass


def safe_destination(root: Path, member_name: str) -> Path:
    root = root.resolve()
    destination = (root / member_name).resolve()
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise UnsafeArchivePathError(f"Archive member escapes extraction root: {member_name!r}") from exc
    return destination


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
