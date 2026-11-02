def validate(archive_path: str) -> bool:
    ext = _detect_format(archive_path)
    if ext.startswith("tar"):
        # Use tarfile.is_tarfile or try opening
        try:
            with tarfile.open(archive_path) as _:
                return True
        except:
            return False
    elif ext in ("zip", "whl"):
        return zipfile.is_zipfile(archive_path)
    elif ext == "7z":
        import py7zr

        try:
            with py7zr.SevenZipFile(archive_path, "r") as _:
                return True
        except:
            return False
    else:
        raise ValueError("Unsupported archive format")
