import zipfile, io


def process_zip_archive(src_path: str, process_func, dst_path: str = None):
    if dst_path is None:
        dst_path = src_path + ".processed"
    with zipfile.ZipFile(src_path, "r") as zin:
        with zipfile.ZipFile(dst_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if not item.is_dir():
                    content = zin.read(item.filename)
                    new_content = process_func(content)
                    zout.writestr(item, new_content)
                else:
                    zout.writestr(item, "")
    return dst_path
