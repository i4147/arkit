from pyarchive import process_archive


def remove_blank_lines(data: bytes) -> bytes:
    return b"\n".join(line for line in data.splitlines() if line.strip())


new_archive = process_archive("data.tar.gz", remove_blank_lines, "cleaned.tar.gz")
