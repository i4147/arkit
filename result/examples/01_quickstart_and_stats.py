#!/usr/bin/env python3
"""Example 01: Quickstart — Compress, Inspect, Validate, Convert, Diff, and Extract."""

from __future__ import annotations

import tempfile
from pathlib import Path

from arkit import Archive, compress, convert, decompress, diff_archives, info, validate


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="arkit_ex01_") as tmp:
        ws = Path(tmp)
        project = ws / "my_service"
        (project / "app").mkdir(parents=True)
        (project / "README.md").write_text("# My Service\n\nProduction archive demo.\n", encoding="utf-8")
        (project / "app" / "config.json").write_text('{"port": 8080, "workers": 4}\n', encoding="utf-8")
        (project / "app" / "main.py").write_text("def run() -> str:\n    return 'ok'\n", encoding="utf-8")

        # 1. Compress directory into multi-threaded .tar.zst with SHA-256 digest
        tar_zst = ws / "release.tar.zst"
        stats = compress(
            project,
            tar_zst,
            format="tar.zst",
            level=6,
            threads=2,
            compute_sha256=True,
        )
        print(f"Created {tar_zst.name}:")
        print(f"  Payload bytes : {stats.source_bytes:,}")
        print(f"  Archive bytes : {stats.result_bytes:,} (ratio={stats.ratio:.3f})")
        print(f"  SHA-256       : {stats.sha256}")

        # 2. Inspect & validate without extracting to disk
        meta = info(tar_zst)
        report = validate(tar_zst)
        print(f"  Members       : {meta.member_count} ({[m.name for m in meta.members]})")
        print(f"  Healthy       : {report.healthy} (checked {report.checked_bytes:,} bytes)")

        # 3. Convert .tar.zst -> .zip in streaming mode and diff the two archives
        zip_path = ws / "release.zip"
        convert(tar_zst, zip_path, output_format="zip", level=6)
        diff = diff_archives(tar_zst, zip_path)
        print(f"  ZIP == TAR.ZST: {diff.identical} ({len(diff.unchanged)} identical members)")

        # 4. Object-oriented Archive handle & safe extraction
        with Archive(zip_path) as arch:
            assert "app/config.json" in arch
            print(f"  Read inline   : {arch.read('app/config.json').decode().strip()}")

        restored = ws / "unpacked"
        decompress(tar_zst, restored)
        assert (restored / "app" / "main.py").is_file()
        print(f"  Extracted to  : {restored}")


if __name__ == "__main__":
    main()
