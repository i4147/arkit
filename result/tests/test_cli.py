"""End-to-end tests for the ``arkit`` CLI (``arkit.cli.main``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from arkit.cli import main, parse_byte_size


def test_parse_byte_size_units() -> None:
    assert parse_byte_size("512") == 512
    assert parse_byte_size("64K") == 64 * 1024
    assert parse_byte_size("4MiB") == 4 * 1024 * 1024
    assert parse_byte_size("1.5G") == int(1.5 * (1024**3))
    with pytest.raises(ValueError):
        parse_byte_size("0B")
    with pytest.raises(ValueError):
        parse_byte_size("10XB")


def test_cli_compress_info_list_validate_convert_diff_extract(
    tmp_path: Path,
    sample_tree: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    arch_zst = tmp_path / "cli_proj.tar.zst"
    arch_zip = tmp_path / "cli_proj.zip"

    # 1. --dry-run compress
    rc = main(["compress", str(sample_tree), str(arch_zst), "--dry-run", "--json"])
    assert rc == 0
    dry_data = json.loads(capsys.readouterr().out)
    assert dry_data["dry_run"] is True
    assert dry_data["count"] == 4
    assert not arch_zst.exists()

    # 2. Real deterministic compress with --checksum --json
    rc = main(["compress", str(sample_tree), str(arch_zst), "--deterministic", "--checksum", "--json"])
    assert rc == 0
    comp_data = json.loads(capsys.readouterr().out)
    assert comp_data["source_bytes"] > 10_000
    assert len(comp_data["sha256"]) == 64

    # 3. info & list & validate
    assert main(["info", str(arch_zst), "--checksum", "--json"]) == 0
    info_data = json.loads(capsys.readouterr().out)
    assert info_data["format"] == "tar.zst"
    assert info_data["member_count"] == 4

    assert main(["list", str(arch_zst), "-p", "*.py", "--json"]) == 0
    ls_data = json.loads(capsys.readouterr().out)
    assert len(ls_data) == 1 and ls_data[0]["name"] == "src/pkg/__init__.py"

    assert main(["validate", str(arch_zst), "--json"]) == 0
    val_data = json.loads(capsys.readouterr().out)
    assert val_data["healthy"] is True

    # 4. convert & diff
    assert main(["convert", str(arch_zst), str(arch_zip), "--json"]) == 0
    capsys.readouterr()

    assert main(["diff", str(arch_zst), str(arch_zip), "--json"]) == 0
    diff_data = json.loads(capsys.readouterr().out)
    assert diff_data["identical"] is True

    # 5. walk --grep inside archive without extracting!
    assert main(["walk", str(arch_zst), "--grep", "Hello from arkit", "--json"]) == 0
    walk_hits = json.loads(capsys.readouterr().out)
    assert len(walk_hits) == 1
    assert walk_hits[0]["path"].endswith("README.md")

    # 6. extract
    ext_dir = tmp_path / "cli_restored"
    assert main(["extract", str(arch_zst), "-o", str(ext_dir), "--json"]) == 0
    capsys.readouterr()
    assert (ext_dir / "README.md").read_text(encoding="utf-8").startswith("# Sample Project")


def test_cli_split_merge_pbrt_batch_and_codecs(
    tmp_path: Path,
    single_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # codecs report
    assert main(["codecs", "--json"]) == 0
    codecs_info = json.loads(capsys.readouterr().out)
    assert codecs_info["missing"] == []

    # pbrt compress & decompress
    pbrt_path = tmp_path / "payload.pbrt"
    restored_pbrt = tmp_path / "payload.restored"
    assert main(["pbrt", str(single_file), str(pbrt_path), "-q", "3", "-c", "64K", "-w", "2", "--json"]) == 0
    capsys.readouterr()
    assert main(["pbrt", "-d", str(pbrt_path), str(restored_pbrt), "-w", "2", "--json"]) == 0
    capsys.readouterr()
    assert restored_pbrt.read_bytes() == single_file.read_bytes()

    # split & merge with SHA-256 manifest
    parts_dir = tmp_path / "split_parts"
    assert main(["split", str(pbrt_path), "-s", "1K", "-o", str(parts_dir), "--manifest", "--json"]) == 0
    split_info = json.loads(capsys.readouterr().out)
    merged_pbrt = tmp_path / "merged.pbrt"
    assert (
        main(
            [
                "merge",
                *split_info["parts"],
                "-o",
                str(merged_pbrt),
                "--sort",
                "--verify-manifest",
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert merged_pbrt.read_bytes() == pbrt_path.read_bytes()

    # batch-extract
    batch_dir = tmp_path / "batch_in"
    batch_dir.mkdir()
    assert main(["compress", str(single_file), str(batch_dir / "item.gz"), "-q"]) == 0
    assert main(["batch-extract", str(batch_dir), "-w", "2", "--json"]) == 0
    b_res = json.loads(capsys.readouterr().out)
    assert b_res["succeeded"] == 1
    assert (batch_dir / "item").read_bytes() == single_file.read_bytes()
