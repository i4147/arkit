"""Tests for ``walk`` (``VirtualPath``) and ``AdvancedWalker`` (``VirtualFile``)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from arkit import (
    AdvancedWalker,
    ArchiveWalker,
    VirtualPath,
    compress,
    pack,
    pack_file,
    read_members,
    walk,
)


def test_lazy_walk_into_3_level_nested_archives_and_virtualpath_api(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "plain.txt").write_text("real file", encoding="utf-8")

    # Level 3: standalone .gz inside Level 2 .zip inside Level 1 .tar.zst
    gz_blob = compress(b"deepest secret", "gz")
    inner_zip_blob = pack({"secret.txt.gz": gz_blob, "sub/readme.md": b"# Inner"}, "zip")
    outer_tar = root / "bundle.tar.zst"
    pack_file(outer_tar, {"nested.zip": inner_zip_blob, "manifest.json": b'{"v":1}'})

    # Corrupt archive should degrade gracefully (yielded as plain file, never crash walk!)
    corrupt_zip = root / "broken.zip"
    corrupt_zip.write_bytes(b"PK\x03\x04not_a_real_zip")

    entries = list(walk(root, max_depth=3))
    rel_paths = [vp.relative_to(root).as_posix() for vp in entries]

    assert "plain.txt" in rel_paths
    assert "broken.zip" in rel_paths
    assert "bundle.tar.zst" in rel_paths
    assert "bundle.tar.zst/manifest.json" in rel_paths
    assert "bundle.tar.zst/nested.zip" in rel_paths
    assert "bundle.tar.zst/nested.zip/sub/readme.md" in rel_paths
    assert "bundle.tar.zst/nested.zip/secret.txt.gz/secret.txt" in rel_paths

    by_rel = {vp.relative_to(root).as_posix(): vp for vp in entries}
    deep_vp = by_rel["bundle.tar.zst/nested.zip/secret.txt.gz/secret.txt"]
    assert deep_vp.in_archive is True
    assert deep_vp.depth == 3
    assert deep_vp.real == outer_tar
    assert deep_vp.name == "secret.txt"
    assert deep_vp.stem == "secret"
    assert deep_vp.suffix == ".txt"
    assert deep_vp.match("*/secret.txt")
    assert deep_vp.read_text() == "deepest secret"

    # Re-openable independent streams!
    with deep_vp.open() as s1, deep_vp.open() as s2:
        assert s1.read(7) == b"deepest"
        assert s2.read() == b"deepest secret"
        assert s1.read() == b" secret"

    # VirtualPath deliberately does NOT implement os.PathLike / __fspath__
    with pytest.raises(TypeError):
        os.fspath(deep_vp)  # type: ignore[arg-type]

    # Path manipulation & comparison
    derived = deep_vp.parent / "sibling.json"
    assert isinstance(derived, VirtualPath)
    assert derived.name == "sibling.json"
    with pytest.raises(ValueError, match="no backing content"):
        derived.read_bytes()

    # Depth guard max_depth=1 stops before entering nested.zip
    shallow_rels = [vp.relative_to(root).as_posix() for vp in walk(root, max_depth=1)]
    assert "bundle.tar.zst/nested.zip" in shallow_rels
    assert "bundle.tar.zst/nested.zip/sub/readme.md" not in shallow_rels


def test_walk_symlink_cycle_detection(tmp_path: Path) -> None:
    root = tmp_path / "symroot"
    sub = root / "a" / "b"
    sub.mkdir(parents=True)
    (sub / "ok.txt").write_text("ok", encoding="utf-8")
    loop = sub / "loop_to_root"
    loop.symlink_to(root)

    # Should terminate cleanly without infinite recursion when follow_symlinks=True!
    walked = list(walk(root, follow_symlinks=True))
    assert any(vp.name == "ok.txt" for vp in walked)


def test_advanced_walker_two_way_nested_rewrite_and_rollback(tmp_path: Path) -> None:
    assert ArchiveWalker is AdvancedWalker

    ws = tmp_path / "workspace"
    ws.mkdir()
    (ws / "disk.txt").write_bytes(b"v1 disk")

    # Standalone .zst file
    single_zst = ws / "config.env.zst"
    single_zst.write_bytes(compress(b"MODE=dev\n", "zst"))

    # Nested: outer.tar.zst -> inner.zip -> app.conf
    inner_zip = pack({"app.conf": b"port=8080\n", "keep.txt": b"unchanged"}, "zip")
    outer_tar = ws / "deploy.tar.zst"
    pack_file(outer_tar, {"inner.zip": inner_zip, "meta.txt": b"release=1"})

    # 1. Exception inside `with AdvancedWalker(...)` must NOT flush archive rewrites!
    original_outer_bytes = outer_tar.read_bytes()
    with pytest.raises(RuntimeError, match="abort"):
        with AdvancedWalker(ws) as walker:
            for vf in walker:
                if vf.path.endswith("app.conf"):
                    vf.write_bytes(b"port=9999\n")
                    raise RuntimeError("abort")
    assert outer_tar.read_bytes() == original_outer_bytes

    # 2. Clean exit rebuilds nested archives deepest-first atomically!
    visited_paths: list[str] = []
    with AdvancedWalker(ws) as walker:
        for vf in walker:
            visited_paths.append(vf.path)
            if vf.path == "disk.txt":
                vf.write_text("v2 disk")
            elif vf.path == "config.env.zst":
                assert vf.read_bytes() == b"MODE=dev\n"
                vf.write_bytes(b"MODE=prod\n")
            elif vf.path == "deploy.tar.zst!inner.zip!app.conf":
                assert vf.read_bytes() == b"port=8080\n"
                vf.write_bytes(b"port=9090\n")

    assert (ws / "disk.txt").read_bytes() == b"v2 disk"

    # Re-walk read-only to verify every modified archive & untouched sibling!
    verified: dict[str, bytes] = {}
    with AdvancedWalker(ws) as check_walker:
        for vf in check_walker:
            verified[vf.path] = vf.read_bytes()

    assert verified["config.env.zst"] == b"MODE=prod\n"
    assert verified["deploy.tar.zst!inner.zip!app.conf"] == b"port=9090\n"
    assert verified["deploy.tar.zst!inner.zip!keep.txt"] == b"unchanged"
    assert verified["deploy.tar.zst!meta.txt"] == b"release=1"
    assert len(read_members(outer_tar)) == 2
