"""Shared pytest fixtures for the ``arkit`` test suite."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def sample_tree(tmp_path: Path) -> Path:
    """Create a realistic multi-directory sample project tree for archive tests."""
    root = tmp_path / "project"
    (root / "src" / "pkg").mkdir(parents=True)
    (root / "docs").mkdir(parents=True)
    (root / "README.md").write_text("# Sample Project\n\nHello from arkit!\n", encoding="utf-8")
    (root / "src" / "pkg" / "__init__.py").write_text("__version__ = '0.1.0'\n", encoding="utf-8")
    (root / "src" / "pkg" / "data.bin").write_bytes(bytes(range(256)) * 64)
    (root / "docs" / "notes.txt").write_text("line 1\n\nline 2\n   \nline 3\n", encoding="utf-8")
    return root


@pytest.fixture
def single_file(tmp_path: Path) -> Path:
    """Create a compressible binary/text hybrid file (~128 KiB)."""
    path = tmp_path / "payload.dat"
    block = b"arkit-streaming-payload-0123456789ABCDEF\n" * 3200
    path.write_bytes(block)
    return path
