#!/usr/bin/env python3
"""Helper script to extract all public classes, functions, and constants from legacy src/arkit."""

from __future__ import annotations

import ast
from pathlib import Path


def main() -> None:
    src_dir = Path(__file__).resolve().parent.parent / "src" / "arkit"
    print(f"Scanning {src_dir}...\n")
    total_lines = 0
    for py_file in sorted(src_dir.glob("*.py")):
        source = py_file.read_text(encoding="utf-8")
        lines = len(source.splitlines())
        total_lines += lines
        try:
            tree = ast.parse(source, filename=str(py_file))
        except SyntaxError as exc:
            print(f"[SYNTAX NOTE] {py_file.name} ({lines} lines): {exc}")
            continue

        classes = [n.name for n in tree.body if isinstance(n, ast.ClassDef) and not n.name.startswith("_")]
        funcs = [n.name for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")]
        assigns = []
        for n in tree.body:
            if isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name) and not t.id.startswith("_"):
                        assigns.append(t.id)
        print(f"=== {py_file.name} ({lines} lines) ===")
        if classes:
            print(f"  Classes : {', '.join(classes)}")
        if funcs:
            print(f"  Funcs   : {', '.join(funcs)}")
        if assigns:
            print(f"  Globals : {', '.join(assigns)}")
    print(f"\nTotal legacy lines across src/arkit: {total_lines}")


if __name__ == "__main__":
    main()
