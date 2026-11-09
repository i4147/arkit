#!/usr/bin/env python3
"""Example 04: Lazy Read-Only ``walk()`` & Two-Way Nested ``AdvancedWalker`` Edits."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from arkit import AdvancedWalker, compress, pack, pack_file, walk


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="arkit_ex04_") as tmp:
        ws = Path(tmp)
        (ws / "README.txt").write_text("Workspace root file\n", encoding="utf-8")

        # Build a 3-level nested archive: outer.tar.zst -> inner.zip -> settings.json (+ notes.txt.gz)
        gz_leaf = compress(b"Deep leaf note inside .gz inside .zip inside .tar.zst!", "gz")
        inner_zip = pack(
            {
                "config/settings.json": b'{"env": "staging", "debug": true}',
                "docs/notes.txt.gz": gz_leaf,
            },
            "zip",
        )
        outer_bundle = ws / "deployment.tar.zst"
        pack_file(outer_bundle, {"service.zip": inner_zip, "VERSION": b"1.0.0\n"})

        # 1. Lazy read-only walk() — inspect 3 levels deep without extracting anything to disk!
        print("=== Read-only walk() ===")
        for vp in walk(ws, max_depth=3, include_dirs=False):
            rel = vp.relative_to(ws)
            print(f"  [depth={vp.depth}] {rel} (size={vp.size})")
            if vp.name == "notes.txt":
                print(f"    -> content: {vp.read_text()!r}")

        # 2. Two-way AdvancedWalker — edit JSON inside deployment.tar.zst!service.zip!config/settings.json
        #    and rebuild nested archives atomically deepest-first on exit!
        with AdvancedWalker(ws) as editor:
            for vf in editor:
                if vf.path.endswith("settings.json"):
                    cfg = json.loads(vf.read_text())
                    cfg["env"] = "production"
                    cfg["debug"] = False
                    vf.write_text(json.dumps(cfg, sort_keys=True))

        # Verify updated nested JSON with walk()
        for vp in walk(outer_bundle, max_depth=2):
            if vp.name == "settings.json":
                print("Updated nested JSON:", vp.read_text())


if __name__ == "__main__":
    main()
