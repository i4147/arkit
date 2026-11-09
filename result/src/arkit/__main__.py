"""Allow running ``python -m arkit`` as a CLI entry point."""

from __future__ import annotations

import sys

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
