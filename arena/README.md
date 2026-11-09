# Session Helper Scripts (`arena/`)

All helper scripts (Bash and Python) created and used during this development session are saved in this folder:

| Script | Purpose |
| :--- | :--- |
| `setup_env.sh` | Creates `.venv` and installs dev/test tools (`pytest`, `pytest-cov`, `mypy`, `ruff`) and optional codecs (`zstandard`, `brotli`, `lz4`, `py7zr`, `cramjam`). |
| `audit_legacy_code.py` | AST audit tool cataloging public symbols, classes, globals, and syntax defects across the original `src/arkit/*.py` prototype files. |
| `probe_codecs.py` | Runtime probe verifying streaming behaviors of `cramjam.snappy`, `py7zr`, `brotli` (including GIL-release threading scaling), `zstandard`, and `lz4.frame`. |
| `run_quality_checks.sh` | Installs `result/` in editable mode and runs `ruff check`, `mypy --strict`, and `pytest --cov=arkit`. |
| `verify_examples.sh` | Executes every example script in `result/examples/` end-to-end. |
| `run_benchmarks.py` | Runs all benchmark suites in `result/benchmark/` and writes empirical measurements to `result/benchmark/results.md`. |
