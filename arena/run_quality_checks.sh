#!/usr/bin/env bash
# Helper script to install `result/` in editable mode and run ruff, mypy --strict, and pytest.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
RESULT_DIR="${REPO_ROOT}/result"

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "[quality] Installing result/ package in editable mode..."
python -m pip install --no-build-isolation --no-deps -e "${RESULT_DIR}"

echo "[quality] Running ruff lint checks..."
ruff check "${RESULT_DIR}"

echo "[quality] Running mypy strict static type checks..."
(cd "${RESULT_DIR}" && mypy)

if [ -d "${RESULT_DIR}/tests" ] && compgen -G "${RESULT_DIR}/tests/test_*.py" > /dev/null; then
    echo "[quality] Running pytest suite with coverage..."
    (cd "${RESULT_DIR}" && pytest --cov=arkit --cov-report=term-missing -v)
fi
