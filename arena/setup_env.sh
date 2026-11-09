#!/usr/bin/env bash
# Helper script used during the Arena session to create an isolated virtual
# environment (.venv) and install test, lint, type-check, and optional codec
# dependencies for developing and verifying `arkit`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"

echo "[setup_env] Repository root: ${REPO_ROOT}"

if [ ! -d "${VENV_DIR}" ]; then
    echo "[setup_env] Creating virtual environment at ${VENV_DIR}..."
    python3 -m venv "${VENV_DIR}"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel

echo "[setup_env] Installing development & optional codec packages..."
python -m pip install \
    pytest \
    pytest-cov \
    mypy \
    ruff \
    zstandard \
    brotli \
    lz4 \
    py7zr \
    cramjam

echo "[setup_env] Installed packages:"
python -m pip list
