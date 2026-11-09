#!/usr/bin/env bash
# Helper script in arena/ to run all example scripts and verify clean execution.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${ROOT_DIR}/.venv/bin/python"

for script in "${ROOT_DIR}"/result/examples/*.py; do
    echo ">>> Running $(basename "${script}") ..."
    "${VENV_PY}" "${script}"
done
echo ">>> All examples executed cleanly!"
