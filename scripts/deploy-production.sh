#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RQUANT_DEPLOY_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    printf 'Deployment Python is not executable: %s\n' "${PYTHON_BIN}" >&2
    exit 2
fi

export PYTHONPATH="${PROJECT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"
exec "${PYTHON_BIN}" -m rquant.ops.production_deploy --repo "${PROJECT_DIR}" "$@"
