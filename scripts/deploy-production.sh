#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RQUANT_DEPLOY_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    printf 'Deployment Python is not executable: %s\n' "${PYTHON_BIN}" >&2
    exit 2
fi

TRUSTED_GIT="${RQUANT_TRUSTED_GIT_PATH:-/usr/bin/git}"
PROJECT_PARENT="$(dirname "${PROJECT_DIR}")"
DEPLOY_LOCK="${RQUANT_DEPLOY_LOCK_PATH:-${PROJECT_PARENT}/.rquant-deploy/$(basename "${PROJECT_DIR}").lock}"

exec "${PYTHON_BIN}" -I -S "${PROJECT_DIR}/scripts/bootstrap-production-deploy.py" \
    --expected-checkout-root "${PROJECT_DIR}" \
    --trusted-git-path "${TRUSTED_GIT}" \
    --deployment-lock-path "${DEPLOY_LOCK}" \
    --python-path "${PYTHON_BIN}" \
    -- "$@"
