#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RQUANT_DEPLOY_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"
UV_BIN="${RQUANT_DEPLOY_UV:-}"
COMMAND_TIMEOUT_SECONDS="${RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS:-300}"
OVERALL_TIMEOUT_SECONDS="${RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS:-1800}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    printf 'Deployment Python is not executable: %s\n' "${PYTHON_BIN}" >&2
    exit 2
fi
TRUSTED_GIT="${RQUANT_TRUSTED_GIT_PATH:-/usr/bin/git}"
PROJECT_PARENT="$(dirname "${PROJECT_DIR}")"
DEPLOY_LOCK="${RQUANT_DEPLOY_LOCK_PATH:-${PROJECT_PARENT}/.rquant-deploy/$(basename "${PROJECT_DIR}").lock}"

case "$(uname -s)" in
    Darwin)
        HOST_PLATFORM="darwin"
        RELEASE_PROFILE="macos-lab"
        LAB_LIFECYCLE_MODE="${RQUANT_LAB_LIFECYCLE_MODE:-installed}"
        ;;
    Linux)
        HOST_PLATFORM="linux"
        RELEASE_PROFILE="linux-production"
        LAB_LIFECYCLE_MODE="uninstalled"
        ;;
    *)
        printf 'Unsupported deployment platform\n' >&2
        exit 2
        ;;
esac
if [[ -n "${RQUANT_RELEASE_PROFILE:-}" && "${RQUANT_RELEASE_PROFILE}" != "${RELEASE_PROFILE}" ]]; then
    printf 'Release profile does not match host platform: %s\n' "${RQUANT_RELEASE_PROFILE}" >&2
    exit 2
fi

exec "${PYTHON_BIN}" -I -S "${PROJECT_DIR}/scripts/bootstrap-production-deploy.py" \
    --expected-checkout-root "${PROJECT_DIR}" \
    --trusted-git-path "${TRUSTED_GIT}" \
    --deployment-lock-path "${DEPLOY_LOCK}" \
    --python-path "${PYTHON_BIN}" \
    --uv-path "${UV_BIN}" \
    --release-profile "${RELEASE_PROFILE}" \
    --host-platform "${HOST_PLATFORM}" \
    --lab-lifecycle-mode "${LAB_LIFECYCLE_MODE}" \
    --command-timeout-seconds "${COMMAND_TIMEOUT_SECONDS}" \
    --overall-timeout-seconds "${OVERALL_TIMEOUT_SECONDS}" \
    "$@"
