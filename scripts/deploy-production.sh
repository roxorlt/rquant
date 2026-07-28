#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RQUANT_DEPLOY_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"
UV_BIN="${RQUANT_DEPLOY_UV:-}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    printf 'Deployment Python is not executable: %s\n' "${PYTHON_BIN}" >&2
    exit 2
fi
PROJECT_PARENT="$(dirname "${PROJECT_DIR}")"
DEPLOY_LOCK="${RQUANT_DEPLOY_LOCK_PATH:-${PROJECT_PARENT}/.rquant-deploy/$(basename "${PROJECT_DIR}").lock}"

case "$(uname -s)" in
    Darwin)
        HOST_PLATFORM="darwin"
        RELEASE_PROFILE="macos-lab"
        LAB_LIFECYCLE_MODE="${RQUANT_LAB_LIFECYCLE_MODE:-}"
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

BOOTSTRAP_ARGS=(
    --expected-checkout-root "${PROJECT_DIR}"
    --deployment-lock-path "${DEPLOY_LOCK}"
    --python-path "${PYTHON_BIN}"
    --release-profile "${RELEASE_PROFILE}"
    --host-platform "${HOST_PLATFORM}"
)
if [[ -n "${RQUANT_TRUSTED_GIT_PATH:-}" ]]; then
    BOOTSTRAP_ARGS+=(--trusted-git-path "${RQUANT_TRUSTED_GIT_PATH}")
fi
if [[ -n "${UV_BIN}" ]]; then
    BOOTSTRAP_ARGS+=(--uv-path "${UV_BIN}")
fi
if [[ -n "${LAB_LIFECYCLE_MODE}" ]]; then
    BOOTSTRAP_ARGS+=(--lab-lifecycle-mode "${LAB_LIFECYCLE_MODE}")
fi
if [[ -n "${RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS:-}" ]]; then
    BOOTSTRAP_ARGS+=(--command-timeout-seconds "${RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS}")
fi
if [[ -n "${RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS:-}" ]]; then
    BOOTSTRAP_ARGS+=(--overall-timeout-seconds "${RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS}")
fi

exec "${PYTHON_BIN}" -I -S "${PROJECT_DIR}/scripts/bootstrap-production-deploy.py" \
    "${BOOTSTRAP_ARGS[@]}" \
    "$@"
