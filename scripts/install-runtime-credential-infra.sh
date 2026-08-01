#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-runtime-credential-sealer"
SUDOERS_SOURCE="${PROJECT_DIR}/deploy/sudoers/rquant-production-deploy"
TEST_ROOT=""
FAIL_STEP=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --test-root)
            TEST_ROOT="${2:?missing --test-root value}"
            shift 2
            ;;
        --fail-step)
            FAIL_STEP="${2:?missing --fail-step value}"
            shift 2
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done

if [[ -n "${FAIL_STEP}" && -z "${TEST_ROOT}" ]]; then
    printf '%s\n' '--fail-step is test-only' >&2
    exit 2
fi
if [[ -n "${TEST_ROOT}" ]]; then
    if [[ "${TEST_ROOT}" != /* || "${TEST_ROOT}" == "/" ]]; then
        printf '%s\n' '--test-root must be a non-root absolute path' >&2
        exit 2
    fi
    PREFIX="${TEST_ROOT}"
    VISUDO_BIN="/usr/bin/true"
else
    PREFIX=""
    VISUDO_BIN="/usr/sbin/visudo"
fi

HELPER_DIR="${PREFIX}/usr/local/libexec"
HELPER_TARGET="${HELPER_DIR}/rquant-runtime-credential-sealer"
SUDOERS_DIR="${PREFIX}/etc/sudoers.d"
SUDOERS_TARGET="${SUDOERS_DIR}/rquant-production-deploy"
HELPER_STAGING="${HELPER_TARGET}.tmp.$$"
SUDOERS_STAGING="${SUDOERS_TARGET}.tmp.$$"
SUDOERS_BACKUP="${SUDOERS_TARGET}.backup"

cleanup() {
    local status=$?
    if [[ -n "${TEST_ROOT}" ]]; then
        /bin/rm -f "${HELPER_STAGING}" "${SUDOERS_STAGING}"
    else
        sudo /bin/rm -f "${HELPER_STAGING}" "${SUDOERS_STAGING}" || true
    fi
    return "${status}"
}
trap cleanup EXIT

run_step() {
    local name="$1"
    shift
    if [[ ",${FAIL_STEP}," == *",${name},"* ]]; then
        printf 'Injected failure: %s\n' "${name}" >&2
        return 97
    fi
    "$@"
}

privileged() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

install_helper_directory() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -d -m 0755 "$1"
    else
        sudo /usr/bin/install -d -o root -g root -m 0755 "$1"
    fi
}

path_exists() {
    privileged /bin/test -e "$1"
}

ensure_sudoers_directory() {
    if ! path_exists "${SUDOERS_DIR}"; then
        if [[ -n "${TEST_ROOT}" ]]; then
            /usr/bin/install -d -m 0750 "${SUDOERS_DIR}"
        else
            sudo /usr/bin/install -d -o root -g root -m 0750 "${SUDOERS_DIR}"
        fi
        return
    fi

    local actual expected_owner mode
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(/usr/bin/stat -f '%u:%g:%Lp' "${SUDOERS_DIR}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${SUDOERS_DIR}")"
        expected_owner="0:0"
    fi
    mode="${actual##*:}"
    if [[ "${actual%:*}" != "${expected_owner}" || ( "${mode}" != "750" && "${mode}" != "700" ) ]]; then
        printf 'Unsafe sudoers directory state: %s (expected owner %s and mode 750 or 700)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

recover_stale_sudoers_backup() {
    if ! path_exists "${SUDOERS_BACKUP}"; then
        return
    fi
    if ! privileged "${VISUDO_BIN}" -cf "${SUDOERS_BACKUP}"; then
        printf 'Preserved invalid sudoers backup for manual recovery: %s\n' \
            "${SUDOERS_BACKUP}" >&2
        exit 1
    fi
    privileged /bin/mv -f "${SUDOERS_BACKUP}" "${SUDOERS_TARGET}"
}

install_file() {
    local mode="$1"
    local source="$2"
    local target="$3"
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -m "${mode}" "${source}" "${target}"
    else
        sudo /usr/bin/install -o root -g root -m "${mode}" "${source}" "${target}"
    fi
}

run_step libexec_dir install_helper_directory "${HELPER_DIR}"
ensure_sudoers_directory
recover_stale_sudoers_backup
run_step helper_install install_file 0755 "${HELPER_SOURCE}" "${HELPER_STAGING}"
run_step helper_publish privileged /bin/mv -f "${HELPER_STAGING}" "${HELPER_TARGET}"
run_step sudoers_install install_file 0440 "${SUDOERS_SOURCE}" "${SUDOERS_STAGING}"
run_step sudoers_validate_staging privileged "${VISUDO_BIN}" -cf "${SUDOERS_STAGING}"

if path_exists "${SUDOERS_TARGET}"; then
    privileged /bin/cp -p "${SUDOERS_TARGET}" "${SUDOERS_BACKUP}"
fi
run_step sudoers_publish privileged /bin/mv -f "${SUDOERS_STAGING}" "${SUDOERS_TARGET}"
if ! run_step sudoers_validate_final privileged "${VISUDO_BIN}" -cf "${SUDOERS_TARGET}"; then
    if path_exists "${SUDOERS_BACKUP}"; then
        if ! run_step sudoers_restore privileged /bin/mv -f \
            "${SUDOERS_BACKUP}" "${SUDOERS_TARGET}"; then
            printf 'Sudoers restore failed; preserved backup: %s\n' \
                "${SUDOERS_BACKUP}" >&2
            exit 1
        fi
        if ! privileged "${VISUDO_BIN}" -cf "${SUDOERS_TARGET}"; then
            printf 'Restored sudoers file failed validation: %s\n' \
                "${SUDOERS_TARGET}" >&2
            exit 1
        fi
    else
        privileged /bin/rm -f "${SUDOERS_TARGET}"
    fi
    exit 1
fi
privileged /bin/rm -f "${SUDOERS_BACKUP}"

printf 'Runtime credential infrastructure installed and validated.\n'
