#!/usr/bin/env bash
# Install candidate-only workload lock infrastructure without changing service state.

set -Eeuo pipefail

readonly PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
unset CDPATH

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT=
FAIL_STEP=
while (( $# > 0 )); do
    case "$1" in
        --test-root)
            [[ $# -ge 2 && "$2" == /* && "$2" != / ]] || exit 2
            TEST_ROOT=$2
            shift 2
            ;;
        --fail-step)
            [[ $# -ge 2 ]] || exit 2
            FAIL_STEP=$2
            shift 2
            ;;
        *)
            printf 'unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done
if [[ -n "${FAIL_STEP}" && -z "${TEST_ROOT}" ]]; then
    printf '%s\n' '--fail-step is test-only' >&2
    exit 2
fi

PREFIX=${TEST_ROOT}
HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-workload-arbiter"
TMPFILES_SOURCE="${PROJECT_DIR}/deploy/tmpfiles.d/rquant-workload-isolation.conf"
HELPER_TARGET="${PREFIX}/usr/local/libexec/rquant-workload-arbiter"
HELPER_HASH_TARGET="${PREFIX}/usr/local/libexec/rquant-workload-arbiter.sha256"
TMPFILES_TARGET="${PREFIX}/etc/tmpfiles.d/rquant-workload-isolation.conf"
TRANSACTION_DIR=$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/rquant-workload-install.XXXXXX")
MUTATION_STARTED=0

privileged() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

inject_fault() {
    if [[ "${FAIL_STEP}" == "$1" ]]; then
        printf 'injected workload installer fault: %s\n' "$1" >&2
        return 1
    fi
}

snapshot_target() {
    local name=$1
    local target=$2
    if privileged /bin/test -e "${target}"; then
        printf '%s\n' present >"${TRANSACTION_DIR}/${name}.state"
        privileged /bin/cp -a -- "${target}" "${TRANSACTION_DIR}/${name}.backup"
    else
        printf '%s\n' absent >"${TRANSACTION_DIR}/${name}.state"
    fi
}

restore_target() {
    local name=$1
    local target=$2
    if [[ "$(<"${TRANSACTION_DIR}/${name}.state")" == present ]]; then
        privileged /bin/rm -f -- "${target}"
        privileged /bin/cp -a -- "${TRANSACTION_DIR}/${name}.backup" "${target}"
    else
        privileged /bin/rm -f -- "${target}"
    fi
}

rollback() {
    local status=$?
    trap - ERR
    set +e
    if (( MUTATION_STARTED != 0 )); then
        restore_target helper "${HELPER_TARGET}"
        restore_target helper_hash "${HELPER_HASH_TARGET}"
        restore_target tmpfiles "${TMPFILES_TARGET}"
    fi
    privileged /bin/rm -f -- "${HELPER_TARGET}.tmp.$$" \
        "${HELPER_HASH_TARGET}.tmp.$$" "${TMPFILES_TARGET}.tmp.$$"
    /bin/rm -rf -- "${TRANSACTION_DIR}"
    printf '%s\n' 'workload installer rolled back candidate files' >&2
    exit "${status}"
}
trap rollback ERR

for source in "${HELPER_SOURCE}" "${TMPFILES_SOURCE}"; do
    [[ -f "${source}" && ! -L "${source}" ]] || {
        printf 'unsafe workload installer source: %s\n' "${source}" >&2
        exit 1
    }
done
snapshot_target helper "${HELPER_TARGET}"
snapshot_target helper_hash "${HELPER_HASH_TARGET}"
snapshot_target tmpfiles "${TMPFILES_TARGET}"
if [[ -n "${TEST_ROOT}" ]]; then
    HELPER_SHA256=$(/usr/bin/shasum -a 256 "${HELPER_SOURCE}" | /usr/bin/awk '{print $1}')
else
    HELPER_SHA256=$(/usr/bin/sha256sum "${HELPER_SOURCE}" | /usr/bin/awk '{print $1}')
fi
printf '%s\n' "${HELPER_SHA256}" >"${TRANSACTION_DIR}/helper.sha256"
if [[ -n "${TEST_ROOT}" ]]; then
    /usr/bin/install -d -m 0755 "$(dirname "${HELPER_TARGET}")"
    /usr/bin/install -d -m 0755 "$(dirname "${TMPFILES_TARGET}")"
    /usr/bin/install -m 0755 "${HELPER_SOURCE}" "${HELPER_TARGET}.tmp.$$"
    /usr/bin/install -m 0444 \
        "${TRANSACTION_DIR}/helper.sha256" "${HELPER_HASH_TARGET}.tmp.$$"
    /usr/bin/install -m 0644 "${TMPFILES_SOURCE}" "${TMPFILES_TARGET}.tmp.$$"
else
    privileged /usr/bin/install -d -o root -g root -m 0755 \
        "$(dirname "${HELPER_TARGET}")"
    privileged /usr/bin/install -d -o root -g root -m 0755 \
        "$(dirname "${TMPFILES_TARGET}")"
    privileged /usr/bin/install -o root -g root -m 0755 \
        "${HELPER_SOURCE}" "${HELPER_TARGET}.tmp.$$"
    privileged /usr/bin/install -o root -g root -m 0444 \
        "${TRANSACTION_DIR}/helper.sha256" "${HELPER_HASH_TARGET}.tmp.$$"
    privileged /usr/bin/install -o root -g root -m 0644 \
        "${TMPFILES_SOURCE}" "${TMPFILES_TARGET}.tmp.$$"
fi
MUTATION_STARTED=1
privileged /bin/mv -f -- "${HELPER_TARGET}.tmp.$$" "${HELPER_TARGET}"
inject_fault helper_publish
privileged /bin/mv -f -- "${HELPER_HASH_TARGET}.tmp.$$" "${HELPER_HASH_TARGET}"
inject_fault helper_hash_publish
privileged /bin/mv -f -- "${TMPFILES_TARGET}.tmp.$$" "${TMPFILES_TARGET}"
inject_fault tmpfiles_publish

if [[ -n "${TEST_ROOT}" ]]; then
    RUN_ROOT="${TEST_ROOT}/run/rquant-workload-isolation"
    MIGRATION_ROOT="${TEST_ROOT}/var/lib/rquant/workload-isolation/migration"
    /usr/bin/install -d -m 0770 "${RUN_ROOT}" "${RUN_ROOT}/research-pids"
    /usr/bin/install -d -m 0700 "${MIGRATION_ROOT}"
    /usr/bin/touch "${MIGRATION_ROOT}/migration.lock"
    /bin/chmod 0600 "${MIGRATION_ROOT}/migration.lock"
    for name in intent.lock research-transition.lock research-active.lock \
        maintenance-active.lock; do
        /usr/bin/touch "${RUN_ROOT}/${name}"
        /bin/chmod 0660 "${RUN_ROOT}/${name}"
    done
else
    privileged /usr/bin/systemd-tmpfiles --create "${TMPFILES_TARGET}"
fi
inject_fault runtime_create

privileged /bin/test -x "${HELPER_TARGET}"
privileged /usr/bin/cmp -s "${HELPER_SOURCE}" "${HELPER_TARGET}"
observed_hash=$(privileged /usr/bin/awk 'NR==1{print $1}' "${HELPER_HASH_TARGET}")
[[ "${observed_hash}" == "${HELPER_SHA256}" ]]
privileged /usr/bin/cmp -s "${TMPFILES_SOURCE}" "${TMPFILES_TARGET}"
trap - ERR
/bin/rm -rf -- "${TRANSACTION_DIR}"
printf '%s\n' 'installed workload candidate lock infrastructure; no unit state changed'
