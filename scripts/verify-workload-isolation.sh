#!/usr/bin/env bash
# Cloud-only, read-only workload acceptance. This script never changes unit state.
set -euo pipefail

readonly PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
readonly ROOT=/home/lighthouse/rquant
readonly UNIT_DIR=/etc/systemd/system
readonly PYTHON=/home/lighthouse/rquant/.venv/bin/python
readonly SYSTEMCTL=/usr/bin/systemctl
readonly SYSTEMD_ANALYZE=/usr/bin/systemd-analyze
readonly AWK=/usr/bin/awk
readonly STAT=/usr/bin/stat
readonly SHA256SUM=/usr/bin/sha256sum
readonly CAT=/usr/bin/cat
readonly ARBITER=/usr/local/libexec/rquant-workload-arbiter
readonly ARBITER_HASH=/usr/local/libexec/rquant-workload-arbiter.sha256
readonly ARBITER_ROOT=/run/rquant-workload-isolation
readonly MIGRATION_ROOT=/var/lib/rquant/workload-isolation/migration
readonly MIGRATION_LOCK=/var/lib/rquant/workload-isolation/migration/migration.lock
unset CDPATH PYTHONHOME PYTHONPATH

if [[ "$(/usr/bin/uname -s)" != "Linux" ]]; then
    printf '%s\n' 'workload isolation cloud gate requires the original Linux systemd host' >&2
    exit 2
fi
for required in "${SYSTEMCTL}" "${SYSTEMD_ANALYZE}" "${AWK}" "${STAT}" \
    "${SHA256SUM}" "${CAT}" "${PYTHON}"; do
    if [[ ! -x "${required}" ]]; then
        printf 'required fixed executable is unavailable: %s\n' "${required}" >&2
        exit 2
    fi
done
if [[ ! -d "${ROOT}" || ! -d "${UNIT_DIR}" ]]; then
    printf '%s\n' 'fixed production checkout or systemd unit directory is unavailable' >&2
    exit 2
fi

assert_root_mode() {
    local path=$1
    local expected_mode=$2
    local actual
    if [[ ! -e "${path}" || -L "${path}" ]]; then
        printf 'required fixed workload path is missing or symlinked: %s\n' "${path}" >&2
        exit 2
    fi
    actual=$("${STAT}" -c '%u:%a' "${path}")
    if [[ "${actual}" != "0:${expected_mode}" ]]; then
        printf 'unsafe workload path metadata: %s=%s expected root:%s\n' \
            "${path}" "${actual}" "${expected_mode}" >&2
        exit 2
    fi
}

assert_root_mode "${ARBITER}" 755
assert_root_mode "${ARBITER_HASH}" 444
declared_arbiter_hash=$("${CAT}" "${ARBITER_HASH}")
observed_arbiter_hash=$("${SHA256SUM}" "${ARBITER}" | "${AWK}" '{print $1}')
if [[ ! "${declared_arbiter_hash}" =~ ^[0-9a-f]{64}$ || \
      "${declared_arbiter_hash}" != "${observed_arbiter_hash}" ]]; then
    printf '%s\n' 'fixed workload arbiter sha256 provenance mismatch' >&2
    exit 2
fi
assert_root_mode "${ARBITER_ROOT}" 770
assert_root_mode "${ARBITER_ROOT}/research-pids" 770
for lock_name in intent.lock research-transition.lock research-active.lock \
    maintenance-active.lock; do
    assert_root_mode "${ARBITER_ROOT}/${lock_name}" 660
done
assert_root_mode "${MIGRATION_ROOT}" 700
assert_root_mode "${MIGRATION_LOCK}" 600
if [[ ! -f "${MIGRATION_LOCK}" || \
      "$("${STAT}" -c '%h' "${MIGRATION_LOCK}")" != 1 ]]; then
    printf '%s\n' 'migration lock must be a single-link regular file' >&2
    exit 2
fi

shopt -s nullglob
unit_files=(
    "${UNIT_DIR}"/rquant-*.service
    "${UNIT_DIR}"/rquant-*.timer
    "${UNIT_DIR}"/rquant-*.socket
    "${UNIT_DIR}"/rquant-*.slice
)
timer_files=("${UNIT_DIR}"/rquant-*.timer)
shopt -u nullglob
if (( ${#unit_files[@]} == 0 )); then
    printf 'no installed rQuant units found in %s\n' "${UNIT_DIR}" >&2
    exit 2
fi

# These are the original cloud parsers. A macOS fixture cannot satisfy this gate.
/usr/bin/systemd-analyze verify "${unit_files[@]}"
for timer in "${timer_files[@]}"; do
    while IFS= read -r calendar; do
        [[ -n "${calendar}" ]] || continue
        /usr/bin/systemd-analyze calendar "${calendar}" --iterations 5
    done < <(/usr/bin/awk -F= '/^OnCalendar=/{print $2}' "${timer}")
done

cd "${ROOT}"
"${PYTHON}" -I - "${UNIT_DIR}" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

from rquant.workload_isolation import (
    check_workload_capacity_baseline,
    check_workload_high_water_evidence,
    check_workload_runtime,
    verify_workload_unit_declarations,
)

checks = (
    verify_workload_unit_declarations(Path(sys.argv[1])),
    check_workload_runtime(
        systemctl_path=Path("/usr/bin/systemctl"),
        strict=True,
    ),
    check_workload_capacity_baseline(strict=True),
    check_workload_high_water_evidence(strict=True),
)
for check in checks:
    print(f"{check.status.upper()} {check.name}: {check.summary}")
    print("\n".join(check.details))

if any(check.status != "ok" for check in checks):
    raise SystemExit(1)
PY
