#!/usr/bin/env bash
# Run one strategy's Stage 1 evidence chain without consulting other manifests.

set -Eeuo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RQUANT_BIN="${RQUANT_STAGE1_RQUANT_BIN:-${PROJECT_DIR}/.venv/bin/rquant}"
PYTHON_BIN="${RQUANT_STAGE1_PYTHON_BIN:-${PROJECT_DIR}/.venv/bin/python}"
STRATEGY=""
MANIFEST_ID=""
START_DATE=""
END_DATE=""
EXPECTED_SHA=""
APPLY=0
MUTATING_TIMERS=(
    "rquant-daily.timer"
    "rquant-monitor-watchdog.timer"
    "rquant-monitor.timer"
    "rquant-surge-watch.timer"
    "rquant-replica-sync.timer"
    "rquant-backup.timer"
    "rquant-kpl-snapshot.timer"
    "rquant-research-ingest.timer"
)
MUTATING_SERVICES=(
    "rquant-daily.service"
    "rquant-monitor-watchdog.service"
    "rquant-monitor.service"
    "rquant-surge-watch.service"
    "rquant-replica-sync.service"
    "rquant-backup.service"
    "rquant-kpl-snapshot.service"
    "rquant-research-ingest.service"
)
ORIGINALLY_ACTIVE_TIMERS=()
TIMERS_CAPTURED=0
TIMERS_RESTORED=0
ROLLOUT_HARD_DEADLINE_EPOCH=""

usage() {
    cat <<'EOF'
Usage: run-stage1-strategy-acceptance.sh \
  --strategy <n_shape|growth_board_surge|auction_gap> \
  --manifest-id <sha256> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> \
  --expected-code-commit <40-char-sha> [--apply]
EOF
}

while (( $# )); do
    case "$1" in
        --strategy) STRATEGY="$2"; shift 2 ;;
        --manifest-id) MANIFEST_ID="$2"; shift 2 ;;
        --start-date) START_DATE="$2"; shift 2 ;;
        --end-date) END_DATE="$2"; shift 2 ;;
        --expected-code-commit) EXPECTED_SHA="$2"; shift 2 ;;
        --apply) APPLY=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ ! "${STRATEGY}" =~ ^(n_shape|growth_board_surge|auction_gap)$ ]]; then
    echo "invalid or missing --strategy" >&2
    exit 2
fi
if [[ ! "${MANIFEST_ID}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid or missing --manifest-id" >&2
    exit 2
fi
if [[ ! "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid or missing --expected-code-commit" >&2
    exit 2
fi
if [[ -z "${START_DATE}" || -z "${END_DATE}" ]]; then
    echo "start and end dates are required" >&2
    exit 2
fi

RUN_DIR="${PROJECT_DIR}/logs/stage1-acceptance/${STRATEGY}-${MANIFEST_ID}"
RUN_LOG="${RUN_DIR}/acceptance.log"
PLAN_FILE="${RUN_DIR}/acceptance-plan.json"
mkdir -p "${RUN_DIR}"
exec > >(tee -a "${RUN_LOG}") 2>&1

json_value() {
    local path=$1
    local key=$2
    "${PYTHON_BIN}" - "${path}" "${key}" <<'PY'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("null")
else:
    print(value)
PY
}

run_json() {
    local output=$1
    shift
    "$@" > "${output}"
}

restore_original_timers() {
    if (( ! TIMERS_CAPTURED || TIMERS_RESTORED )); then
        return
    fi
    if (( ${#ORIGINALLY_ACTIVE_TIMERS[@]} )); then
        sudo systemctl start "${ORIGINALLY_ACTIVE_TIMERS[@]}"
        local timer
        for timer in "${ORIGINALLY_ACTIVE_TIMERS[@]}"; do
            systemctl is-active --quiet "${timer}"
        done
    fi
    TIMERS_RESTORED=1
}

on_exit() {
    local rc=$?
    trap - EXIT
    set +e
    if ! restore_original_timers; then
        echo "timer restore failed; inspect systemctl immediately" >&2
        rc=1
    fi
    if (( rc != 0 )); then
        echo "Stage 1 strategy acceptance failed at first error (exit=${rc})" >&2
        echo "ROLLOUT_LOG=${RUN_LOG}" >&2
    fi
    exit "${rc}"
}

assert_exact_commit() {
    local actual_sha
    actual_sha="$(git rev-parse HEAD)"
    [[ "${actual_sha}" == "${EXPECTED_SHA}" ]]
    [[ -z "$(git status --porcelain --untracked-files=normal)" ]]
    export RQUANT_CODE_COMMIT="${EXPECTED_SHA}"
}

assert_outside_market_window() {
    local weekday hour_minute
    weekday="$(TZ=Asia/Shanghai date +%u)"
    hour_minute="$((10#$(TZ=Asia/Shanghai date +%H%M)))"
    if (( weekday <= 5 && hour_minute >= 915 && hour_minute <= 1510 )); then
        echo "refusing production mutation during 09:15-15:10 Asia/Shanghai" >&2
        return 1
    fi
}

configure_rollout_hard_deadline() {
    ROLLOUT_HARD_DEADLINE_EPOCH="$(
        "${PYTHON_BIN}" - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

zone = ZoneInfo("Asia/Shanghai")
now = datetime.now(zone)
candidate = now.date()
deadline = datetime.combine(candidate, time(9, 10), tzinfo=zone)
if now >= deadline:
    candidate += timedelta(days=1)
while candidate.weekday() >= 5:
    candidate += timedelta(days=1)
print(int(datetime.combine(candidate, time(9, 10), tzinfo=zone).timestamp()))
PY
    )"
    echo "ROLLOUT_HARD_DEADLINE_EPOCH=${ROLLOUT_HARD_DEADLINE_EPOCH} (next weekday 09:10)"
}

run_guarded() {
    local remaining
    remaining=$((ROLLOUT_HARD_DEADLINE_EPOCH - $(date +%s)))
    if (( remaining <= 0 )); then
        echo "Stage 1 acceptance hard deadline already elapsed" >&2
        return 124
    fi
    timeout --signal=TERM --kill-after=30s "${remaining}s" "$@"
}

capture_and_stop_mutating_units() {
    local timer service
    for timer in "${MUTATING_TIMERS[@]}"; do
        if systemctl is-active --quiet "${timer}"; then
            ORIGINALLY_ACTIVE_TIMERS+=("${timer}")
        fi
    done
    TIMERS_CAPTURED=1
    run_guarded sudo systemctl stop "${MUTATING_TIMERS[@]}"
    for service in "${MUTATING_SERVICES[@]}"; do
        if systemctl is-active --quiet "${service}"; then
            echo "refusing acceptance while mutating service is active: ${service}" >&2
            return 1
        fi
    done
}

assert_exact_commit
run_json "${PLAN_FILE}" \
    "${RQUANT_BIN}" stage1-acceptance \
    --strategy "${STRATEGY}" \
    --manifest-id "${MANIFEST_ID}" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --expected-code-commit "${EXPECTED_SHA}"

DISPOSITION="$(json_value "${PLAN_FILE}" disposition)"
if [[ "${DISPOSITION}" == "retired" ]]; then
    echo "ROLLOUT_RESULT=retired"
    echo "ROLLOUT_EVIDENCE=${RUN_DIR}"
    exit 0
fi
if [[ "${DISPOSITION}" != "ready" ]]; then
    echo "selected strategy is not ready: ${DISPOSITION}" >&2
    exit 1
fi

REPAIR_PREVIEW="${RUN_DIR}/minute-repair-preview.json"
run_json "${REPAIR_PREVIEW}" \
    "${RQUANT_BIN}" research-repair-minute --manifest-id "${MANIFEST_ID}"

if (( ! APPLY )); then
    echo "ROLLOUT_RESULT=dry_run"
    echo "ROLLOUT_EVIDENCE=${RUN_DIR}"
    exit 0
fi

assert_outside_market_window
command -v timeout >/dev/null
configure_rollout_hard_deadline
trap on_exit EXIT
capture_and_stop_mutating_units

REPAIR_STATUS="$(json_value "${REPAIR_PREVIEW}" status)"
case "${REPAIR_STATUS}" in
    planned)
        REPAIR_PLAN_ID="$(json_value "${REPAIR_PREVIEW}" plan_id)"
        run_guarded "${RQUANT_BIN}" research-repair-minute \
            --manifest-id "${MANIFEST_ID}" --plan-id "${REPAIR_PLAN_ID}" --apply \
            > "${RUN_DIR}/minute-repair-apply.json"
        ;;
    unchanged) ;;
    *) echo "unexpected minute repair status: ${REPAIR_STATUS}" >&2; exit 1 ;;
esac

run_guarded "${PROJECT_DIR}/scripts/sync-readonly-replica.sh"
SNAPSHOT_AS_OF_FILE="${RUN_DIR}/snapshot-as-of.txt"
if [[ -s "${SNAPSHOT_AS_OF_FILE}" ]]; then
    SNAPSHOT_AS_OF="$(<"${SNAPSHOT_AS_OF_FILE}")"
else
    SNAPSHOT_AS_OF="$(
        TZ=Asia/Shanghai "${PYTHON_BIN}" - <<'PY'
from datetime import datetime
from zoneinfo import ZoneInfo

print(datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"))
PY
    )"
fi
SNAPSHOT_PREVIEW="${RUN_DIR}/snapshot-preview.json"
run_guarded "${RQUANT_BIN}" dataset-snapshot \
    --strategy "${STRATEGY}" --as-of "${SNAPSHOT_AS_OF}" \
    --manifest-id "${MANIFEST_ID}" --dry-run > "${SNAPSHOT_PREVIEW}"
if [[ ! -s "${SNAPSHOT_AS_OF_FILE}" ]]; then
    printf '%s\n' "${SNAPSHOT_AS_OF}" > "${SNAPSHOT_AS_OF_FILE}"
fi
SNAPSHOT_APPLY="${RUN_DIR}/snapshot-apply.json"
run_guarded "${RQUANT_BIN}" dataset-snapshot \
    --strategy "${STRATEGY}" --as-of "${SNAPSHOT_AS_OF}" \
    --manifest-id "${MANIFEST_ID}" --apply > "${SNAPSHOT_APPLY}"
SNAPSHOT_ID="$(json_value "${SNAPSHOT_APPLY}" snapshot_id)"
BINDING_HASH="$(json_value "${SNAPSHOT_APPLY}" binding_hash)"

AUDIT_FILE="${RUN_DIR}/data-audit.json"
run_guarded "${RQUANT_BIN}" data-audit \
    --start-date "${START_DATE}" --as-of "${END_DATE}" > "${AUDIT_FILE}"
[[ "$(json_value "${AUDIT_FILE}" status)" == "completed" ]]
[[ "$(json_value "${AUDIT_FILE}" p0_count)" == "0" ]]
AUDIT_RUN_ID="$(json_value "${AUDIT_FILE}" audit_run_id)"

run_guarded "${PROJECT_DIR}/scripts/sync-readonly-replica.sh"
run_guarded "${RQUANT_BIN}" formal-smoke-replay \
    --strategy "${STRATEGY}" --start-date "${START_DATE}" --end-date "${END_DATE}" \
    --audit-run-id "${AUDIT_RUN_ID}" --snapshot-id "${SNAPSHOT_ID}" \
    --binding-hash "${BINDING_HASH}" > "${RUN_DIR}/formal-smoke.json"
[[ "$(json_value "${RUN_DIR}/formal-smoke.json" status)" == "comparable" ]]

run_guarded "${PROJECT_DIR}/scripts/sync-readonly-replica.sh"
restore_original_timers
"${RQUANT_BIN}" preflight

trap - EXIT
echo "ROLLOUT_RESULT=success"
echo "ROLLOUT_EVIDENCE=${RUN_DIR}"
