#!/usr/bin/env bash
# Run the v0.25.1 suspension refresh and three-strategy Stage 1 acceptance.

set -Eeuo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_TAG="v0.25.1"
EXPECTED_SHA="${RQUANT_STAGE1_EXPECTED_SHA:-}"
START_DATE="${RQUANT_STAGE1_START_DATE:-2026-04-01}"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"
RUN_STAMP="$(TZ=Asia/Shanghai date +%Y%m%dT%H%M%S)"
RUN_DIR="${PROJECT_DIR}/logs/stage1-v0.25.1-${RUN_STAMP}"
RUN_LOG="${RUN_DIR}/rollout.log"
STRATEGIES=("n_shape" "growth_board_surge" "auction_gap")
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

declare -A MANIFEST_IDS
declare -A EFFECTIVE_END_DATES
declare -A SNAPSHOT_IDS
declare -A BINDING_HASHES

restore_original_timers() {
    if (( ! TIMERS_CAPTURED || TIMERS_RESTORED )); then
        return
    fi
    if (( ${#ORIGINALLY_ACTIVE_TIMERS[@]} )); then
        sudo systemctl start "${ORIGINALLY_ACTIVE_TIMERS[@]}"
    fi
    TIMERS_RESTORED=1
}

on_exit() {
    local rc=$?
    trap - EXIT
    trap '' HUP INT TERM
    set +e
    if ! restore_original_timers; then
        echo "timer restore failed; inspect systemctl immediately" >&2
        rc=1
    fi
    if (( rc != 0 )); then
        echo "v0.25.1 Stage 1 rollout failed (exit=${rc}); original timers restored" >&2
        echo "ROLLOUT_LOG=${RUN_LOG}" >&2
    fi
    exit "${rc}"
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

assert_exact_release() {
    local actual_tag actual_sha
    if [[ ! "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]]; then
        echo "RQUANT_STAGE1_EXPECTED_SHA must be the exact 40-character release SHA" >&2
        return 1
    fi
    actual_tag="$(git describe --tags --exact-match)"
    actual_sha="$(git rev-parse HEAD)"
    [[ "${actual_tag}" == "${TARGET_TAG}" ]]
    [[ "${actual_sha}" == "${EXPECTED_SHA}" ]]
    [[ -z "$(git status --porcelain --untracked-files=normal)" ]]
    export RQUANT_CODE_COMMIT="${EXPECTED_SHA}"
    echo "TARGET_TAG=${actual_tag}"
    echo "TARGET_SHA=${actual_sha}"
}

capture_active_timers() {
    local timer
    for timer in "${MUTATING_TIMERS[@]}"; do
        if systemctl is-active --quiet "${timer}"; then
            ORIGINALLY_ACTIVE_TIMERS+=("${timer}")
        fi
    done
    TIMERS_CAPTURED=1
    printf 'ORIGINALLY_ACTIVE_TIMERS=%s\n' "${ORIGINALLY_ACTIVE_TIMERS[*]:-none}"
}

stop_mutating_units() {
    capture_active_timers
    sudo systemctl stop "${MUTATING_TIMERS[@]}"
    sudo systemctl stop "${MUTATING_SERVICES[@]}"
    local service
    for service in "${MUTATING_SERVICES[@]}"; do
        if systemctl is-active --quiet "${service}"; then
            echo "mutating service remained active after stop: ${service}" >&2
            return 1
        fi
    done
}

run_json() {
    local output=$1
    shift
    echo "RUN_JSON=${output}"
    "$@" > "${output}"
    "${PYTHON_BIN}" -m json.tool "${output}"
}

json_value() {
    local input=$1
    local dotted_path=$2
    "${PYTHON_BIN}" - "${input}" "${dotted_path}" <<'PY'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for key in sys.argv[2].split("."):
    value = value[key]
print(value)
PY
}

require_json_value() {
    local input=$1
    local dotted_path=$2
    local expected=$3
    local actual
    actual="$(json_value "${input}" "${dotted_path}")"
    if [[ "${actual}" != "${expected}" ]]; then
        echo "${input}: expected ${dotted_path}=${expected}, got ${actual}" >&2
        return 1
    fi
}

verify_backup() {
    local started_epoch=$1
    "${PYTHON_BIN}" - "${PROJECT_DIR}/backup/latest.json" "${started_epoch}" <<'PY'
import json
import sys
from datetime import datetime
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
started_epoch = int(sys.argv[2])
snapshot_epoch = int(datetime.fromisoformat(metadata["snapshot_at"]).timestamp())
assert metadata["source"] == "main"
assert metadata["source_lag_seconds"] == 0
assert metadata["verified"] is True
assert metadata["table_count"] > 0
assert snapshot_epoch >= started_epoch
print("BACKUP_METADATA", metadata)
PY
}

preserve_backup() {
    local label=$1
    local destination
    destination="${PROJECT_DIR}/backup/${label}-${RUN_STAMP}"
    cp -- "${PROJECT_DIR}/backup/latest.duckdb.gz" "${destination}.duckdb.gz"
    cp -- "${PROJECT_DIR}/backup/latest.json" "${destination}.json"
    echo "PRESERVED_BACKUP=${destination}"
}

backup_main() {
    local label=$1
    local started_epoch
    started_epoch="$(date -u +%s)"
    RQUANT_BACKUP_SOURCE=main \
        RQUANT_BACKUP_PROJECT_DIR="${PROJECT_DIR}" \
        "${PROJECT_DIR}/scripts/backup-snapshot.sh"
    verify_backup "${started_epoch}"
    preserve_backup "${label}"
}

resolve_refresh_end() {
    "${PYTHON_BIN}" - "${PROJECT_DIR}/data/rquant.duckdb" <<'PY'
import sys
from datetime import datetime, time
from zoneinfo import ZoneInfo

import duckdb

now = datetime.now(ZoneInfo("Asia/Shanghai"))
cutoff = now.date()
if now.weekday() < 5 and now.time() <= time(15, 10):
    cutoff = cutoff.fromordinal(cutoff.toordinal() - 1)
conn = duckdb.connect(sys.argv[1], read_only=True)
row = conn.execute(
    """
    SELECT max(calendar.cal_date)
    FROM trade_calendar AS calendar
    WHERE calendar.exchange = 'SSE'
      AND calendar.is_open
      AND calendar.cal_date <= ?
      AND EXISTS (
          SELECT 1
          FROM daily_bar AS daily
          WHERE daily.trade_date = calendar.cal_date
      )
    """,
    [cutoff],
).fetchone()
conn.close()
if row is None or row[0] is None:
    raise RuntimeError("no completed authoritative open session is available")
print(row[0].isoformat())
PY
}

verify_suspension_preview() {
    "${PYTHON_BIN}" - "$1" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert payload["missing_only"] is False
assert payload["open_dates"]
assert payload["requested_dates"] == payload["open_dates"]
print("SUSPENSION_PREVIEW_DATES", len(payload["requested_dates"]))
PY
}

verify_suspension_apply() {
    "${PYTHON_BIN}" - "$1" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert payload["open_date_count"] > 0
assert payload["requested_date_count"] == payload["open_date_count"]
assert payload["persisted_date_count"] == payload["requested_date_count"]
print("SUSPENSION_PERSISTED_DATES", payload["persisted_date_count"])
PY
}

sync_operational_replica() {
    "${PROJECT_DIR}/scripts/sync-readonly-replica.sh"
}

verify_main_replica() {
    "${PYTHON_BIN}" - \
        "${PROJECT_DIR}/data/rquant.duckdb" \
        "${PROJECT_DIR}/data/rquant_ro.duckdb" \
        "${START_DATE}" \
        "${REFRESH_END}" <<'PY'
import sys
from datetime import date

import duckdb

start = date.fromisoformat(sys.argv[3])
end = date.fromisoformat(sys.argv[4])

def summary(path: str) -> tuple[object, ...]:
    conn = duckdb.connect(path, read_only=True)
    row = conn.execute(
        """
        SELECT
            (SELECT max(version) FROM schema_migration),
            (SELECT count(*) FROM stock_suspend_coverage
             WHERE source = 'tushare'
               AND trade_date BETWEEN ? AND ?),
            (SELECT count(*) FROM stock_suspend_coverage
             WHERE source = 'tushare'
               AND coverage_state <> 'complete'
               AND trade_date BETWEEN ? AND ?),
            (SELECT bit_xor(hash(
                source, trade_date, coverage_state, row_count, snapshot_hash
             )) FROM stock_suspend_coverage
             WHERE source = 'tushare'
               AND trade_date BETWEEN ? AND ?),
            (SELECT count(*) FROM dataset_snapshot WHERE status = 'ready'),
            (SELECT count(*) FROM data_audit_run WHERE status = 'completed')
        """,
        [start, end, start, end, start, end],
    ).fetchone()
    open_count = conn.execute(
        """
        SELECT count(*)
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open AND cal_date BETWEEN ? AND ?
        """,
        [start, end],
    ).fetchone()[0]
    conn.close()
    assert row[0] == 10
    assert row[1] == open_count
    assert row[2] == 0
    return (*row, open_count)

main = summary(sys.argv[1])
replica = summary(sys.argv[2])
print("MAIN_SUMMARY", main)
print("REPLICA_SUMMARY", replica)
assert main == replica
PY
}

trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

assert_outside_market_window
cd "${PROJECT_DIR}"
assert_exact_release
[[ -x "${RQUANT_BIN}" ]]
[[ -x "${PYTHON_BIN}" ]]
mkdir -p "${RUN_DIR}"
exec > >(tee -a "${RUN_LOG}") 2>&1
echo "TARGET_TAG=${TARGET_TAG}"
echo "TARGET_SHA=${EXPECTED_SHA}"

stop_mutating_units
backup_main "v0.25.1-pre-stage1"

REFRESH_END="$(
    resolve_refresh_end
)"
echo "START_DATE=${START_DATE}"
echo "REFRESH_END=${REFRESH_END}"

SUSPENSION_PREVIEW="${RUN_DIR}/suspension-preview.json"
SUSPENSION_APPLY="${RUN_DIR}/suspension-apply.json"
run_json "${SUSPENSION_PREVIEW}" "${RQUANT_BIN}" suspension-backfill --start-date "${START_DATE}" --end-date "${REFRESH_END}" --full-refresh --dry-run
verify_suspension_preview "${SUSPENSION_PREVIEW}"
run_json "${SUSPENSION_APPLY}" "${RQUANT_BIN}" suspension-backfill --start-date "${START_DATE}" --end-date "${REFRESH_END}" --full-refresh
verify_suspension_apply "${SUSPENSION_APPLY}"
sync_operational_replica

for strategy in "${STRATEGIES[@]}"; do
    plan_file="${RUN_DIR}/${strategy}-backfill-plan.json"
    run_file="${RUN_DIR}/${strategy}-backfill-run.json"
    status_file="${RUN_DIR}/${strategy}-backfill-status.json"
    repair_preview_file="${RUN_DIR}/${strategy}-minute-repair-preview.json"

    run_json "${plan_file}" "${RQUANT_BIN}" backfill-plan --strategy "${strategy}" --start-date "${START_DATE}"
    MANIFEST_IDS["${strategy}"]="$(json_value "${plan_file}" manifest_id)"
    EFFECTIVE_END_DATES["${strategy}"]="$(json_value "${plan_file}" effective_end_date)"

    run_json "${run_file}" "${RQUANT_BIN}" backfill-run --manifest-id "${MANIFEST_IDS[${strategy}]}"
    run_json "${status_file}" "${RQUANT_BIN}" backfill-status --manifest-id "${MANIFEST_IDS[${strategy}]}" --json
    require_json_value "${status_file}" status completed

    sync_operational_replica
    run_json "${repair_preview_file}" "${RQUANT_BIN}" research-repair-minute --manifest-id "${MANIFEST_IDS[${strategy}]}"
    repair_status="$(json_value "${repair_preview_file}" status)"
    case "${repair_status}" in
        planned)
            repair_plan_id="$(json_value "${repair_preview_file}" plan_id)"
            repair_apply_file="${RUN_DIR}/${strategy}-minute-repair-apply.json"
            run_json "${repair_apply_file}" "${RQUANT_BIN}" research-repair-minute --manifest-id "${MANIFEST_IDS[${strategy}]}" --apply --plan-id "${repair_plan_id}"
            require_json_value "${repair_apply_file}" status candidate
            ;;
        unchanged)
            echo "MINUTE_REPAIR_UNCHANGED=${strategy}"
            ;;
        *)
            echo "unexpected minute repair preview status: ${repair_status}" >&2
            exit 1
            ;;
    esac
done

SNAPSHOT_AS_OF="$(
    "${PYTHON_BIN}" - <<'PY'
from datetime import datetime
from zoneinfo import ZoneInfo

print(datetime.now(ZoneInfo("Asia/Shanghai")).isoformat())
PY
)"
echo "SNAPSHOT_AS_OF=${SNAPSHOT_AS_OF}"

for strategy in "${STRATEGIES[@]}"; do
    snapshot_preview_file="${RUN_DIR}/${strategy}-snapshot-preview.json"
    snapshot_apply_file="${RUN_DIR}/${strategy}-snapshot-apply.json"
    run_json "${snapshot_preview_file}" "${RQUANT_BIN}" dataset-snapshot --strategy "${strategy}" --as-of "${SNAPSHOT_AS_OF}" --manifest-id "${MANIFEST_IDS[${strategy}]}" --dry-run
    require_json_value "${snapshot_preview_file}" status dry_run
    require_json_value "${snapshot_preview_file}" apply_required True
    run_json "${snapshot_apply_file}" "${RQUANT_BIN}" dataset-snapshot --strategy "${strategy}" --as-of "${SNAPSHOT_AS_OF}" --manifest-id "${MANIFEST_IDS[${strategy}]}" --apply
    require_json_value "${snapshot_apply_file}" status ready
    SNAPSHOT_IDS["${strategy}"]="$(json_value "${snapshot_apply_file}" snapshot_id)"
    BINDING_HASHES["${strategy}"]="$(json_value "${snapshot_apply_file}" binding_hash)"
done

AUDIT_FILE="${RUN_DIR}/data-audit.json"
run_json "${AUDIT_FILE}" "${RQUANT_BIN}" data-audit --start-date "${START_DATE}" --as-of "${REFRESH_END}"
require_json_value "${AUDIT_FILE}" status completed
require_json_value "${AUDIT_FILE}" p0_count 0
AUDIT_RUN_ID="$(json_value "${AUDIT_FILE}" audit_run_id)"

sync_operational_replica
for strategy in "${STRATEGIES[@]}"; do
    replay_file="${RUN_DIR}/${strategy}-formal-smoke.json"
    run_json "${replay_file}" "${RQUANT_BIN}" formal-smoke-replay --strategy "${strategy}" --start-date "${START_DATE}" --end-date "${EFFECTIVE_END_DATES[${strategy}]}" --audit-run-id "${AUDIT_RUN_ID}" --snapshot-id "${SNAPSHOT_IDS[${strategy}]}" --binding-hash "${BINDING_HASHES[${strategy}]}"
    require_json_value "${replay_file}" status comparable
    "${PYTHON_BIN}" - "${replay_file}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert payload["missing_evidence"] == []
assert payload["sample_count"] >= 0
PY
done

backup_main "v0.25.1-post-stage1"
sync_operational_replica
verify_main_replica
"${RQUANT_BIN}" research-authority-status

restore_original_timers
"${RQUANT_BIN}" preflight

echo "=== VERSION ==="
git describe --tags --exact-match
git rev-parse HEAD
echo "=== TIMERS ==="
systemctl list-timers --all 'rquant-*' --no-pager
echo "=== SERVICES ==="
systemctl show \
    rquant-monitor.service \
    rquant-surge-watch.service \
    rquant-replica-sync.service \
    rquant-research-ingest.service \
    --property=Id,ActiveState,SubState,Result,NRestarts

trap - EXIT HUP INT TERM
echo "ROLLOUT_RESULT=success"
echo "ROLLOUT_LOG=${RUN_LOG}"
