#!/usr/bin/env bash
# Produce one verified, WAL-free DuckDB snapshot for the backup API.

set -Eeuo pipefail

SCRIPT_PROJECT_DIR="$(cd -- "$(dirname -- "$0")/.." && pwd)"
if [[ "${RQUANT_WORKLOAD_ARBITER_HELD:-}" != "maintenance" ]]; then
    exec /usr/local/libexec/rquant-workload-arbiter maintenance -- "$0" "$@"
fi
PROJECT_DIR="${RQUANT_BACKUP_PROJECT_DIR:-${SCRIPT_PROJECT_DIR}}"
MAIN_FILE="${PROJECT_DIR}/data/rquant.duckdb"
REPLICA_FILE="${PROJECT_DIR}/data/rquant_ro.duckdb"
BACKUP_DIR="${PROJECT_DIR}/backup"
LOG="${PROJECT_DIR}/logs/backup-snapshot.log"
VENV_PY="${PROJECT_DIR}/.venv/bin/python"
RECOVERY_CLI="${RQUANT_RECOVERY_CLI:-${PROJECT_DIR}/.venv/bin/rquant}"
RECOVERY_MODE="${RQUANT_RECOVERY_BACKUP_ENABLED:-auto}"
RUNTIME_ROOT="${RQUANT_RUNTIME_ROOT:-${PROJECT_DIR}/data/runtime}"
RECOVERY_CONFIG_ASSERTION="${RQUANT_RECOVERY_BACKUP_CONFIG:-}"
RECOVERY_CREDENTIAL_ASSERTION="${RQUANT_RECOVERY_CREDENTIAL_FILE:-}"
RECOVERY_PROFILE_GENERATION_ASSERTION="${RQUANT_RECOVERY_PROFILE_GENERATION:-}"
RECOVERY_SIGNER_KEY_ID_ASSERTION="${RQUANT_RECOVERY_SIGNER_KEY_ID:-}"
SOURCE_MODE="${RQUANT_BACKUP_SOURCE:-replica}"
MAX_SOURCE_LAG_SECONDS="${RQUANT_BACKUP_MAX_SOURCE_LAG_SECONDS:-720}"
REPLICA_WAIT_SECONDS="${RQUANT_BACKUP_REPLICA_WAIT_SECONDS:-60}"
TMP_DB="${BACKUP_DIR}/.latest.duckdb.${$}"
TMP_GZ="${TMP_DB}.gz"
TMP_JSON="${BACKUP_DIR}/.latest.json.${$}"

mkdir -p "${BACKUP_DIR}" "$(dirname -- "${LOG}")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG}"; }
cleanup() { rm -f "${TMP_DB}" "${TMP_DB}.wal" "${TMP_GZ}" "${TMP_JSON}"; }
# 被信号打断时先删私有代际再写日志：备份跑在 rquant-workload-arbiter 下，arbiter 在转发
# SIGTERM 后 5s 就 SIGKILL 整个子进程组（--preempt-grace-seconds 默认 5.0），清理预算只有
# 那 5 秒。EXIT trap 在 bash 收到 SIGTERM 时其实也会跑（Linux bash 5.2 实测），但那依赖
# bash 的致命信号实现细节；显式 handler 让退出码是确定的 128+signum，并留下一条日志。
on_signal() {
    local name=$1
    local number=$2
    trap - EXIT INT TERM HUP ERR
    cleanup
    log "ABORT: SIG${name} received; removed this run's private generation"
    exit $(( 128 + number ))
}
# SIGKILL（超时后的强杀、OOM、主机重启）没有 trap 可言，只能靠下面的开机清扫兜底。
sweep_stale_generations() {
    # 只扫本脚本自己的临时命名（.latest.duckdb.<pid>[.wal|.gz]、.latest.json.<pid>），
    # 且只删 mtime 超过 1 天的；latest.duckdb.gz / latest.json 与 v*-preview-* 不在模式内。
    local swept
    swept=$(
        find "${BACKUP_DIR}" -maxdepth 1 -type f -mtime +0 \
            \( -name '.latest.duckdb.*' -o -name '.latest.json.*' \) \
            -print -delete 2>/dev/null | wc -l | tr -d ' '
    ) || swept=0
    if [[ "${swept}" != "0" ]]; then
        log "swept ${swept} stale temporary file(s) left by an earlier killed run"
    fi
}
# 低优先级前缀：cp/gzip 是常驻服务之外的批处理，不该跟盘中 monitor 抢 CPU/IO。
# 注意 cgroup v2 下跨 slice 的调度由 CPUWeight 决定，nice 只在同一 cgroup 内生效；
# 这是不花钱的兜底，真正的闸门是 deploy/systemd/rquant-*.slice 里的 CPUQuota/CPUWeight。
#
# ionice 的类别要看磁盘调度器（#243 评审 S-3）：
#   - **BFQ** 会真的兑现 idle 类（`-c3`）= 只在磁盘空闲时才给 IO。10 GB 的 cp 正是
#     IO 密集段，idle 类会把它拖慢，与「放宽超时让备份跑完」的方向相反 ⇒ 用
#     best-effort 最低优先级 `-c2 -n7`：仍然让路给盘中服务，但不会被无限期饿着。
#   - **none / mq-deadline / kyber**（云主机 virtio 的常见默认）根本不看 ionice 类别，
#     `-c3` 与 `-c2 -n7` 效果相同。
# `auto` 读 /sys/block/*/queue/scheduler 自己判；装机时可以先跑
# `cat /sys/block/vda/queue/scheduler` 核对，必要时用 RQUANT_BACKUP_IONICE 显式覆盖。
IONICE_MODE="${RQUANT_BACKUP_IONICE:-auto}"
case "${IONICE_MODE}" in
    auto|idle|best-effort|none) ;;
    *)
        log "ERROR: RQUANT_BACKUP_IONICE must be auto, idle, best-effort, or none"
        exit 2
        ;;
esac
NICE_BIN="$(command -v nice || true)"
IONICE_BIN="$(command -v ionice || true)"
IONICE_ARGS=()
if [[ -n "${IONICE_BIN}" && "${IONICE_MODE}" != "none" ]]; then
    if [[ "${IONICE_MODE}" == "auto" ]]; then
        if grep -q '\[bfq\]' /sys/block/*/queue/scheduler 2>/dev/null; then
            IONICE_MODE=best-effort
        else
            IONICE_MODE=idle
        fi
    fi
    case "${IONICE_MODE}" in
        idle) IONICE_ARGS=(-c3) ;;
        best-effort) IONICE_ARGS=(-c2 -n7) ;;
    esac
fi
low_priority() {
    if [[ -n "${NICE_BIN}" && ${#IONICE_ARGS[@]} -gt 0 ]]; then
        "${NICE_BIN}" -n 19 "${IONICE_BIN}" "${IONICE_ARGS[@]}" "$@"
    elif [[ -n "${NICE_BIN}" ]]; then
        "${NICE_BIN}" -n 19 "$@"
    else
        "$@"
    fi
}
on_error() {
    local rc=$?
    trap - ERR
    log "ERROR: snapshot failed (exit=${rc}, source=${SOURCE_MODE})" || true
    exit "${rc}"
}
file_size() { stat -c %s "$1" 2>/dev/null || stat -f %z "$1"; }
file_mtime() { stat -c %Y "$1" 2>/dev/null || stat -f %m "$1"; }
generation_mtime() {
    local path=$1
    local latest
    latest=$(file_mtime "${path}")
    if [[ -f "${path}.wal" ]]; then
        local wal_mtime
        wal_mtime=$(file_mtime "${path}.wal")
        if (( wal_mtime > latest )); then
            latest=${wal_mtime}
        fi
    fi
    echo "${latest}"
}

trap cleanup EXIT
trap 'on_signal INT 2' INT
trap 'on_signal TERM 15' TERM
trap 'on_signal HUP 1' HUP
trap on_error ERR

sweep_stale_generations

case "${SOURCE_MODE}" in
    main) SOURCE_FILE="${MAIN_FILE}" ;;
    replica) SOURCE_FILE="${REPLICA_FILE}" ;;
    *)
        log "ERROR: RQUANT_BACKUP_SOURCE must be main or replica"
        exit 2
        ;;
esac

if [[ ! -x "${VENV_PY}" ]]; then
    log "ERROR: backup Python is not executable: ${VENV_PY}"
    exit 1
fi
if [[ ! -f "${SOURCE_FILE}" ]]; then
    log "ERROR: source database not found: ${SOURCE_FILE}"
    exit 1
fi
if [[ ! "${MAX_SOURCE_LAG_SECONDS}" =~ ^[0-9]+$ ]] \
    || [[ ! "${REPLICA_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
    log "ERROR: backup lag/wait settings must be non-negative integers"
    exit 2
fi

log "snapshot start: source=${SOURCE_MODE}"

if [[ "${SOURCE_MODE}" == "main" ]]; then
    # Deployment backups explicitly use main after every writer is stopped.
    # Keep the writer lock through the copy so no process can create a new WAL
    # between CHECKPOINT and the private snapshot generation.
    "${VENV_PY}" - "${SOURCE_FILE}" "${TMP_DB}" <<'PY'
import duckdb
import os
import shutil
import sys

path = sys.argv[1]
target = sys.argv[2]
conn = duckdb.connect(path)
try:
    conn.execute("CHECKPOINT")
    if os.path.exists(path + ".wal"):
        raise RuntimeError("main WAL remains after checkpoint")
    shutil.copy2(path, target)
finally:
    conn.close()
PY
    if [[ -e "${SOURCE_FILE}.wal" ]]; then
        log "ERROR: main WAL remains after checkpoint: ${SOURCE_FILE}.wal"
        exit 2
    fi
    source_mtime=$(generation_mtime "${SOURCE_FILE}")
    source_lag_seconds=0
else
    # Replica and backup timers may fire together. Intraday lag up to the
    # replica SLA is intentional; a substantially stale replica (notably the
    # 17:30 daily boundary) waits for this cycle's sync instead of publishing
    # an old dataset with a fresh snapshot timestamp.
    main_generation_mtime=$(generation_mtime "${MAIN_FILE}")
    wait_deadline=$(( $(date +%s) + REPLICA_WAIT_SECONDS ))
    while true; do
        source_mtime=$(generation_mtime "${SOURCE_FILE}")
        source_lag_seconds=$(( main_generation_mtime - source_mtime ))
        if (( source_lag_seconds < 0 )); then
            source_lag_seconds=0
        fi
        if (( source_lag_seconds <= MAX_SOURCE_LAG_SECONDS )); then
            break
        fi
        if (( $(date +%s) >= wait_deadline )); then
            log "ERROR: replica is ${source_lag_seconds}s behind main (max=${MAX_SOURCE_LAG_SECONDS}s)"
            exit 2
        fi
        sleep 2
    done
fi

# Scheduled backups read the independently verified replica. If its WAL exists,
# copy the pair and consolidate it only in the private temporary generation.
if [[ "${SOURCE_MODE}" == "replica" ]]; then
    low_priority cp -- "${SOURCE_FILE}" "${TMP_DB}"
    if [[ -f "${SOURCE_FILE}.wal" ]]; then
        low_priority cp -- "${SOURCE_FILE}.wal" "${TMP_DB}.wal"
    fi
fi
chmod u+w "${TMP_DB}"

table_count=$("${VENV_PY}" - "${TMP_DB}" <<'PY'
import os
import sys

import duckdb

path = sys.argv[1]
conn = duckdb.connect(path)
conn.execute("CHECKPOINT")
conn.close()
if os.path.exists(path + ".wal"):
    raise RuntimeError("temporary snapshot WAL remains after checkpoint")
conn = duckdb.connect(path, read_only=True)
conn.execute("SELECT 1").fetchone()
table_count = int(
    conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchone()[0]
)
conn.close()
if table_count < 1:
    raise RuntimeError("temporary snapshot contains no main-schema tables")
print(table_count)
PY
)

src_size=$(file_size "${TMP_DB}")
low_priority gzip -c -- "${TMP_DB}" > "${TMP_GZ}"
low_priority gzip -t -- "${TMP_GZ}"
gz_size=$(file_size "${TMP_GZ}")
snapshot_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

printf '%s\n' \
    "{\"snapshot_at\": \"${snapshot_at}\", \"source\": \"${SOURCE_MODE}\", \"source_mtime_epoch\": ${source_mtime}, \"source_lag_seconds\": ${source_lag_seconds}, \"verified\": true, \"table_count\": ${table_count}, \"src_bytes\": ${src_size}, \"compressed_bytes\": ${gz_size}}" \
    > "${TMP_JSON}"

mv -- "${TMP_GZ}" "${BACKUP_DIR}/latest.duckdb.gz"
mv -- "${TMP_JSON}" "${BACKUP_DIR}/latest.json"

ratio=$(awk "BEGIN{printf \"%.0f\", ${gz_size}*100/${src_size}}")
log "snapshot OK: source=${SOURCE_MODE}, tables=${table_count}, gz=${gz_size}B (${ratio}% of source)"

case "${RECOVERY_MODE}" in
    auto|true|false) ;;
    *)
        log "ERROR: RQUANT_RECOVERY_BACKUP_ENABLED must be auto, true, or false"
        exit 2
        ;;
esac
RECOVERY_PROFILE_PRESENT=false
if [[ -e "${RUNTIME_ROOT}/current" || -L "${RUNTIME_ROOT}/current" ]]; then
    RECOVERY_PROFILE_PRESENT=true
fi
RECOVERY_REQUIRED=false
if [[ "${RECOVERY_PROFILE_PRESENT}" == "true" || "${RECOVERY_MODE}" == "true" ]]; then
    RECOVERY_REQUIRED=true
fi

case "${RECOVERY_REQUIRED}" in
    false) ;;
    true)
        recovery_assertion_count=0
        for recovery_assertion in \
            "${RECOVERY_CONFIG_ASSERTION}" \
            "${RECOVERY_CREDENTIAL_ASSERTION}" \
            "${RECOVERY_PROFILE_GENERATION_ASSERTION}" \
            "${RECOVERY_SIGNER_KEY_ID_ASSERTION}"; do
            if [[ -n "${recovery_assertion}" ]]; then
                recovery_assertion_count=$((recovery_assertion_count + 1))
            fi
        done
        if [[ "${recovery_assertion_count}" -ne 0 && "${recovery_assertion_count}" -ne 4 ]]; then
            log "ERROR: recovery backup profile assertions must be supplied together"
            exit 2
        fi
        if [[ ! -x "${RECOVERY_CLI}" ]]; then
            log "ERROR: recovery CLI is not executable: ${RECOVERY_CLI}"
            exit 1
        fi
        recovery_profile=$("${RECOVERY_CLI}" runtime-recovery-production-config \
            --runtime-root "${RUNTIME_ROOT}")
        recovery_profile_value() {
            printf '%s' "${recovery_profile}" | "${VENV_PY}" -c \
                'import json,sys; payload=json.load(sys.stdin); value=payload'"$1"'; assert isinstance(value,str) and value; print(value)'
        }
        recovery_status=$(recovery_profile_value '["status"]')
        recovery_runtime_root=$(recovery_profile_value '["runtime_root"]')
        RECOVERY_PROFILE_ENABLED=$(recovery_profile_value '["backup_environment"]["RQUANT_RECOVERY_BACKUP_ENABLED"]')
        RECOVERY_CONFIG=$(recovery_profile_value '["backup_environment"]["RQUANT_RECOVERY_BACKUP_CONFIG"]')
        RECOVERY_CREDENTIAL=$(recovery_profile_value '["backup_environment"]["RQUANT_RECOVERY_CREDENTIAL_FILE"]')
        RECOVERY_PROFILE_GENERATION=$(recovery_profile_value '["backup_environment"]["RQUANT_RECOVERY_PROFILE_GENERATION"]')
        RECOVERY_SIGNER_KEY_ID=$(recovery_profile_value '["backup_environment"]["RQUANT_RECOVERY_SIGNER_KEY_ID"]')
        if [[ "${recovery_status}" != "ready" || "${recovery_runtime_root}" != "${RUNTIME_ROOT}" ]] \
            || [[ "${RECOVERY_PROFILE_ENABLED}" != "true" ]] \
            || [[ ! "${RECOVERY_PROFILE_GENERATION}" =~ ^[0-9a-f]{64}$ ]] \
            || [[ ! "${RECOVERY_SIGNER_KEY_ID}" =~ ^[a-z0-9][a-z0-9_.-]{0,127}$ ]]; then
            log "ERROR: current recovery production profile is invalid"
            exit 2
        fi
        if [[ "${RECOVERY_MODE}" != "auto" && "${RECOVERY_MODE}" != "${RECOVERY_PROFILE_ENABLED}" ]]; then
            log "ERROR: recovery backup switch differs from current production profile"
            exit 2
        fi
        if [[ -n "${RECOVERY_CONFIG_ASSERTION}" && "${RECOVERY_CONFIG_ASSERTION}" != "${RECOVERY_CONFIG}" ]] \
            || [[ -n "${RECOVERY_CREDENTIAL_ASSERTION}" && "${RECOVERY_CREDENTIAL_ASSERTION}" != "${RECOVERY_CREDENTIAL}" ]] \
            || [[ -n "${RECOVERY_PROFILE_GENERATION_ASSERTION}" && "${RECOVERY_PROFILE_GENERATION_ASSERTION}" != "${RECOVERY_PROFILE_GENERATION}" ]] \
            || [[ -n "${RECOVERY_SIGNER_KEY_ID_ASSERTION}" && "${RECOVERY_SIGNER_KEY_ID_ASSERTION}" != "${RECOVERY_SIGNER_KEY_ID}" ]]; then
            log "ERROR: recovery backup environment differs from current production profile"
            exit 2
        fi
        if [[ ! -f "${RECOVERY_CONFIG}" || ! -f "${RECOVERY_CREDENTIAL}" ]]; then
            log "ERROR: recovery config or credential is missing"
            exit 1
        fi
        recovery_preview=$("${RECOVERY_CLI}" runtime-recovery-backup dry-run \
            --config "${RECOVERY_CONFIG}" \
            --credential-file "${RECOVERY_CREDENTIAL}")
        recovery_plan=$(printf '%s' "${recovery_preview}" | "${VENV_PY}" -c \
            'import json,sys; value=json.load(sys.stdin)["plan_id"]; assert isinstance(value,str) and len(value)==64; print(value)')
        recovery_generation=$(printf '%s' "${recovery_preview}" | "${VENV_PY}" -c \
            'import json,sys; print(json.load(sys.stdin)["target_profile_generation"])')
        recovery_signer=$(printf '%s' "${recovery_preview}" | "${VENV_PY}" -c \
            'import json,sys; print(json.load(sys.stdin)["signer_key_id"])')
        if [[ "${recovery_generation}" != "${RECOVERY_PROFILE_GENERATION}" ]] \
            || [[ "${recovery_signer}" != "${RECOVERY_SIGNER_KEY_ID}" ]]; then
            log "ERROR: recovery backup config differs from production profile generation/key"
            exit 2
        fi
        recovery_result=$("${RECOVERY_CLI}" runtime-recovery-backup execute \
            --config "${RECOVERY_CONFIG}" \
            --credential-file "${RECOVERY_CREDENTIAL}" \
            --plan-id "${recovery_plan}")
        log "recovery backup OK: ${recovery_result}"
        ;;
esac
