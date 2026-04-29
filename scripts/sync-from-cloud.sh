#!/usr/bin/env bash
# 本地从云端拉 backup snapshot 做热备
# - 走 HTTP basic auth（绕开 SSH/fail2ban）
# - 只在数据有变化的时段同步：盘中 09:30-15:05 + 日终 17:10-17:30
# - 失败重试 2 次（间隔 60s），最终失败 PushDeer 告警

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA_FILE="${PROJECT_DIR}/data/rquant.duckdb"
LOG_DIR="${PROJECT_DIR}/logs"
LOG="${LOG_DIR}/sync-from-cloud.log"
ENV_FILE="${PROJECT_DIR}/.env"

mkdir -p "${LOG_DIR}"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

# 加载 .env
if [[ ! -f "${ENV_FILE}" ]]; then
    log "ERROR: .env not found at ${ENV_FILE}"
    exit 1
fi
# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

: "${RQUANT_BACKUP_USER:?missing RQUANT_BACKUP_USER in .env}"
: "${RQUANT_BACKUP_TOKEN:?missing RQUANT_BACKUP_TOKEN in .env}"
: "${RQUANT_BACKUP_URL:?missing RQUANT_BACKUP_URL in .env}"

# --force 跳过时段判断（手动调试用）
force_mode=0
if [[ "${1:-}" == "--force" ]]; then force_mode=1; fi

# 时段判断
hour=$(date +%H); minute=$(date +%M); dow=$(date +%u)
hhmm=$((10#${hour} * 100 + 10#${minute}))
sync_window=""
if (( hhmm >= 930 && hhmm <= 1505 )); then
    sync_window="intraday"
elif (( hhmm >= 1710 && hhmm <= 1730 )); then
    sync_window="daily_after_pipeline"
fi

if (( force_mode == 1 )); then
    log "sync window: forced (manual)"
elif [[ -z "${sync_window}" ]]; then
    log "skip: not in sync window (hhmm=${hhmm}, use --force to override)"
    exit 0
elif [[ "${sync_window}" == "intraday" && "${dow}" -gt 5 ]]; then
    log "skip: weekend (dow=${dow})"
    exit 0
else
    log "sync window: ${sync_window}"
fi

TMP_GZ="${LOCAL_DATA_FILE}.gz.tmp"
TMP_DB="${LOCAL_DATA_FILE}.tmp"

# curl 失败重试 2 次（HTTP 失败不触发 fail2ban，比 SSH rsync 重试安全得多）
ok=0
http_status=""
for attempt in 1 2; do
    log "curl attempt ${attempt}/2"
    http_status=$(curl -sS --fail --max-time 60 \
            --user "${RQUANT_BACKUP_USER}:${RQUANT_BACKUP_TOKEN}" \
            -o "${TMP_GZ}" \
            -w "%{http_code}" \
            "${RQUANT_BACKUP_URL}/latest.duckdb.gz" 2>>"${LOG}") && {
        ok=1
        break
    } || true
    log "curl failed (attempt ${attempt}, http_status=${http_status})"
    sleep 60
done

if (( ok == 0 )); then
    log "ERROR: curl failed 2 times (last status=${http_status}), sending PushDeer alert"

    keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
    title="❌ rQuant 备份同步失败"
    body="curl 拉云端 backup 失败 2 次。

时间：$(date '+%Y-%m-%d %H:%M:%S')
URL：${RQUANT_BACKUP_URL}/latest.duckdb.gz
HTTP 状态：${http_status}

最近日志：
\`\`\`
$(tail -n 15 "${LOG}")
\`\`\`"
    IFS=',' read -ra KEY_ARR <<< "${keys}"
    for key in "${KEY_ARR[@]}"; do
        k=$(echo "${key}" | xargs)
        [[ -n "${k}" ]] || continue
        curl -s -X POST "${endpoint}" \
            --data-urlencode "pushkey=${k}" \
            --data-urlencode "text=${title}" \
            --data-urlencode "desp=${body}" \
            --data-urlencode "type=markdown" \
            --max-time 10 >/dev/null 2>&1 || true
    done
    rm -f "${TMP_GZ}"
    exit 1
fi

# 解压
if ! gunzip -c "${TMP_GZ}" > "${TMP_DB}" 2>>"${LOG}"; then
    log "ERROR: gunzip failed"
    rm -f "${TMP_GZ}" "${TMP_DB}"
    exit 1
fi
rm -f "${TMP_GZ}"

# atomic rename → 本地始终是完整文件
mv "${TMP_DB}" "${LOCAL_DATA_FILE}"
size=$(du -h "${LOCAL_DATA_FILE}" | cut -f1)
log "sync OK: ${size}"
