#!/usr/bin/env bash
# 本地从云端拉 backup snapshot 做热备
# - 走 HTTP basic auth（绕开 SSH/fail2ban）
# - 只在数据有变化的时段同步：盘中 09:30-15:05 + 日终 17:10-17:30
# - 失败重试 2 次（间隔 60s），最终失败 PushDeer 告警
# - 5/13 新增：检测源 stale（snapshot_at 持续不变），intraday 时段下推 PushDeer
#   防 v0.11.3 翻车再发：云端 backup.timer 假装在跑但 OnCalendar 被静默拒收

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
    # --max-time 600（10 分钟）：234MB 源文件 gzip 后 80-120MB，
    # 5Mbps 带宽下需 ~3-5 分钟，留余量
    http_status=$(curl -sS --fail --max-time 600 \
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

# 解压（用 gzip -dc 替代 gunzip，避免对 .tmp 后缀严格检查）
if ! gzip -dc "${TMP_GZ}" > "${TMP_DB}" 2>>"${LOG}"; then
    log "ERROR: gunzip failed"
    rm -f "${TMP_GZ}" "${TMP_DB}"
    exit 1
fi
rm -f "${TMP_GZ}"

# atomic rename → 本地始终是完整文件
mv "${TMP_DB}" "${LOCAL_DATA_FILE}"
size=$(du -h "${LOCAL_DATA_FILE}" | cut -f1)

# ---------- Stale 检测（5/13 新增）----------
# 拉 latest.json 比较 snapshot_at，identify 云端 backup 是否真在更新
# 通用约定：服务端 backup-snapshot.sh 写 latest.json，含 {"snapshot_at": "<ISO>"}
JSON_URL="${RQUANT_BACKUP_URL}/latest.json"
TMP_JSON="${LOCAL_DATA_FILE}.json.tmp"
STATE_DIR="${PROJECT_DIR}/data"
LAST_SNAPSHOT_FILE="${STATE_DIR}/.last-sync-snapshot-at"
LAST_STALE_ALERT_FILE="${STATE_DIR}/.last-stale-alert-at"
STALE_ALERT_COOLDOWN=1800  # 30 分钟，避免 stale 持续时每 5min 都告警刷屏

new_snapshot=""
if curl -sS --fail --max-time 30 \
        --user "${RQUANT_BACKUP_USER}:${RQUANT_BACKUP_TOKEN}" \
        -o "${TMP_JSON}" \
        "${JSON_URL}" 2>>"${LOG}"; then
    # 解析 "snapshot_at": "2026-05-13T06:30:01Z" → 取引号间的 value
    new_snapshot=$(grep -oE '"snapshot_at"[[:space:]]*:[[:space:]]*"[^"]*"' "${TMP_JSON}" 2>/dev/null \
                   | head -n1 | sed -E 's/.*:[[:space:]]*"([^"]*)"$/\1/' || true)
    rm -f "${TMP_JSON}"
fi

if [[ -z "${new_snapshot}" ]]; then
    log "sync OK: ${size} (latest.json 拉取/解析失败，跳过 stale 检查)"
else
    last_snapshot=""
    [[ -f "${LAST_SNAPSHOT_FILE}" ]] && last_snapshot=$(cat "${LAST_SNAPSHOT_FILE}")

    if [[ -z "${last_snapshot}" ]]; then
        # 首次 sync，建立 baseline
        echo "${new_snapshot}" > "${LAST_SNAPSHOT_FILE}"
        log "sync OK: ${size} (baseline snapshot_at=${new_snapshot})"
    elif [[ "${last_snapshot}" != "${new_snapshot}" ]]; then
        # 源文件有更新，正常
        echo "${new_snapshot}" > "${LAST_SNAPSHOT_FILE}"
        log "sync OK: ${size} (snapshot_at=${new_snapshot})"
    elif [[ "${sync_window}" != "intraday" ]]; then
        # 非 intraday 时段（如 daily_after_pipeline 17:10-17:30 抓的还是 17:30 那份）拉到一致是正常
        log "sync OK: ${size} (snapshot_at 持续 ${new_snapshot}，非 intraday 时段，正常)"
    else
        # intraday 时段下源文件不动 → backup.timer 可能没在跑（v0.11.3 风险）
        log "WARN: source stale (snapshot_at=${new_snapshot} 跟上次一致); intraday backup 可能已停"

        # 防刷屏：距上次 stale 告警 ≥ STALE_ALERT_COOLDOWN 才推
        now_ts=$(date +%s)
        should_alert=1
        if [[ -f "${LAST_STALE_ALERT_FILE}" ]]; then
            last_alert_ts=$(cat "${LAST_STALE_ALERT_FILE}")
            if (( now_ts - last_alert_ts < STALE_ALERT_COOLDOWN )); then
                should_alert=0
                remain=$(( STALE_ALERT_COOLDOWN - (now_ts - last_alert_ts) ))
                log "stale alert cooldown: ${remain}s 后才能再推"
            fi
        fi

        if (( should_alert == 1 )); then
            keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
            endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
            endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
            title="[RQ][WARN] backup intraday 卡住"
            body="本地 sync 拉到的 snapshot_at 持续不变（${new_snapshot}），
intraday 时段（盘中）云端 backup 应每 5min 步进。
可能 rquant-backup.timer 没在调度（v0.11.3 风险再发）。

时间：$(date '+%Y-%m-%d %H:%M:%S')
URL：${RQUANT_BACKUP_URL}/latest.duckdb.gz

排查：
1. ssh 上服务器：systemctl list-timers rquant-backup.timer
2. NEXT 应在 5min 内（不是 17:30）
3. 看 backup.service 日志：journalctl -u rquant-backup.service --since '1 hour ago'"
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
            echo "${now_ts}" > "${LAST_STALE_ALERT_FILE}"
            log "PushDeer stale 告警已推（cooldown ${STALE_ALERT_COOLDOWN}s）"
        fi
    fi
fi
