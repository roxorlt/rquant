#!/usr/bin/env bash
# 本地从腾讯云拉 rQuant 数据做热备
# - 业务时段（09:30-15:00, 17:00-17:10）跳过，避免拉到 DuckDB 写入中状态
# - rsync 失败重试 3 次，最终失败推 PushDeer 告警

set -uo pipefail

CLOUD_HOST="${RQUANT_CLOUD_HOST:-lighthouse@82.156.0.68}"
CLOUD_PATH="${RQUANT_CLOUD_PATH:-rquant/data/}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_PATH="${PROJECT_DIR}/data/"
LOG_DIR="${PROJECT_DIR}/logs"
LOG="${LOG_DIR}/sync-from-cloud.log"

mkdir -p "${LOG_DIR}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"
}

# 业务时段判断（HHMM 格式）
hour=$(date +%H)
minute=$(date +%M)
hhmm=$((10#${hour} * 100 + 10#${minute}))

skip_reason=""
if (( hhmm >= 930 && hhmm <= 1500 )); then
    skip_reason="trading hours (09:30-15:00)"
elif (( hhmm >= 1700 && hhmm <= 1710 )); then
    skip_reason="pipeline window (17:00-17:10)"
fi

if [[ -n "${skip_reason}" ]]; then
    log "skip: ${skip_reason} (current: ${hhmm})"
    exit 0
fi

# rsync 重试 3 次（间隔 60s）
for attempt in 1 2 3; do
    log "rsync attempt ${attempt}/3 from ${CLOUD_HOST}:${CLOUD_PATH}"
    if rsync -avz --delete \
        -e "ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new" \
        "${CLOUD_HOST}:${CLOUD_PATH}" \
        "${LOCAL_PATH}" >> "${LOG}" 2>&1; then
        size=$(du -sh "${LOCAL_PATH}" | cut -f1)
        size_bytes=$(du -sb "${LOCAL_PATH}" | cut -f1)
        log "sync OK (attempt ${attempt}, local size ${size})"

        # 写 marker 到云端，供 dashboard 显示"本地最近 sync"状态
        marker_json=$(cat <<EOF
{"sync_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)", "local_size_bytes": ${size_bytes}, "host": "$(hostname -s)"}
EOF
)
        echo "${marker_json}" | ssh -o ConnectTimeout=5 "${CLOUD_HOST}" \
            "cat > ~/rquant/data/.last-local-sync.json" 2>>"${LOG}" || \
            log "warn: marker upload failed (sync 本身已成功)"

        exit 0
    fi
    log "rsync failed (attempt ${attempt})"
    if [[ "${attempt}" -lt 3 ]]; then
        sleep 60
    fi
done

# 3 次失败 → 推 PushDeer 告警
log "ERROR: rsync failed 3 times, sending PushDeer alert"

ENV_FILE="${PROJECT_DIR}/.env"
if [[ -f "${ENV_FILE}" ]]; then
    keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"

    title="❌ rQuant 数据同步失败"
    body="本地从云端 rsync 失败 3 次。

**时间**：$(date '+%Y-%m-%d %H:%M:%S')
**主机**：$(hostname)
**最近日志**：
\`\`\`
$(tail -n 20 "${LOG}")
\`\`\`

请手动检查 SSH 连通性 + 云端服务状态。"

    IFS=',' read -ra KEY_ARR <<< "${keys}"
    for key in "${KEY_ARR[@]}"; do
        key_trimmed=$(echo "${key}" | xargs)
        if [[ -n "${key_trimmed}" ]]; then
            curl -s -X POST "${endpoint}" \
                --data-urlencode "pushkey=${key_trimmed}" \
                --data-urlencode "text=${title}" \
                --data-urlencode "desp=${body}" \
                --data-urlencode "type=markdown" \
                --max-time 10 >> "${LOG}" 2>&1 || true
        fi
    done
fi

exit 1
