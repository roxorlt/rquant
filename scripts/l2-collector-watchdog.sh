#!/usr/bin/env bash
# rQuant L2 采集器盘中守护（2026-11-06 P0 事故后新增）
#
# 背景：2026-11-06 09:31 供应商 peer_closed，采集器退出后 systemd Restart=no 未恢复，
# 开盘缺口约 10 分钟才被人工发现。本 watchdog 每 2 分钟（交易时段）做三件事：
#   1) unit failed → reset-failed + 拉起一次，并告警（兜住 Restart 策略放弃的情况）
#   2) unit 不活跃（非 failed，例如被手工停）→ 只告警，不擅自拉起
#   3) 活跃但行情不新鲜（status.json last_transport 超 STALE_SECONDS）→ 告警
# 告警走 rquant alert（PushDeer），带 dedup key + 15 分钟冷却，避免每 2 分钟刷屏。
set -uo pipefail

PROJECT_DIR=/home/lighthouse/rquant
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
UNIT="rquant-l2-collector.service"
STATUS_FILE="${STATUS_FILE:-/home/lighthouse/rquant-l2-state/cache/status.json}"
LOG_FILE="${PROJECT_DIR}/logs/l2-collector-watchdog-$(date +%F).log"
STALE_SECONDS="${STALE_SECONDS:-300}"
DEDUP_FAIL="service:${UNIT}"
DEDUP_FRESH="service:rquant-l2-collector-freshness"

mkdir -p "${PROJECT_DIR}/logs" /home/lighthouse/rquant-l2-state/logs 2>/dev/null || true
mkdir -p /home/lighthouse/rquant-l2-state/logs 2>/dev/null || true
LOG_FILE=/home/lighthouse/rquant-l2-state/logs/l2-collector-watchdog-$(date +%F).log

log_event() { printf '%s %s\n' "$(date -Iseconds)" "$*" >> "${LOG_FILE}"; }
alert() {  # $1=dedup $2=subject $3=body
    if ! "${RQUANT_BIN}" alert --subject "$2" --body "$3" --dedup-key "$1" --cooldown-seconds 900; then
        log_event "ALERT-FAILED $1"
    fi
}
resolve() { "${RQUANT_BIN}" alert-resolve --dedup-key "$1" >/dev/null 2>&1 || true; }

# 交易时段 gate（timer 已限定 Mon..Fri 9..15:0/2，这里再收窄到实际盘中）
NOW_HM=$(date +%H%M); NOW_HM_INT=$((10#${NOW_HM}))
IN_WINDOW=0
# 收盘集合竞价 15:00 结束；窗口截到 15:03，避免 15:03 之后把「收盘后无数据」误报为断流
if { [ "${NOW_HM_INT}" -ge 915 ] && [ "${NOW_HM_INT}" -le 1135 ]; } || \
   { [ "${NOW_HM_INT}" -ge 1255 ] && [ "${NOW_HM_INT}" -le 1503 ]; }; then
    IN_WINDOW=1
fi
if [ "${IN_WINDOW}" != 1 ]; then log_event "out-of-window"; exit 0; fi

# 1) failed → 自愈一次 + 告警
if sudo systemctl is-failed --quiet "${UNIT}"; then
    log_event "unit-failed: attempting reset-failed + start"
    sudo systemctl reset-failed "${UNIT}" || true
    if sudo systemctl start "${UNIT}"; then
        log_event "unit-failed: restart-ok"
        alert "${DEDUP_FAIL}" \
          "🚨 [RQ] L2 采集器失败并已自动重启" \
          "## L2 collector failed and was restarted

| item | value |
|---|---|
| unit | \`${UNIT}\` |
| host | $(hostname) |
| time | $(date '+%F %T') |

watchdog 已执行 reset-failed + start。请检查是否再次断线：

\`\`\`bash
sudo systemctl status ${UNIT} --no-pager -n 20
curl -s http://127.0.0.1:8771/api/v1/l2/status
\`\`\`"
    else
        log_event "unit-failed: restart-FAILED"
        alert "${DEDUP_FAIL}" \
          "🚨🚨 [RQ] L2 采集器失败且自动重启失败" \
          "## L2 collector failed, restart FAILED

| item | value |
|---|---|
| unit | \`${UNIT}\` |
| host | $(hostname) |
| time | $(date '+%F %T') |

需要人工介入：

\`\`\`bash
sudo systemctl status ${UNIT} --no-pager -n 30
sudo journalctl -u ${UNIT} -n 100 --no-pager
\`\`\`"
    fi
    exit 0
fi

# 2) 不活跃（非 failed，例如被手工停）→ 仅告警
if ! sudo systemctl is-active --quiet "${UNIT}"; then
    log_event "unit-inactive: alert-only"
    alert "${DEDUP_FAIL}" \
      "🚨 [RQ] L2 采集器未运行（非 failed）" \
      "## L2 collector is not running

| item | value |
|---|---|
| unit | \`${UNIT}\` |
| host | $(hostname) |
| time | $(date '+%F %T') |

看板规则是不擅自拉起手工停服；请确认是否有意停机。"
    exit 0
fi

# 3) 活跃：检查行情新鲜度
VERDICT=$(python3 - "${STATUS_FILE}" "${STALE_SECONDS}" <<'PY'
import json, sys, datetime
path, stale = sys.argv[1], float(sys.argv[2])
try:
    with open(path, 'r', encoding='utf-8') as fh:
        st = json.load(fh)
except Exception as exc:
    print(f"NO_STATUS {type(exc).__name__}")
    raise SystemExit(0)
state = st.get("state")
last = st.get("last_transport_received_at")
if state not in ("listening_candidate", "seeded_waiting", "connected", "starting"):
    print(f"IDLE state={state}")
    raise SystemExit(0)
if not last:
    print(f"STALE state={state} age=unknown")
    raise SystemExit(0)
ts = datetime.datetime.fromisoformat(last.replace("Z", "+00:00")).timestamp()
age = time_now = __import__("time").time() - ts
if age > stale:
    print(f"STALE state={state} age={int(age)}s")
else:
    print(f"OK state={state} age={int(age)}s")
PY
)
case "${VERDICT}" in
  OK*) log_event "fresh ${VERDICT}"; resolve "${DEDUP_FRESH}" ;;
  IDLE*) log_event "idle ${VERDICT}" ;;
  STALE*)
    log_event "STALE ${VERDICT}"
    alert "${DEDUP_FRESH}" \
      "🚨 [RQ] L2 行情不新鲜：${VERDICT}" \
      "## L2 market data is stale

| item | value |
|---|---|
| unit | \`${UNIT}\` |
| verdict | \`${VERDICT}\` |
| host | $(hostname) |
| time | $(date '+%F %T') |

进程活着但超过 ${STALE_SECONDS}s 没有收到任何传输（心跳/行情）。可能是供应商静默断流：

\`\`\`bash
curl -s http://127.0.0.1:8771/api/v1/l2/status
sudo journalctl -u ${UNIT} -n 50 --no-pager
\`\`\`"
    ;;
  NO_STATUS*) log_event "status-unreadable ${VERDICT}"; alert "${DEDUP_FRESH}" \
      "⚠️ [RQ] L2 status.json 读不到（${VERDICT}）" \
      "## L2 status.json unreadable

| item | value |
|---|---|
| path | \`${STATUS_FILE}\` |
| verdict | \`${VERDICT}\` |
| time | $(date '+%F %T') |" ;;
esac
