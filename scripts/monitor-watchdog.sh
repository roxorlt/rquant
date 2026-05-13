#!/usr/bin/env bash
# 盘中 monitor 守护：每 2 分钟检查一次 rquant-monitor.service。
# 不活：先看是不是"今天已成功跑完退了"（节假日 / 收盘后），是则静默；
#       否则推 PushDeer + 尝试拉起。timer 限定在 09:30..15:00 触发，盘外不打扰。
#
# 每次调用追加一行到 logs/watchdog-YYYY-MM-DD.log，供 daily-report 统计：
#   <ISO ts> active           monitor 活，正常路径
#   <ISO ts> skip-clean-exit  今天已 exit 0 过（节假日 / 收盘后），静默
#   <ISO ts> alert-restart    告警 + systemctl start 成功
#   <ISO ts> restart-failed   告警 + systemctl start 失败 + 升级告警
#
# 5/13 复盘：systemctl 调用必须用 sudo。lighthouse 已 NOPASSWD ALL，但
# `systemctl start` 直接调走 polkit 会被拒（`Interactive authentication
# required`）。5/6 09:30-09:48 因此死循环 40 分钟无法自愈，需用户人工 ssh。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
UNIT="rquant-monitor.service"
LOG_FILE="${PROJECT_DIR}/logs/watchdog-$(date +%Y-%m-%d).log"

mkdir -p "${PROJECT_DIR}/logs"

log_event() {
    # $1 = tag
    printf "%s %s\n" "$(date -Iseconds)" "$1" >> "${LOG_FILE}"
}

# 交易时段自检（09:30-15:00）：timer 范围 `09..14:*/2` 包含 09:00-09:28
# 和 11:30-12:58 等非交易时段，本脚本自己 gate。
NOW_HM=$(date +%H%M)
NOW_HM_INT=$((10#${NOW_HM}))
if (( NOW_HM_INT < 930 || NOW_HM_INT > 1500 )); then
    log_event "out-of-window"
    exit 0
fi

# 已 active：正常路径，直接退
if systemctl is-active --quiet "${UNIT}"; then
    log_event "active"
    exit 0
fi

# 不 active：判断是不是"今天已经成功 exit 0 过了"
# 节假日 monitor 在 09:25 触发后 is_trading_day False 立刻退 0
# 此时 watchdog 不该重启 + 不该告警（否则 60 次/天告警轰炸）
LAST_EXIT_TS=$(systemctl show "${UNIT}" -p ExecMainExitTimestamp --value 2>/dev/null || echo "")
LAST_STATUS=$(systemctl show "${UNIT}" -p ExecMainStatus --value 2>/dev/null || echo "")
TODAY=$(date +%Y-%m-%d)

# systemctl 时间戳格式 "Thu 2026-04-30 11:31:03 CST"，用 date -d 解析
LAST_DATE=""
if [[ -n "${LAST_EXIT_TS}" ]]; then
    LAST_DATE=$(date -d "${LAST_EXIT_TS}" +%Y-%m-%d 2>/dev/null || echo "")
fi

if [[ "${LAST_DATE}" == "${TODAY}" && "${LAST_STATUS}" == "0" ]]; then
    # 今天已成功退出（非交易日 monitor 自检退 / 交易日 15:00 收盘后退）
    # 不告警不重启，watchdog 静默
    log_event "skip-clean-exit"
    exit 0
fi

# 真不活：告警 + 拉起（用 sudo；详见 header 注释 5/13 复盘）
"${RQUANT_BIN}" alert \
    --subject "[RQ] ${UNIT} 不在跑（盘中）" \
    --body "watchdog 自动尝试 sudo systemctl start" || true

if sudo systemctl start "${UNIT}"; then
    log_event "alert-restart"
else
    # 重启失败：升级告警让用户立刻介入（sudoers / polkit / unit 配置问题）
    log_event "restart-failed"
    "${RQUANT_BIN}" alert \
        --subject "[RQ][CRITICAL] ${UNIT} 重启失败，需人工介入" \
        --body "watchdog 调用 sudo systemctl start ${UNIT} 失败。可能原因：sudoers 配置漏 / polkit 拒 / unit 自身错。立刻 ssh 上服务器排查。" || true
fi
