#!/usr/bin/env bash
# 盘中 monitor 守护：每 2 分钟检查一次 rquant-monitor.service。
# 不活：先看是不是"今天已成功跑完退了"（节假日 / 收盘后），是则静默；
#       否则推 PushDeer + 尝试拉起。timer 限定在 09:30..15:00 触发，盘外不打扰。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
UNIT="rquant-monitor.service"

# 已 active：正常路径，直接退
if systemctl is-active --quiet "${UNIT}"; then
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
    # 今天已成功退出（非交易日 monitor 自检退 / 收盘后正常退）
    # 不告警不重启，watchdog 静默
    exit 0
fi

# 真不活：告警 + 拉起
"${RQUANT_BIN}" alert \
    --subject "[D] ${UNIT} 不在跑（盘中）" \
    --body "watchdog 自动尝试 systemctl start" || true

systemctl start "${UNIT}"
