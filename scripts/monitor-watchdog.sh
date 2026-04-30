#!/usr/bin/env bash
# 盘中 monitor 守护：每 2 分钟检查一次 rquant-monitor.service。
# 不活：推一条 PushDeer + 尝试拉起。timer 限定在 09:30..15:00 触发，盘外不打扰。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
UNIT="rquant-monitor.service"

if systemctl is-active --quiet "${UNIT}"; then
    exit 0
fi

# 不活：先告警，再尝试拉起
"${RQUANT_BIN}" alert \
    --subject "[D] ${UNIT} 不在跑（盘中）" \
    --body "watchdog 自动尝试 systemctl start" || true

systemctl start "${UNIT}"
