#!/usr/bin/env bash
# Tushare token 到期提醒（一次性，由 rquant-tushare-token-reminder.timer 在
# 2027-03-13 12:00 CST 触发，距离 2027-04-16 到期日 34 天）。
#
# 背景：5/25 事故后，pre_market_check 的 tushare 积分自动监控（pro.user 接口）
# 失效（PR #30 降级为 warn），没法自动追踪积分到期。改用 systemd 一次性 timer
# 兜底提醒 → 收到 push 后人工去 web 端确认 / 续费 / 换 token。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PY="${PROJECT_DIR}/.venv/bin/python"

"${VENV_PY}" <<'PYEOF'
from rquant.config import settings
from rquant.notify.client import PushDeerClient

title = "⏰ Tushare token 到期提醒（约 34 天后）"
body = (
    "Tushare 主 token 将于 **2027-04-16** 到期。\n\n"
    "## 行动项\n"
    "1. 登录 https://tushare.pro 个人主页查看当前积分与到期日\n"
    "2. 续费 / 申请新 token\n"
    "3. 更新 `/home/lighthouse/rquant/.env` 的 `TUSHARE_TOKEN_MAIN`\n"
    "4. `sudo systemctl restart rquant-monitor.service`\n\n"
    "（5/25 事故 lesson：tushare 把 `pro.user` 内部接口下线，无法再自动监控\n"
    "积分到期，本提醒由 systemd 一次性 timer 兜底。）"
)

import sys

client = PushDeerClient(
    keys=settings.pushdeer_key_list,
    endpoint=settings.pushdeer_endpoint,
)
results = client.push(title, body)
any_ok = False
for key, (success, err) in zip(settings.pushdeer_key_list, results, strict=False):
    tag = "OK" if success else f"FAIL: {err}"
    print(f"PushDeer {key[:8]}…: {tag}")
    any_ok = any_ok or success

# PR2：一次性提醒推送全失败时 exit 1，让 service OnFailure 接管（落盘兜底），
# 否则一次性 timer 触发后不再来，token 续费提醒就永久丢了。
if not any_ok:
    print("所有 PushDeer 推送失败，exit 1 触发 OnFailure 兜底", file=sys.stderr)
    sys.exit(1)
PYEOF
