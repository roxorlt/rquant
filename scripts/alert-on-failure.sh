#!/usr/bin/env bash
# 被 deploy/systemd/rquant-alert@.service 调用：
#   ExecStart=/home/lighthouse/rquant/scripts/alert-on-failure.sh %i
# %i = 失败的 unit 名（如 rquant-daily.service）
#
# 构造 markdown body 含立即排查 + 恢复命令，调 `rquant alert` 推 PushDeer。
# subject 用 🚨 emoji + 「立即排查」让用户手机一眼看出严重性。

set -euo pipefail

UNIT="${1:?usage: $0 <failed_unit_name>}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
TIME="$(date '+%Y-%m-%d %H:%M:%S')"
HOST="$(hostname)"

SUBJECT="🚨 [RQ] ${UNIT} 失败 — 立即排查"

# PushDeer 支持 markdown（client.py 已传 type=markdown）
BODY="## 🚨 rQuant 服务失败

| 项 | 值 |
|---|---|
| **unit** | \`${UNIT}\` |
| **host** | ${HOST} |
| **time** | ${TIME} |

### 立即排查

\`\`\`bash
sudo systemctl status ${UNIT} --no-pager -n 20
sudo journalctl -u ${UNIT} -n 50 --no-pager
\`\`\`

### 通用恢复

\`\`\`bash
sudo systemctl reset-failed ${UNIT}
sudo systemctl start ${UNIT}
\`\`\`

### 如果是 DuckDB 锁冲突

\`\`\`bash
sudo fuser -v /home/lighthouse/rquant/data/rquant.duckdb
# 找到长期持锁的 read-only streamlit（dashboard / nl-screen / canvas），逐个 restart
sudo systemctl restart rquant-canvas.service
\`\`\`"

# 不用 exec：要在推送失败时落盘兜底（审计 PR2-E）。所有业务 service 的 OnFailure
# 都汇到这里，若 PushDeer/PushPlus 全挂（网络抖动 / token 失效），告警会静默消失。
# 落盘到 logs/alert-failures.jsonl，由 daily-report 扫描带出 + dashboard 可见，
# 保证原始故障"至少在服务器上有记录"。
if ! "${RQUANT_BIN}" alert \
    --subject "${SUBJECT}" \
    --body "${BODY}" \
    --dedup-key "service:${UNIT}" \
    --cooldown-seconds 1800; then
    FAIL_LOG="${PROJECT_DIR}/logs/alert-failures.jsonl"
    mkdir -p "${PROJECT_DIR}/logs"
    # 用 python 写 json，避免 subject 含特殊字符破坏格式
    "${PROJECT_DIR}/.venv/bin/python" - "$UNIT" "$HOST" "$TIME" "$SUBJECT" <<'PYEOF' >> "${FAIL_LOG}" || true
import json, sys
print(json.dumps({
    "failed_at": sys.argv[3], "unit": sys.argv[1],
    "host": sys.argv[2], "subject": sys.argv[4],
}, ensure_ascii=False))
PYEOF
    echo "alert 推送失败，已落盘 ${FAIL_LOG}" >&2
    exit 1
fi
