#!/usr/bin/env bash
# 幂等安装盘中脉搏 + 午间战报的 launchd 任务（mac 本地）。
#
#   morning-pulse  Mon..Fri 10:00 / 10:30 / 11:00 / 11:30
#   midday-report  Mon..Fri 12:00
#
# 跑的是主 checkout（REPO 变量）的 venv 与代码，不是 worktree。重复执行安全：
# 先 bootout 旧实例再 bootstrap 新的。
set -euo pipefail

REPO="/Users/roxor/brain/30-projects/rQuant"
SRC_DIR="${REPO}/deploy/launchd"
DEST_DIR="${HOME}/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"
LABELS=(com.roxor.rquant-morning-pulse com.roxor.rquant-midday-report)

mkdir -p "${DEST_DIR}" "${REPO}/logs"

for label in "${LABELS[@]}"; do
    src="${SRC_DIR}/${label}.plist"
    dest="${DEST_DIR}/${label}.plist"
    if [[ ! -f "${src}" ]]; then
        echo "❌ 缺少 plist: ${src}" >&2
        exit 1
    fi

    echo "→ 安装 ${label}"
    cp "${src}" "${dest}"

    # 已加载则先卸载（bootout 对未加载的 label 返回非 0，容错忽略）
    launchctl bootout "${DOMAIN}/${label}" 2>/dev/null || true
    launchctl bootstrap "${DOMAIN}" "${dest}"
    echo "  ✅ bootstrap 完成"
done

echo
echo "已加载任务："
for label in "${LABELS[@]}"; do
    launchctl print "${DOMAIN}/${label}" 2>/dev/null \
        | grep -E "state|next run|path" || echo "  (${label} 未能 print，请检查)"
done

echo
echo "手动补跑验证（dry-run，不推送）："
echo "  cd ${REPO} && .venv/bin/rquant morning-pulse --slot 10:00 --force --dry-run"
echo "  cd ${REPO} && .venv/bin/rquant midday-report --force --dry-run"
