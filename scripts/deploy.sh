#!/usr/bin/env bash
# 云端部署一键脚本（lighthouse 用户在 /home/lighthouse/rquant 跑）。
#
# 用法:
#   bash scripts/deploy.sh           # 拉代码 → 同步 systemd → enable 新 timer → restart 关联 service
#   bash scripts/deploy.sh --dry-run # 只打印动作，不真改
#
# 干的事（按顺序）：
#   1. git pull（无变化直接退）
#   2. deploy/systemd/ 有改动 → cp 到 /etc/systemd/system + daemon-reload
#   3. 启用新 timer + restart 已改 timer（让新 OnCalendar 生效）
#   4. 按 src/rquant/* 改动路径关联 restart 受影响的 service（只动跑着的）
#   5. post-deploy 验证：所有改动的 timer 下次 trigger 在 24h 内（防 v0.11.3 翻车再发：
#      OnCalendar 被 systemd 静默拒收 → timer 假装在跑但实际从不 trigger）
#   6. 输出 systemd timer 状态 + 各 service is-active 汇总
#
# 安全约束：
#   - 只在 main 分支上跑（不要在其他分支误部署）
#   - Python 改动→service restart 走白名单关联表，不无脑全部重启
#   - sudo 操作集中前置，密码只输一次（systemd cp + daemon-reload + enable + restart）

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SYSTEMD_DIR="${PROJECT_DIR}/deploy/systemd"
SYSTEMD_TARGET="/etc/systemd/system"

cd "${PROJECT_DIR}"

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

# ---------- helpers ----------

if [[ -t 1 ]]; then
    BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
else
    BLUE=''; GREEN=''; YELLOW=''; RED=''; NC=''
fi

step() { echo -e "\n${BLUE}▸ $*${NC}"; }
ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
err()  { echo -e "  ${RED}✗${NC} $*"; }

run() {
    if [[ ${DRY_RUN} -eq 1 ]]; then
        echo "  [dry-run] $*"
    else
        eval "$@"
    fi
}

# ---------- 0. 前置检查 ----------

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "${CURRENT_BRANCH}" != "main" ]]; then
    err "当前分支 ${CURRENT_BRANCH} 不是 main，拒绝部署。"
    err "用 \`git checkout main\` 切回 main 后重跑。"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    warn "工作区有未提交改动："
    git status --short | sed 's/^/    /'
    warn "继续执行（不会被 git pull 覆盖，但提醒一下）"
fi

# ---------- 1. git pull ----------

step "[1/5] git pull"
PRE_HEAD=$(git rev-parse HEAD)
if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "  [dry-run] git pull --ff-only"
    git fetch origin --quiet
    POST_HEAD=$(git rev-parse origin/main)
else
    git pull --ff-only
    POST_HEAD=$(git rev-parse HEAD)
fi

if [[ "${PRE_HEAD}" == "${POST_HEAD}" ]]; then
    ok "已最新（${PRE_HEAD:0:7}），跳过部署。"
    exit 0
fi

CHANGED=$(git diff --name-only "${PRE_HEAD}..${POST_HEAD}")
ok "${PRE_HEAD:0:7} → ${POST_HEAD:0:7}，变更："
echo "${CHANGED}" | sed 's/^/    /'

# ---------- 2. systemd unit 同步 ----------

step "[2/5] systemd unit 同步"
SYSTEMD_CHANGED=$(echo "${CHANGED}" | grep -E "^deploy/systemd/.*\.(service|timer)$" || true)
if [[ -n "${SYSTEMD_CHANGED}" ]]; then
    echo "  改动文件："
    echo "${SYSTEMD_CHANGED}" | sed 's|^deploy/systemd/|    |'
    run "sudo cp '${SYSTEMD_DIR}/'*.{service,timer} '${SYSTEMD_TARGET}/'"
    run "sudo systemctl daemon-reload"
    ok "daemon-reload 完成"
else
    ok "无 systemd unit 改动"
fi

# ---------- 3. 启用新 timer + restart 已改 timer ----------
#
# 关键修复（v0.11.3 翻车教训）：daemon-reload 只是让 systemd 重新读 unit 文件，
# 但已经在跑的 timer 实例仍用旧 OnCalendar。必须 restart 才能让新调度生效。
#
# - 已 enabled 的 timer：sudo systemctl restart（应用新 OnCalendar）
# - 未 enabled 的 timer：sudo systemctl enable --now（首次启用 + 启动）

step "[3/6] 启用 / 重启 timer（让新 OnCalendar 生效）"
CHANGED_TIMER_FILES=$(echo "${SYSTEMD_CHANGED}" | grep "\.timer$" || true)
TIMER_HANDLED=0
for f in ${CHANGED_TIMER_FILES}; do
    t=$(basename "${f}")
    if systemctl is-enabled "${t}" 2>/dev/null | grep -qE "^enabled"; then
        run "sudo systemctl restart '${t}'"
        ok "↻ ${t} (restarted, 应用新 OnCalendar)"
    else
        run "sudo systemctl enable --now '${t}'"
        ok "+ ${t} (enabled + started)"
    fi
    TIMER_HANDLED=$((TIMER_HANDLED + 1))
done
[[ ${TIMER_HANDLED} -eq 0 ]] && ok "无 timer 改动"

# ---------- 4. Python 改动 → restart 关联 service ----------

step "[4/6] 检测 Python 改动 → restart 关联 service"

# Python 路径 → service 关联（grep -E 模式）。新增 service 时往这个 case 加一行。
svc_pattern() {
    case "$1" in
        rquant-monitor.service)
            echo '^src/rquant/(monitor\.py|notify/|storage/|risk/|state/|config\.py|presets\.py|screen/|indicator/)'
            ;;
        rquant-dashboard.service)
            echo '^src/rquant/(dashboard/app\.py|dashboard/__init__\.py|storage/|risk/|config\.py|presets\.py|state/)'
            ;;
        rquant-nl-screen.service)
            echo '^src/rquant/(dashboard/nl_screen\.py|dashboard/__init__\.py|llm/|screen/|storage/|config\.py|presets\.py|state/)'
            ;;
    esac
}
LONG_RUNNING_SVCS="rquant-monitor.service rquant-dashboard.service rquant-nl-screen.service"

# 直接改了 service 文件的也要 restart
SVC_FILE_CHANGED=$(echo "${SYSTEMD_CHANGED}" | grep "\.service$" | xargs -I{} basename {} 2>/dev/null || true)
PY_CHANGED=$(echo "${CHANGED}" | grep -E "^src/rquant/" || true)

# 用空格分隔字符串收集，最后去重
TO_RESTART=""
if [[ -n "${PY_CHANGED}" ]]; then
    for svc in ${LONG_RUNNING_SVCS}; do
        pattern=$(svc_pattern "${svc}")
        if echo "${PY_CHANGED}" | grep -qE "${pattern}"; then
            TO_RESTART="${TO_RESTART} ${svc}"
        fi
    done
fi
TO_RESTART="${TO_RESTART} ${SVC_FILE_CHANGED}"
TO_RESTART=$(echo "${TO_RESTART}" | tr ' ' '\n' | grep -v '^$' | sort -u)

if [[ -z "${TO_RESTART}" ]]; then
    ok "无关联 service 需要 restart"
else
    for svc in ${TO_RESTART}; do
        if systemctl is-active "${svc}" &>/dev/null; then
            run "sudo systemctl restart '${svc}'"
            ok "→ ${svc} restarted"
        else
            warn "${svc} 未运行（inactive），跳过 restart"
        fi
    done
fi

# ---------- 5. post-deploy 验证：改动的 timer 是否真在调度 ----------
#
# v0.11.3 翻车教训：OnCalendar 语法错误会被 systemd 静默拒收，整段丢弃。
# `systemctl status` 看起来是 active (waiting)，但下次 trigger 实际是 24h 后
# 甚至 "n/a"。这一步用 `list-timers --all` 检查改动 timer 的 NEXT 列：
#   - NEXT 在 24h 内 → 正常
#   - NEXT 是 "n/a" 或 > 24h → 翻车（OnCalendar 没生效），exit 1 让 caller 知道
#
# 跳过：dry-run、无 timer 改动

step "[5/6] post-deploy 验证：改动的 timer NEXT trigger 在 24h 内"
TIMER_FAIL=0
if [[ ${DRY_RUN} -eq 1 ]]; then
    ok "dry-run，跳过验证"
elif [[ -z "${CHANGED_TIMER_FILES}" ]]; then
    ok "无 timer 改动，跳过验证"
else
    # 给 systemd 1s 应用 restart 后的下次 trigger 计算
    sleep 1
    for f in ${CHANGED_TIMER_FILES}; do
        t=$(basename "${f}")
        # list-timers 输出列：NEXT LEFT LAST PASSED UNIT ACTIVATES
        # show -p NextElapseUSecRealtime 返回微秒 epoch 或 0（n/a）
        next_us=$(systemctl show "${t}" -p NextElapseUSecRealtime --value 2>/dev/null)
        if [[ -z "${next_us}" || "${next_us}" == "0" ]]; then
            err "${t}: 下次 trigger = n/a（OnCalendar 可能被 systemd 拒收！）"
            err "  排查：systemctl status ${t} / systemd-analyze calendar '<OnCalendar 表达式>'"
            TIMER_FAIL=1
            continue
        fi
        # 微秒 → 秒；跟当前时间比
        next_s=$((next_us / 1000000))
        now_s=$(date +%s)
        delta=$((next_s - now_s))
        if [[ ${delta} -lt 0 ]]; then
            warn "${t}: 下次 trigger 在过去（${delta}s 前），可能正在 catch up"
        elif [[ ${delta} -gt 86400 ]]; then
            err "${t}: 下次 trigger 在 $(( delta / 3600 ))h 后（> 24h），OnCalendar 可能有问题"
            TIMER_FAIL=1
        else
            human=$(date -d "@${next_s}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "${next_s}" '+%Y-%m-%d %H:%M:%S')
            ok "${t}: 下次 trigger ${human}（${delta}s 后）"
        fi
    done
fi

# ---------- 6. 状态汇总 ----------

step "[6/6] 部署后状态"
echo ""
echo "  Timers (rquant-*)："
systemctl list-timers --no-pager 'rquant-*' 2>/dev/null | head -12 | sed 's/^/    /'
echo ""
echo "  Services (rquant-*)："
for s in rquant-monitor rquant-dashboard rquant-nl-screen; do
    # systemctl is-active 对 inactive 服务退码 3 + 输出 "inactive"；
    # 用 cmd && true 让 set -uo pipefail 不挂；命令替换只取 stdout
    state=$(systemctl is-active "${s}.service" 2>/dev/null) && true
    [[ -z "${state}" ]] && state="unknown"
    echo "    ${s}.service: ${state}"
done

echo ""
if [[ ${TIMER_FAIL} -eq 1 ]]; then
    err "部署完成但有 timer 验证失败（${PRE_HEAD:0:7} → ${POST_HEAD:0:7}），见 step [5/6]"
    exit 2
fi
ok "部署完成（${PRE_HEAD:0:7} → ${POST_HEAD:0:7}）"
