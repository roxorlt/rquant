#!/usr/bin/env bash
# 云端部署一键脚本（lighthouse 用户在 /home/lighthouse/rquant 跑）。
#
# 仅保留给 systemd unit 等人工基础设施发布；不得用于无人值守代码发布。
# 日常受控发布使用 scripts/deploy-production.sh --target <exact-tag-or-sha>。
#
# 用法:
#   bash scripts/deploy.sh                                      # 拉代码 → 同步 systemd → restart 已启用 timer → restart 关联 service
#   bash scripts/deploy.sh --dry-run                            # 只打印动作，不真改
#   RQUANT_DAILY_ORCHESTRATOR_INSTANCE=svc-... \
#     bash scripts/deploy.sh --accept-daily-orchestrator-timer  # 只显式接受并 enable Shadow daily timer 实例，不 start
#
# 干的事（按顺序）：
#   1. git pull（无变化直接退）
#   2. deploy/systemd/ 有改动 → cp service/timer/socket/slice 到 /etc/systemd/system + daemon-reload
#   3. restart 已启用 timer（让新 OnCalendar 生效）；未启用 timer 默认保持 disabled
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
ACCEPT_DAILY_ORCHESTRATOR_TIMER=0
START_DAILY_ORCHESTRATOR_TIMER=0
DAILY_ORCHESTRATOR_TEMPLATE_TIMER="rquant-runtime-daily-orchestrator@.timer"
for argument in "$@"; do
    case "${argument}" in
        --dry-run)
            DRY_RUN=1
            ;;
        --accept-daily-orchestrator-timer)
            ACCEPT_DAILY_ORCHESTRATOR_TIMER=1
            ;;
        --start-daily-orchestrator-timer)
            START_DAILY_ORCHESTRATOR_TIMER=1
            ;;
        *)
            echo "unknown argument: ${argument}" >&2
            exit 64
            ;;
    esac
done

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

daily_orchestrator_target_timer() {
    echo "rquant-runtime-daily-orchestrator@${RQUANT_DAILY_ORCHESTRATOR_INSTANCE}.timer"
}

daily_orchestrator_instance_is_valid() {
    [[ "$1" =~ ^[A-Za-z0-9_.-]+$ ]]
}

# 兼容 GNU date（生产 Linux）与 BSD date（本地 macOS 单测），解析 systemctl show
# NextElapseUSecRealtime 返回的人类可读时间戳，无法解析时打印 0。
epoch_seconds_from_timestamp() {
    date -d "$1" +%s 2>/dev/null && return 0
    date -j -f "%a %Y-%m-%d %H:%M:%S %Z" "$1" +%s 2>/dev/null && return 0
    echo 0
}

# ---------- 0. 前置检查 ----------

if [[ ${ACCEPT_DAILY_ORCHESTRATOR_TIMER} -eq 1 || ${START_DAILY_ORCHESTRATOR_TIMER} -eq 1 ]]; then
    if [[ -z "${RQUANT_DAILY_ORCHESTRATOR_INSTANCE:-}" ]]; then
        err "${DAILY_ORCHESTRATOR_TEMPLATE_TIMER}: RQUANT_DAILY_ORCHESTRATOR_INSTANCE is required"
        exit 1
    fi
    if ! daily_orchestrator_instance_is_valid "${RQUANT_DAILY_ORCHESTRATOR_INSTANCE}"; then
        err "${DAILY_ORCHESTRATOR_TEMPLATE_TIMER}: illegal RQUANT_DAILY_ORCHESTRATOR_INSTANCE '${RQUANT_DAILY_ORCHESTRATOR_INSTANCE}'"
        exit 1
    fi
fi

if [[ ${START_DAILY_ORCHESTRATOR_TIMER} -eq 1 ]]; then
    target_timer=$(daily_orchestrator_target_timer)
    step "受控启动 daily orchestrator timer: ${target_timer}（只 start，不 enable）"
    run "sudo systemctl start '${target_timer}'"
    active_state=$(systemctl show "${target_timer}" -p ActiveState --value 2>/dev/null)
    sub_state=$(systemctl show "${target_timer}" -p SubState --value 2>/dev/null)
    if [[ "${active_state}" != "active" || "${sub_state}" != "waiting" ]]; then
        err "${target_timer}: ActiveState=${active_state:-unknown} SubState=${sub_state:-unknown}，期望 active/waiting"
        exit 2
    fi
    next_raw=$(systemctl show "${target_timer}" -p NextElapseUSecRealtime --value 2>/dev/null)
    if [[ -z "${next_raw}" || "${next_raw}" == "n/a" || "${next_raw}" == "0" ]]; then
        err "${target_timer}: 下次 trigger = n/a（OnCalendar 可能被 systemd 拒收！）"
        exit 2
    fi
    next_s=$(epoch_seconds_from_timestamp "${next_raw}")
    if [[ "${next_s}" == "0" ]]; then
        err "${target_timer}: 无法解析下次 trigger 时间戳: '${next_raw}'"
        exit 2
    fi
    now_s=$(date +%s)
    delta=$((next_s - now_s))
    if [[ ${delta} -gt 31536000 ]]; then
        err "${target_timer}: 下次 trigger 在 $(( delta / 86400 ))d 后（> 1 年），OnCalendar 可能有问题"
        exit 2
    fi
    ok "${target_timer}: ActiveState=active SubState=waiting，下次 trigger 已验证"
    exit 0
fi

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

step "runtime credential 基础设施校准"
if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "  [dry-run] /bin/bash '${PROJECT_DIR}/scripts/install-runtime-credential-infra.sh'"
elif ! /bin/bash "${PROJECT_DIR}/scripts/install-runtime-credential-infra.sh"; then
    err "runtime credential 基础设施安装或校验失败"
    exit 1
fi
if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "  [dry-run] /bin/bash '${PROJECT_DIR}/scripts/install-workload-isolation-infra.sh'"
elif ! /bin/bash "${PROJECT_DIR}/scripts/install-workload-isolation-infra.sh"; then
    err "workload isolation candidate infrastructure reconcile failed"
    exit 1
fi
ok "credential sealer 与 sudoers 已校准"

if [[ "${PRE_HEAD}" == "${POST_HEAD}" ]]; then
    CHANGED=""
else
    CHANGED=$(git diff --name-only "${PRE_HEAD}..${POST_HEAD}")
    ok "${PRE_HEAD:0:7} → ${POST_HEAD:0:7}，变更："
    echo "${CHANGED}" | sed 's/^/    /'
fi

# ---------- 2. systemd unit 同步 ----------

step "[2/5] systemd unit 校准"
SYSTEMD_CHANGED=$(echo "${CHANGED}" | grep -E "^deploy/systemd/.*\.(service|timer|socket|slice)$" || true)
if [[ -n "${SYSTEMD_CHANGED}" ]]; then
    echo "  改动文件："
    echo "${SYSTEMD_CHANGED}" | sed 's|^deploy/systemd/|    |'
fi
run "sudo cp '${SYSTEMD_DIR}/'*.{service,timer,socket,slice} '${SYSTEMD_TARGET}/'"
run "sudo systemctl daemon-reload"
ok "service、timer、socket 与 slice 已校准，daemon-reload 完成"

if [[ "${PRE_HEAD}" == "${POST_HEAD}" && ${ACCEPT_DAILY_ORCHESTRATOR_TIMER} -ne 1 ]]; then
    ok "代码已最新（${PRE_HEAD:0:7}），基础设施校准完成。"
    exit 0
elif [[ "${PRE_HEAD}" == "${POST_HEAD}" ]]; then
    ok "代码已最新（${PRE_HEAD:0:7}），继续处理 daily orchestrator timer acceptance。"
fi

# ---------- 3. restart 已启用 timer；候选 timer 显式接受后只 enable 不 start ----------
#
# 关键修复（v0.11.3 翻车教训）：daemon-reload 只是让 systemd 重新读 unit 文件，
# 但已经在跑的 timer 实例仍用旧 OnCalendar。必须 restart 才能让新调度生效。
#
# - 已 enabled 的 timer：sudo systemctl restart（应用新 OnCalendar）
# - 未 enabled 的 timer：保持 disabled；Daily orchestrator 候选 timer 需显式接受，
#   且只 sudo systemctl enable，不 start

step "[3/6] 重启已启用 timer（让新 OnCalendar 生效）"
CHANGED_TIMER_FILES=$(echo "${SYSTEMD_CHANGED}" | grep "\.timer$" || true)
if [[ ${ACCEPT_DAILY_ORCHESTRATOR_TIMER} -eq 1 ]]; then
    CHANGED_TIMER_FILES=$(printf '%s\n%s\n' "${CHANGED_TIMER_FILES}" "deploy/systemd/${DAILY_ORCHESTRATOR_TEMPLATE_TIMER}" | grep -v '^$' | sort -u)
fi
TIMER_HANDLED=0
for f in ${CHANGED_TIMER_FILES}; do
    t=$(basename "${f}")
    target_timer="${t}"
    if [[ "${t}" == "${DAILY_ORCHESTRATOR_TEMPLATE_TIMER}" ]]; then
        if [[ ${ACCEPT_DAILY_ORCHESTRATOR_TIMER} -ne 1 ]]; then
            warn "${t}: daily orchestrator timer remains disabled by default; rerun with --accept-daily-orchestrator-timer after candidate acceptance"
            continue
        fi
        target_timer=$(daily_orchestrator_target_timer)
        run "sudo systemctl enable '${target_timer}'"
        ok "+ ${target_timer} (enabled only, 未 start)"
        TIMER_HANDLED=$((TIMER_HANDLED + 1))
        continue
    fi
    if [[ "${t}" == "rquant-research-ingest.timer" ]] && ! systemctl is-enabled "${t}" &>/dev/null; then
        warn "${t}: 研究日增量须按独立上线手册手工启用，跳过自动 enable"
        continue
    fi
    if systemctl is-enabled "${target_timer}" 2>/dev/null | grep -qE "^enabled"; then
        run "sudo systemctl restart '${target_timer}'"
        ok "↻ ${target_timer} (restarted, 应用新 OnCalendar)"
    else
        warn "${target_timer}: 未启用，跳过自动 enable/start"
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
        rquant-page-control.service)
            echo '^src/rquant/(page_control\.py|page_control_service\.py|canvas_publication_receipt\.py|serving_page_projection_source\.py|config\.py)'
            ;;
        rquant-nl-screen.service)
            echo '^src/rquant/(dashboard/nl_screen\.py|dashboard/__init__\.py|llm/|screen/|storage/|config\.py|presets\.py|state/)'
            ;;
    esac
}
LONG_RUNNING_SVCS="rquant-monitor.service rquant-dashboard.service rquant-page-control.service rquant-nl-screen.service"

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
# `systemctl status` 看起来是 active (waiting)，但下次 trigger 实际是 "n/a"。
# 这一步用 NextElapseUSecRealtime 检查改动 timer 的 NEXT：
#   - NEXT = "n/a" → 翻车（OnCalendar 没生效），exit 2 让 caller 知道
#   - NEXT 在过去 → 正在 catch up，warn
#   - NEXT 在 1 年内 → 正常（包括日常 timer 和长期一次性提醒 timer）
#   - NEXT > 1 年 → 极不寻常（如配错到 2099），exit 2
#
# 5/25 调整：阈值从 24h 放宽到 365d，支持长期一次性提醒（如 token 续费提醒
# OnCalendar=2027-03-13）。原 24h 阈值的初衷是检测 OnCalendar 被静默拒收，
# 但拒收的真正信号是 NEXT=n/a，不是 NEXT 远期，所以放宽到 1 年仍保护这一点。
#
# 跳过：dry-run、无 timer 改动

step "[5/6] post-deploy 验证：改动的 timer NEXT trigger 在 1 年内"
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
        if [[ "${t}" == "${DAILY_ORCHESTRATOR_TEMPLATE_TIMER}" ]]; then
            if [[ ${ACCEPT_DAILY_ORCHESTRATOR_TIMER} -ne 1 ]]; then
                warn "${t}: daily orchestrator timer 未接受，保持 disabled/inactive，跳过 NEXT trigger 验证"
                continue
            fi
            target_timer=$(daily_orchestrator_target_timer)
            unit_file_state=$(systemctl show "${target_timer}" -p UnitFileState --value 2>/dev/null)
            active_state=$(systemctl show "${target_timer}" -p ActiveState --value 2>/dev/null)
            if [[ "${unit_file_state}" != "enabled" ]]; then
                err "${target_timer}: UnitFileState=${unit_file_state:-unknown}，期望 enabled"
                TIMER_FAIL=1
                continue
            fi
            if [[ "${active_state}" != "inactive" && "${active_state}" != "dead" ]]; then
                err "${target_timer}: ActiveState=${active_state:-unknown}，期望 inactive/dead（accept 只 enable，不 start）"
                TIMER_FAIL=1
                continue
            fi
            ok "${target_timer}: UnitFileState=enabled, ActiveState=${active_state} (enabled only, NEXT 待受控启动后验证)"
            continue
        fi
        if [[ "${t}" == "rquant-research-ingest.timer" ]] && ! systemctl is-enabled "${t}" &>/dev/null; then
            warn "${t}: 尚未手工启用，跳过 NEXT trigger 验证"
            continue
        fi
        # systemd 252 (OpenCloudOS 9 / RHEL 9) 上 `show -p NextElapseUSecRealtime --value`
        # 返回的是**人类可读时间戳**（如 "Wed 2026-05-20 11:50:00 CST"），不是微秒数字。
        # 5/20 翻车：原版做 `next_s=$((next_us / 1000000))` 时，算术展开把 "Wed" 当
        # 变量名 → `set -u` 报 unbound variable。改用 date -d 解析时间戳字符串。
        next_raw=$(systemctl show "${t}" -p NextElapseUSecRealtime --value 2>/dev/null)
        if [[ -z "${next_raw}" || "${next_raw}" == "n/a" || "${next_raw}" == "0" ]]; then
            err "${t}: 下次 trigger = n/a（OnCalendar 可能被 systemd 拒收！）"
            err "  排查：systemctl status ${t} / systemd-analyze calendar '<OnCalendar 表达式>'"
            TIMER_FAIL=1
            continue
        fi
        # 复用 epoch_seconds_from_timestamp：兼容 GNU date（生产 Linux）与 BSD date（本地 macOS 单测）
        next_s=$(epoch_seconds_from_timestamp "${next_raw}")
        if [[ "${next_s}" == "0" ]]; then
            err "${t}: 无法解析下次 trigger 时间戳: '${next_raw}'"
            TIMER_FAIL=1
            continue
        fi
        now_s=$(date +%s)
        delta=$((next_s - now_s))
        if [[ ${delta} -lt 0 ]]; then
            warn "${t}: 下次 trigger 在过去（${delta}s 前），可能正在 catch up"
        elif [[ ${delta} -gt 31536000 ]]; then
            err "${t}: 下次 trigger 在 $(( delta / 86400 ))d 后（> 1 年），OnCalendar 可能有问题"
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
for s in rquant-monitor rquant-dashboard rquant-page-control rquant-nl-screen; do
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
