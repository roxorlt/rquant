#!/usr/bin/env bash
# 本地从云端拉 backup snapshot 做热备
# - 走 HTTP basic auth（绕开 SSH/fail2ban）
# - 只在数据有变化的时段同步：盘中 09:30-15:05 + 日终 17:10-17:30
# - 失败重试 2 次（间隔 60s），最终失败 PushDeer 告警
# - 5/13 新增：检测源 stale（snapshot_at 持续不变），intraday 时段下推 PushDeer
#   防 v0.11.3 翻车再发：云端 backup.timer 假装在跑但 OnCalendar 被静默拒收
# - 7/2 分家：下载只落 cloud_backup.duckdb，不再整文件替换 rquant.duckdb。
#   原替换逻辑会把本地盘中 monitor 的写入打进被 unlink 的幽灵 inode，且残留
#   WAL 与新文件代际错配（7/2 主库损坏事故）。生产表在日终窗口由
#   `rquant research-sync` 合并进本地库，研究表按主键保留。
# - 7/2 加固：mkdir 锁防手动/launchd 并发互相截断 TMP 文件；日终合并按
#   data/.last-research-sync-date 记账，睡过 17:10-17:30 窗口后任意 tick
#   追赶补跑直到成功；--force 显式下载+合并（无论时段）；合并失败告警
#   加 30min cooldown 防 catch_up 重试刷屏

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_DATA_FILE="${PROJECT_DIR}/data/cloud_backup.duckdb"
LOG_DIR="${PROJECT_DIR}/logs"
LOG="${LOG_DIR}/sync-from-cloud.log"
ENV_FILE="${PROJECT_DIR}/.env"
STATE_DIR="${PROJECT_DIR}/data"
LAST_RESEARCH_SYNC_FILE="${STATE_DIR}/.last-research-sync-date"
LAST_MERGE_ALERT_FILE="${STATE_DIR}/.last-merge-alert-at"
MERGE_ALERT_COOLDOWN=1800  # 30 分钟：catch_up 每 5min 重试，失败别每 tick 都推

# ---------- 参数 ----------
force_mode=0
skip_post_sync_captures=0
while (( $# > 0 )); do
    case "$1" in
        --force)
            force_mode=1
            ;;
        --skip-post-sync-captures)
            skip_post_sync_captures=1
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
    shift
done

if ! mkdir -p "${LOG_DIR}" "${STATE_DIR}"; then
    echo "ERROR: failed to create data/log directories" >&2
    exit 1
fi
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG}"; }

# ---------- 并发互斥（7/2 加固）----------
# TMP_GZ/TMP_DB 是固定路径，手动运行与 launchd tick 并发会互相截断。
# macOS 无 flock 命令，用 mkdir 原子锁目录 + PID 所有权代替。SIGKILL
# 遗留目录由下一次运行识别死 PID 后恢复；trap 只删除自己持有的锁。
LOCKDIR="${STATE_DIR}/.sync-from-cloud.lock"
LOCK_PID_FILE="${LOCKDIR}/pid"
LOCK_PID_TMP_FILE="${LOCK_PID_FILE}.tmp.$$"
LOCK_PID_PUBLISH_RETRIES=8
LOCK_PID_PUBLISH_DELAY=0.1
lock_owned=0
lock_pid=""
lock_pid_present=0

acquire_lock() {
    if ! mkdir "${LOCKDIR}" 2>/dev/null; then
        return 1
    fi
    if ! printf '%s\n' "$$" > "${LOCK_PID_TMP_FILE}"; then
        rm -f "${LOCK_PID_TMP_FILE}"
        rmdir "${LOCKDIR}" 2>/dev/null || true
        return 2
    fi
    if ! mv "${LOCK_PID_TMP_FILE}" "${LOCK_PID_FILE}"; then
        rm -f "${LOCK_PID_TMP_FILE}"
        rmdir "${LOCKDIR}" 2>/dev/null || true
        return 2
    fi
    lock_owned=1
    return 0
}

cleanup_lock() {
    local owner_pid=""
    rm -f "${LOCK_PID_TMP_FILE}"
    if (( lock_owned != 1 )); then
        return
    fi
    [[ -f "${LOCK_PID_FILE}" ]] && owner_pid=$(cat "${LOCK_PID_FILE}" 2>/dev/null || true)
    if [[ "${owner_pid}" == "$$" ]]; then
        rm -f "${LOCK_PID_FILE}"
        rmdir "${LOCKDIR}" 2>/dev/null || true
    fi
    lock_owned=0
}

read_lock_owner() {
    lock_pid=""
    lock_pid_present=0
    if [[ -f "${LOCK_PID_FILE}" ]]; then
        lock_pid_present=1
        lock_pid=$(cat "${LOCK_PID_FILE}" 2>/dev/null || true)
    fi
}

lock_owner_is_active() {
    lock_owner_is_complete && kill -0 "${lock_pid}" 2>/dev/null
}

lock_owner_is_complete() {
    [[ "${lock_pid}" =~ ^[1-9][0-9]*$ ]]
}

wait_for_lock_owner_publication() {
    local attempt
    for (( attempt = 0; attempt < LOCK_PID_PUBLISH_RETRIES; attempt += 1 )); do
        sleep "${LOCK_PID_PUBLISH_DELAY}"
        read_lock_owner
        if lock_owner_is_complete; then
            return 0
        fi
    done
    return 1
}

inspect_interrupted_lock_publication() {
    local dotglob_was_set=0
    local nullglob_was_set=0
    local entry
    local tmp_file
    local tmp_pid
    local confirmed_tmp_pid
    local -a entries=()
    local -a tmp_files=()
    local -a unknown_entries=()

    shopt -q dotglob && dotglob_was_set=1
    shopt -q nullglob && nullglob_was_set=1
    shopt -s dotglob nullglob
    entries=("${LOCKDIR}"/*)
    (( dotglob_was_set == 1 )) || shopt -u dotglob
    (( nullglob_was_set == 1 )) || shopt -u nullglob

    for entry in "${entries[@]}"; do
        if [[ "${entry}" == "${LOCK_PID_FILE}" ]]; then
            continue
        fi
        if [[ "${entry}" == "${LOCK_PID_FILE}.tmp."* ]] \
                && [[ -f "${entry}" ]] && [[ ! -L "${entry}" ]]; then
            tmp_files+=("${entry}")
        else
            unknown_entries+=("${entry}")
        fi
    done

    if (( ${#unknown_entries[@]} > 0 || ${#tmp_files[@]} > 1 )); then
        return 2
    fi
    if (( ${#tmp_files[@]} == 0 )); then
        return 0
    fi

    tmp_file="${tmp_files[0]}"
    tmp_pid=$(cat "${tmp_file}" 2>/dev/null || true)
    if [[ "${tmp_pid}" =~ ^[1-9][0-9]*$ ]] \
            && kill -0 "${tmp_pid}" 2>/dev/null; then
        lock_pid="${tmp_pid}"
        return 4
    fi

    if [[ ! -f "${tmp_file}" ]] || [[ -L "${tmp_file}" ]]; then
        return 3
    fi
    confirmed_tmp_pid=$(cat "${tmp_file}" 2>/dev/null || true)
    if [[ "${confirmed_tmp_pid}" != "${tmp_pid}" ]]; then
        return 3
    fi
    if [[ "${confirmed_tmp_pid}" =~ ^[1-9][0-9]*$ ]] \
            && kill -0 "${confirmed_tmp_pid}" 2>/dev/null; then
        lock_pid="${confirmed_tmp_pid}"
        return 4
    fi
    if ! rm -f "${tmp_file}"; then
        return 2
    fi
    log "recover stale lock publication: pid=${tmp_pid:-invalid} (${tmp_file})"
    return 5
}

exit_for_active_lock() {
    log "skip: active sync-from-cloud pid=${lock_pid} holds lock (${LOCKDIR})"
    if (( force_mode == 1 )); then
        exit 75
    fi
    exit 0
}

recover_observed_stale_lock() {
    local observed_pid="$1"
    local observed_pid_present="$2"

    read_lock_owner
    if [[ "${lock_pid}" != "${observed_pid}" ]] \
            || (( lock_pid_present != observed_pid_present )); then
        return 3
    fi
    if lock_owner_is_active; then
        return 4
    fi

    if (( observed_pid_present == 0 )); then
        if rmdir "${LOCKDIR}" 2>/dev/null; then
            log "recover stale lock: pid=missing (${LOCKDIR})"
            return 0
        fi
        return 3
    fi

    log "recover stale lock: pid=${observed_pid:-invalid} (${LOCKDIR})"
    if ! rm -f "${LOCK_PID_FILE}"; then
        return 2
    fi
    if rmdir "${LOCKDIR}" 2>/dev/null; then
        return 0
    fi
    return 3
}

if acquire_lock; then
    :
else
    lock_result=$?
    if (( lock_result == 2 )) || [[ ! -d "${LOCKDIR}" ]]; then
        log "ERROR: failed to create sync lock (${LOCKDIR})"
        exit 1
    fi

    stale_lock_removed=0
    for (( lock_check = 0; lock_check < 3; lock_check += 1 )); do
        read_lock_owner
        if ! lock_owner_is_complete; then
            wait_for_lock_owner_publication || true
        fi
        if lock_owner_is_active; then
            exit_for_active_lock
        fi
        if ! lock_owner_is_complete; then
            inspect_interrupted_lock_publication
            interrupted_result=$?
            case "${interrupted_result}" in
                0|5)
                    ;;
                2)
                    log "ERROR: interrupted lock publication is not safely removable (${LOCKDIR})"
                    exit 1
                    ;;
                4)
                    exit_for_active_lock
                    ;;
                3)
                    continue
                    ;;
            esac
        fi

        observed_pid="${lock_pid}"
        observed_pid_present=${lock_pid_present}
        recover_observed_stale_lock "${observed_pid}" "${observed_pid_present}"
        recover_result=$?
        case "${recover_result}" in
            0)
                stale_lock_removed=1
                break
                ;;
            2)
                log "ERROR: stale lock is not safely removable (${LOCKDIR})"
                exit 1
                ;;
            4)
                exit_for_active_lock
                ;;
            3)
                continue
                ;;
        esac
    done
    if (( stale_lock_removed != 1 )); then
        read_lock_owner
        if lock_owner_is_active; then
            exit_for_active_lock
        fi
        log "ERROR: sync lock changed during stale recovery (${LOCKDIR})"
        exit 1
    fi

    if acquire_lock; then
        :
    else
        retry_result=$?
        if [[ -d "${LOCKDIR}" ]]; then
            read_lock_owner
            if ! lock_owner_is_complete; then
                wait_for_lock_owner_publication || true
            fi
            if lock_owner_is_active; then
                exit_for_active_lock
            fi
            if ! lock_owner_is_complete; then
                inspect_interrupted_lock_publication
                interrupted_result=$?
                case "${interrupted_result}" in
                    4)
                        exit_for_active_lock
                        ;;
                    2|3)
                        log "ERROR: interrupted lock publication changed after stale recovery (${LOCKDIR})"
                        exit 1
                        ;;
                    5)
                        read_lock_owner
                        observed_pid="${lock_pid}"
                        observed_pid_present=${lock_pid_present}
                        recover_observed_stale_lock \
                            "${observed_pid}" "${observed_pid_present}"
                        retry_cleanup_result=$?
                        if (( retry_cleanup_result == 4 )); then
                            exit_for_active_lock
                        fi
                        log "ERROR: stale lock publication interrupted lock retry (${LOCKDIR})"
                        exit 1
                        ;;
                esac
            fi
            log "skip: another sync-from-cloud won stale-lock recovery (${LOCKDIR})"
            if (( force_mode == 1 )); then
                exit 75
            fi
            exit 0
        fi
        log "ERROR: failed to create sync lock after stale recovery (${LOCKDIR}, rc=${retry_result})"
        exit 1
    fi
fi
trap cleanup_lock EXIT

# 加载 .env
if [[ ! -f "${ENV_FILE}" ]]; then
    log "ERROR: .env not found at ${ENV_FILE}"
    exit 1
fi
# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

: "${RQUANT_BACKUP_USER:?missing RQUANT_BACKUP_USER in .env}"
: "${RQUANT_BACKUP_TOKEN:?missing RQUANT_BACKUP_TOKEN in .env}"
: "${RQUANT_BACKUP_URL:?missing RQUANT_BACKUP_URL in .env}"

send_sync_failure_alert() {
    local failure_detail="$1"
    local now_ts
    local should_alert=1
    local last_alert_ts=""
    local remain
    local keys
    local endpoint
    local title
    local body
    local key
    local k
    local -a key_arr=()

    now_ts=$(date +%s)
    if [[ -f "${LAST_MERGE_ALERT_FILE}" ]]; then
        last_alert_ts=$(cat "${LAST_MERGE_ALERT_FILE}")
        if [[ "${last_alert_ts}" =~ ^[0-9]+$ ]] \
                && (( now_ts - last_alert_ts < MERGE_ALERT_COOLDOWN )); then
            should_alert=0
            remain=$(( MERGE_ALERT_COOLDOWN - (now_ts - last_alert_ts) ))
            log "sync alert cooldown: ${remain}s 后才能再推"
        fi
    fi
    if (( should_alert == 0 )); then
        return
    fi

    keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
    title="❌ rQuant sync-from-cloud 失败"
    body="云端备份同步未完整完成，下一个 tick 会自动重试。

时间：$(date '+%Y-%m-%d %H:%M:%S')
备份：${LOCAL_DATA_FILE}
原因：${failure_detail}

最近日志：
\`\`\`
$(tail -n 15 "${LOG}")
\`\`\`"
    IFS=',' read -ra key_arr <<< "${keys}"
    for key in "${key_arr[@]}"; do
        k=$(echo "${key}" | xargs)
        [[ -n "${k}" ]] || continue
        curl -s -X POST "${endpoint}" \
            --data-urlencode "pushkey=${k}" \
            --data-urlencode "text=${title}" \
            --data-urlencode "desp=${body}" \
            --data-urlencode "type=markdown" \
            --max-time 10 >/dev/null 2>&1 || true
    done
    echo "${now_ts}" > "${LAST_MERGE_ALERT_FILE}"
    log "PushDeer sync 失败告警已推（cooldown ${MERGE_ALERT_COOLDOWN}s）"
}

# 时段判断
hour=$(date +%H); minute=$(date +%M); dow=$(date +%u)
hhmm=$((10#${hour} * 100 + 10#${minute}))
today=$(date '+%Y-%m-%d')
sync_window=""
if (( hhmm >= 930 && hhmm <= 1505 )); then
    sync_window="intraday"
elif (( hhmm >= 1710 && hhmm <= 1730 )); then
    sync_window="daily_after_pipeline"
elif (( hhmm >= 1710 && dow <= 5 )); then
    # 日终追赶（7/2 加固）：笔记本合盖睡过 17:10-17:30 窗口时，工作日
    # >=17:10 的任意 tick 只要当天还没成功合并过就补跑，直到成功为止
    last_research_sync=""
    [[ -f "${LAST_RESEARCH_SYNC_FILE}" ]] && last_research_sync=$(cat "${LAST_RESEARCH_SYNC_FILE}")
    if [[ "${last_research_sync}" != "${today}" ]]; then
        sync_window="catch_up"
    fi
fi

if (( force_mode == 1 )); then
    log "sync window: forced (manual)"
elif [[ -z "${sync_window}" ]]; then
    log "skip: not in sync window (hhmm=${hhmm}, use --force to override)"
    exit 0
elif [[ "${sync_window}" == "intraday" && "${dow}" -gt 5 ]]; then
    log "skip: weekend (dow=${dow})"
    exit 0
else
    log "sync window: ${sync_window}"
fi

TMP_GZ="${LOCAL_DATA_FILE}.gz.tmp"
TMP_DB="${LOCAL_DATA_FILE}.tmp"

# curl 失败重试 2 次（HTTP 失败不触发 fail2ban，比 SSH rsync 重试安全得多）
ok=0
http_status=""
for attempt in 1 2; do
    log "curl attempt ${attempt}/2"
    # --max-time 600（10 分钟）：234MB 源文件 gzip 后 80-120MB，
    # 5Mbps 带宽下需 ~3-5 分钟，留余量
    http_status=$(curl -sS --fail --max-time 600 \
            --user "${RQUANT_BACKUP_USER}:${RQUANT_BACKUP_TOKEN}" \
            -o "${TMP_GZ}" \
            -w "%{http_code}" \
            "${RQUANT_BACKUP_URL}/latest.duckdb.gz" 2>>"${LOG}") && {
        ok=1
        break
    } || true
    log "curl failed (attempt ${attempt}, http_status=${http_status})"
    sleep 60
done

if (( ok == 0 )); then
    log "ERROR: curl failed 2 times (last status=${http_status}), sending PushDeer alert"

    keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
    endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
    title="❌ rQuant 备份同步失败"
    body="curl 拉云端 backup 失败 2 次。

时间：$(date '+%Y-%m-%d %H:%M:%S')
URL：${RQUANT_BACKUP_URL}/latest.duckdb.gz
HTTP 状态：${http_status}

最近日志：
\`\`\`
$(tail -n 15 "${LOG}")
\`\`\`"
    IFS=',' read -ra KEY_ARR <<< "${keys}"
    for key in "${KEY_ARR[@]}"; do
        k=$(echo "${key}" | xargs)
        [[ -n "${k}" ]] || continue
        curl -s -X POST "${endpoint}" \
            --data-urlencode "pushkey=${k}" \
            --data-urlencode "text=${title}" \
            --data-urlencode "desp=${body}" \
            --data-urlencode "type=markdown" \
            --max-time 10 >/dev/null 2>&1 || true
    done
    rm -f "${TMP_GZ}"
    exit 1
fi

# 解压（用 gzip -dc 替代 gunzip，避免对 .tmp 后缀严格检查）
if ! gzip -dc "${TMP_GZ}" > "${TMP_DB}" 2>>"${LOG}"; then
    log "ERROR: gunzip failed"
    rm -f "${TMP_GZ}" "${TMP_DB}"
    exit 1
fi
rm -f "${TMP_GZ}"

# atomic rename → 本地始终是完整文件；顺手清掉备份文件的陈旧 WAL
# （备份是纯下载工件，本地不该有进程写它，残留 WAL 只可能是垃圾）
rm -f "${LOCAL_DATA_FILE}.wal"
if ! mv "${TMP_DB}" "${LOCAL_DATA_FILE}"; then
    log "ERROR: failed to replace local cloud backup"
    rm -f "${TMP_DB}"
    exit 1
fi
size=$(du -h "${LOCAL_DATA_FILE}" | cut -f1)

# ---------- 生产表合并（7/2 分家新增）----------
# 盘中不合并：本地 monitor 持研究库写锁，research-sync 会撞锁，且盘中
# 生产表本来就没有新日线数据。--force 例外：显式下载 + 合并（无论时段），
# 合并失败风险自担（见下方失败日志提示）
merge_status="skipped"
post_sync_status="skipped"
final_status=0
if (( force_mode == 1 )) || [[ "${sync_window}" != "intraday" ]]; then
    RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
    if "${RQUANT_BIN}" research-sync --backup "${LOCAL_DATA_FILE}" >>"${LOG}" 2>&1; then
        merge_status="ok"
        log "research-sync OK: 生产表已合并进本地研究库"
        if (( skip_post_sync_captures == 0 )); then
            captures_ok=1
            # 涨停池只有当天有数据，历史无法回补；云端东财源被屏蔽，只能本地日终采
            if ! "${RQUANT_BIN}" zt-pool-capture >>"${LOG}" 2>&1; then
                captures_ok=0
                post_sync_status="failed"
                final_status=1
                log "ERROR: zt-pool-capture failed（当天数据不可历史回补，状态不记完成）"
                send_sync_failure_alert "zt-pool-capture 失败（当天数据不可历史回补）"
            fi
            # 官方涨跌停榜（tushare limit_list_d）当日增量；漏采可事后 limit-list-backfill 补
            if ! "${RQUANT_BIN}" limit-list-backfill --today >>"${LOG}" 2>&1; then
                captures_ok=0
                [[ "${post_sync_status}" == "failed" ]] || post_sync_status="incomplete"
                log "WARN: limit-list-backfill --today failed（可事后回补，状态不记完成）"
            fi
            # 开盘啦题材成分快照（30 天窗口整表替换，全景页涨停排行用）；失败次日重跑即补
            if ! "${RQUANT_BIN}" data-backfill --dataset kpl_concept --today >>"${LOG}" 2>&1; then
                captures_ok=0
                [[ "${post_sync_status}" == "failed" ]] || post_sync_status="incomplete"
                log "WARN: data-backfill kpl_concept failed（快照可次日重跑，状态不记完成）"
            fi
            if (( captures_ok == 1 )); then
                post_sync_status="ok"
            fi
        else
            post_sync_status="skipped"
            log "post-sync captures skipped by --skip-post-sync-captures"
        fi

        sync_complete=0
        if (( skip_post_sync_captures == 1 )) || [[ "${post_sync_status}" == "ok" ]]; then
            sync_complete=1
        fi
        # 盘前/盘中 force 不含当晚 daily pipeline 数据，不能吃掉日终 catch_up。
        if (( sync_complete == 1 && hhmm >= 1710 )); then
            if ! echo "${today}" > "${LAST_RESEARCH_SYNC_FILE}"; then
                final_status=1
                log "ERROR: failed to record completed research sync"
                send_sync_failure_alert "日终同步完成状态写入失败"
            fi
        fi
    else
        merge_status="failed"
        final_status=1
        if (( force_mode == 1 )); then
            log "ERROR: research-sync failed（force 模式：可能撞盘中 monitor 写锁，请收盘后重试）"
        else
            log "ERROR: research-sync failed（状态文件不更新，下一个 tick 自动重试补跑）"
        fi

        send_sync_failure_alert "research-sync 失败（主库合并或副本刷新未完成）"
    fi
fi

# ---------- Stale 检测（5/13 新增）----------
# 拉 latest.json 比较 snapshot_at，identify 云端 backup 是否真在更新
# 通用约定：服务端 backup-snapshot.sh 写 latest.json，含 {"snapshot_at": "<ISO>"}
JSON_URL="${RQUANT_BACKUP_URL}/latest.json"
TMP_JSON="${LOCAL_DATA_FILE}.json.tmp"
LAST_SNAPSHOT_FILE="${STATE_DIR}/.last-sync-snapshot-at"
LAST_STALE_ALERT_FILE="${STATE_DIR}/.last-stale-alert-at"
STALE_ALERT_COOLDOWN=1800  # 30 分钟，避免 stale 持续时每 5min 都告警刷屏

# 合并失败时后续日志不再谎报 "sync OK"（7/2 加固）
sync_label="sync OK"
if [[ "${merge_status}" == "failed" ]]; then
    sync_label="download OK, merge FAILED"
elif [[ "${post_sync_status}" == "failed" ]]; then
    sync_label="download/merge OK, post-sync FAILED"
elif [[ "${post_sync_status}" == "incomplete" ]]; then
    sync_label="download/merge OK, post-sync INCOMPLETE"
fi

new_snapshot=""
if curl -sS --fail --max-time 30 \
        --user "${RQUANT_BACKUP_USER}:${RQUANT_BACKUP_TOKEN}" \
        -o "${TMP_JSON}" \
        "${JSON_URL}" 2>>"${LOG}"; then
    # 解析 "snapshot_at": "2026-05-13T06:30:01Z" → 取引号间的 value
    new_snapshot=$(grep -oE '"snapshot_at"[[:space:]]*:[[:space:]]*"[^"]*"' "${TMP_JSON}" 2>/dev/null \
                   | head -n1 | sed -E 's/.*:[[:space:]]*"([^"]*)"$/\1/' || true)
    rm -f "${TMP_JSON}"
fi

if [[ -z "${new_snapshot}" ]]; then
    log "${sync_label}: ${size} (latest.json 拉取/解析失败，跳过 stale 检查)"
else
    last_snapshot=""
    [[ -f "${LAST_SNAPSHOT_FILE}" ]] && last_snapshot=$(cat "${LAST_SNAPSHOT_FILE}")

    if [[ -z "${last_snapshot}" ]]; then
        # 首次 sync，建立 baseline
        echo "${new_snapshot}" > "${LAST_SNAPSHOT_FILE}"
        log "${sync_label}: ${size} (baseline snapshot_at=${new_snapshot})"
    elif [[ "${last_snapshot}" != "${new_snapshot}" ]]; then
        # 源文件有更新，正常
        echo "${new_snapshot}" > "${LAST_SNAPSHOT_FILE}"
        log "${sync_label}: ${size} (snapshot_at=${new_snapshot})"
    elif [[ "${sync_window}" != "intraday" ]]; then
        # 非 intraday 时段（如 daily_after_pipeline 17:10-17:30 抓的还是 17:30 那份）拉到一致是正常
        log "${sync_label}: ${size} (snapshot_at 持续 ${new_snapshot}，非 intraday 时段，正常)"
    else
        # intraday 时段下源文件不动 → backup.timer 可能没在跑（v0.11.3 风险）
        log "WARN: source stale (snapshot_at=${new_snapshot} 跟上次一致); intraday backup 可能已停"

        # 防刷屏：距上次 stale 告警 ≥ STALE_ALERT_COOLDOWN 才推
        now_ts=$(date +%s)
        should_alert=1
        if [[ -f "${LAST_STALE_ALERT_FILE}" ]]; then
            last_alert_ts=$(cat "${LAST_STALE_ALERT_FILE}")
            if (( now_ts - last_alert_ts < STALE_ALERT_COOLDOWN )); then
                should_alert=0
                remain=$(( STALE_ALERT_COOLDOWN - (now_ts - last_alert_ts) ))
                log "stale alert cooldown: ${remain}s 后才能再推"
            fi
        fi

        if (( should_alert == 1 )); then
            keys=$(grep "^PUSHDEER_KEYS=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
            endpoint=$(grep "^PUSHDEER_ENDPOINT=" "${ENV_FILE}" | cut -d= -f2 | tr -d '\n\r')
            endpoint="${endpoint:-https://api2.pushdeer.com/message/push}"
            title="[RQ][WARN] backup intraday 卡住"
            body="本地 sync 拉到的 snapshot_at 持续不变（${new_snapshot}），
intraday 时段（盘中）云端 backup 应每 5min 步进。
可能 rquant-backup.timer 没在调度（v0.11.3 风险再发）。

时间：$(date '+%Y-%m-%d %H:%M:%S')
URL：${RQUANT_BACKUP_URL}/latest.duckdb.gz

排查：
1. ssh 上服务器：systemctl list-timers rquant-backup.timer
2. NEXT 应在 5min 内（不是 17:30）
3. 看 backup.service 日志：journalctl -u rquant-backup.service --since '1 hour ago'"
            IFS=',' read -ra KEY_ARR <<< "${keys}"
            for key in "${KEY_ARR[@]}"; do
                k=$(echo "${key}" | xargs)
                [[ -n "${k}" ]] || continue
                curl -s -X POST "${endpoint}" \
                    --data-urlencode "pushkey=${k}" \
                    --data-urlencode "text=${title}" \
                    --data-urlencode "desp=${body}" \
                    --data-urlencode "type=markdown" \
                    --max-time 10 >/dev/null 2>&1 || true
            done
            echo "${now_ts}" > "${LAST_STALE_ALERT_FILE}"
            log "PushDeer stale 告警已推（cooldown ${STALE_ALERT_COOLDOWN}s）"
        fi
    fi
fi

exit "${final_status}"
