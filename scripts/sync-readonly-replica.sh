#!/usr/bin/env bash
# 把主库 rquant.duckdb 拷贝成只读副本 rquant_ro.duckdb，dashboard / canvas / nl-screen 读副本。
#
# 背景：DuckDB 单文件锁，monitor 盘中 9:25-15:00 持写锁期间，任何 read_only 连接都开不了
# （CLAUDE.md「单写多读」原描述有误：read_only 不能跟 writer 并存）。
# 副本是独立的文件，dashboard 永远能读，5min 延迟可接受。
#
# 设计：
#   1. cp 主库 + WAL（如有）→ tmp
#   2. 用 DuckDB read_only 打开 tmp 验证可读（防 cp 时撞 monitor fsync → corrupt）
#   3. 验证通过 → atomic mv 替换副本；失败 → 保留上次成功副本，下个 timer 周期重试
#
# 部署：rquant-replica-sync.timer 每 5min 触发，无需手动跑。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${PROJECT_DIR}/data/rquant.duckdb"
DST="${PROJECT_DIR}/data/rquant_ro.duckdb"
TMP="${DST}.tmp.$$"
LOG="${PROJECT_DIR}/logs/replica-sync.log"
VENV_PY="${PROJECT_DIR}/.venv/bin/python"

mkdir -p "$(dirname "${LOG}")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG}"; }
cleanup() { rm -f "${TMP}" "${TMP}.wal"; }
trap cleanup EXIT

if [[ ! -f "${SRC}" ]]; then
    log "ERROR: 主库不存在 ${SRC}"
    exit 1
fi

src_size=$(stat -c %s "${SRC}")
log "sync start: src=${src_size}B"

# 1. cp 主库 + WAL 到 tmp（cp 本身不受 DuckDB advisory lock 阻塞）
if ! cp "${SRC}" "${TMP}"; then
    log "ERROR: cp 主库失败"
    exit 1
fi
if [[ -f "${SRC}.wal" ]]; then
    cp "${SRC}.wal" "${TMP}.wal" || log "WARN: WAL cp 失败（可能 monitor 刚 checkpoint）"
fi

# 2. 验证 tmp 可被 DuckDB read_only 打开（防 cp 拿到撕裂状态）
if ! "${VENV_PY}" -c "
import duckdb
conn = duckdb.connect('${TMP}', read_only=True)
conn.execute('SELECT 1').fetchone()
conn.close()
" 2>>"${LOG}"; then
    log "ERROR: tmp 副本验证失败（可能 cp 撞上 monitor 写入），保留旧副本"
    exit 2
fi

# 3. atomic mv 替换副本（同分区 mv 是 rename(2) 原子操作）
if ! mv "${TMP}" "${DST}"; then
    log "ERROR: mv tmp → dst 失败"
    exit 1
fi
if [[ -f "${TMP}.wal" ]]; then
    mv "${TMP}.wal" "${DST}.wal" || true
else
    # WAL 已合并进主库 → 删旧副本的 wal 避免开副本时 replay 旧 wal
    rm -f "${DST}.wal"
fi

dst_size=$(stat -c %s "${DST}")
log "sync OK: dst=${dst_size}B"
