#!/usr/bin/env bash
# 把主库 rquant.duckdb 拷贝成只读副本 rquant_ro.duckdb，dashboard / canvas / nl-screen 读副本。
#
# 背景：DuckDB 单文件锁，monitor 盘中 9:25-15:00 持写锁期间，任何 read_only 连接都开不了
# （CLAUDE.md「单写多读」原描述有误：read_only 不能跟 writer 并存）。
# 副本是独立的文件，dashboard 永远能读，5min 延迟可接受。
#
# 设计：
#   1. cp 主库 + WAL（如有）→ tmp
#   2. 在 tmp 回放并 checkpoint WAL，再只读验证为单文件（防 cp 撞 monitor 写入）
#   3. 验证通过 → atomic mv 替换 WAL-free 副本；失败 → 保留上次成功副本
#
# 部署：rquant-replica-sync.timer 每 5min 触发，无需手动跑。

set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${PROJECT_DIR}/data/rquant.duckdb"
DST="${PROJECT_DIR}/data/rquant_ro.duckdb"
TMP="${DST}.tmp.$$"
LOG="${PROJECT_DIR}/logs/replica-sync.log"
VENV_PY="${PROJECT_DIR}/.venv/bin/python"

mkdir -p "$(dirname "${LOG}")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG}"; }
cleanup() { rm -f "${TMP}" "${TMP}.wal"; }
on_error() {
    local rc=$?
    trap - ERR
    log "ERROR: replica sync failed (exit=${rc})" || true
    exit "${rc}"
}
file_size() { stat -c %s "$1" 2>/dev/null || stat -f %z "$1"; }
trap cleanup EXIT
trap on_error ERR

if [[ ! -f "${SRC}" ]]; then
    log "ERROR: 主库不存在 ${SRC}"
    exit 1
fi

src_size=$(file_size "${SRC}")
log "sync start: src=${src_size}B"

# 1. cp 主库 + WAL 到 tmp（cp 本身不受 DuckDB advisory lock 阻塞）
if ! cp "${SRC}" "${TMP}"; then
    log "ERROR: cp 主库失败"
    exit 1
fi
if [[ -f "${SRC}.wal" ]]; then
    if ! cp "${SRC}.wal" "${TMP}.wal"; then
        log "ERROR: WAL cp 失败（可能 monitor 刚 checkpoint），本轮不发布"
        exit 2
    fi
fi

# 2. 在私有代际合并 WAL，并验证为包含业务表的单文件副本。
if ! table_count=$("${VENV_PY}" - "${TMP}" 2>>"${LOG}" <<'PY'
import os
import sys

import duckdb

path = sys.argv[1]
conn = duckdb.connect(path)
conn.execute("CHECKPOINT")
conn.close()
if os.path.exists(path + ".wal"):
    raise RuntimeError("temporary replica WAL remains after checkpoint")
conn = duckdb.connect(path, read_only=True)
conn.execute("SELECT 1").fetchone()
table_count = int(
    conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchone()[0]
)
conn.close()
if table_count < 1:
    raise RuntimeError("temporary replica contains no main-schema tables")
print(table_count)
PY
); then
    log "ERROR: tmp 副本 checkpoint/验证失败，保留旧副本"
    exit 2
fi

# 3. atomic mv 替换 WAL-free 副本（同分区 mv 是 rename(2) 原子操作）。
if ! mv "${TMP}" "${DST}"; then
    log "ERROR: mv tmp → dst 失败"
    exit 1
fi
rm -f "${DST}.wal"

dst_size=$(file_size "${DST}")
log "sync OK: dst=${dst_size}B, tables=${table_count}, wal_free=1"
