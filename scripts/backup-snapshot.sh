#!/usr/bin/env bash
# 服务器侧：生成 DuckDB 一致性快照到 backup/latest.duckdb.gz
# 通过 cp + gzip + atomic mv 保证 mac 拉到永远是完整文件。
# 极小概率 cp 时撞上 monitor 写入 → partial state，下次 cp 自动修复。

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_FILE="${PROJECT_DIR}/data/rquant.duckdb"
BACKUP_DIR="${PROJECT_DIR}/backup"
LOG="${PROJECT_DIR}/logs/backup-snapshot.log"

mkdir -p "${BACKUP_DIR}" "$(dirname "${LOG}")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG}"; }

if [[ ! -f "${DATA_FILE}" ]]; then
    log "ERROR: ${DATA_FILE} not found"
    exit 1
fi

src_size=$(stat -c %s "${DATA_FILE}" 2>/dev/null || stat -f %z "${DATA_FILE}")
log "snapshot start: src=${src_size}B"

# 1. cp 到 .tmp（同分区保证 mv 是 atomic）
cp "${DATA_FILE}" "${BACKUP_DIR}/latest.duckdb.tmp"

# 2. gzip 压缩（DuckDB 文件压缩比通常 30-50%）
gzip -f "${BACKUP_DIR}/latest.duckdb.tmp"
# 现在文件名是 latest.duckdb.tmp.gz

# 3. atomic rename
mv "${BACKUP_DIR}/latest.duckdb.tmp.gz" "${BACKUP_DIR}/latest.duckdb.gz"

# 4. 写元数据 JSON（dashboard 用）
gz_size=$(stat -c %s "${BACKUP_DIR}/latest.duckdb.gz" 2>/dev/null || stat -f %z "${BACKUP_DIR}/latest.duckdb.gz")
cat > "${BACKUP_DIR}/latest.json.tmp" <<EOF
{"snapshot_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)", "src_bytes": ${src_size}, "compressed_bytes": ${gz_size}}
EOF
mv "${BACKUP_DIR}/latest.json.tmp" "${BACKUP_DIR}/latest.json"

ratio=$(awk "BEGIN{printf \"%.0f\", ${gz_size}*100/${src_size}}")
log "snapshot OK: gz=${gz_size}B (${ratio}% of source)"
