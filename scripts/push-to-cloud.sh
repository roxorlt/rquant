#!/usr/bin/env bash
# 本地推文件到云端 (curl -T → nginx WebDAV PUT，与 sync-from-cloud.sh 对称)。
#
# 用法:
#   scripts/push-to-cloud.sh <local-file> [<remote-name>]
#
# 示例:
#   scripts/push-to-cloud.sh data/risk_blacklist.parquet
#   scripts/push-to-cloud.sh data/foo.csv mybackup.csv
#
# 文件类型白名单（nginx 端强制）: .parquet .csv .pdf .json
# 落点: 服务器 /home/lighthouse/rquant/data/uploads/<remote-name>
# 后续: 服务器宝塔 web 终端用 sudo mv uploads/<file> data/

set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"

if [[ $# -lt 1 ]]; then
    echo "用法: $0 <local-file> [<remote-name>]" >&2
    exit 2
fi

LOCAL_FILE="$1"
REMOTE_NAME="${2:-$(basename "${LOCAL_FILE}")}"

if [[ ! -f "${LOCAL_FILE}" ]]; then
    echo "ERROR: 本地文件不存在: ${LOCAL_FILE}" >&2
    exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "ERROR: .env not found at ${ENV_FILE}" >&2
    exit 1
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

: "${RQUANT_UPLOAD_USER:?missing RQUANT_UPLOAD_USER in .env}"
: "${RQUANT_UPLOAD_TOKEN:?missing RQUANT_UPLOAD_TOKEN in .env}"
: "${RQUANT_UPLOAD_URL:?missing RQUANT_UPLOAD_URL in .env}"

size=$(stat -f%z "${LOCAL_FILE}" 2>/dev/null || stat -c%s "${LOCAL_FILE}")
echo "推送 ${LOCAL_FILE} (${size} bytes) → ${RQUANT_UPLOAD_URL}/${REMOTE_NAME}"

http_code=$(curl -sS -o /tmp/push-response.txt -w "%{http_code}" \
    --max-time 600 \
    -u "${RQUANT_UPLOAD_USER}:${RQUANT_UPLOAD_TOKEN}" \
    -T "${LOCAL_FILE}" \
    "${RQUANT_UPLOAD_URL}/${REMOTE_NAME}")

if [[ "${http_code}" =~ ^20|^30 ]]; then
    echo "OK (HTTP ${http_code})"
    exit 0
else
    echo "FAILED (HTTP ${http_code})" >&2
    cat /tmp/push-response.txt >&2 2>/dev/null || true
    exit 1
fi
