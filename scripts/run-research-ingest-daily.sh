#!/usr/bin/env bash
# Gate the research daily ingest on the completed daily pipeline and the replica
# prepared by the required systemd oneshot. The target date and retry count are
# fixed here so a failed service cannot drift across midnight or retry forever.

set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RQUANT_BIN="${PROJECT_DIR}/.venv/bin/rquant"
DAILY_UNIT="rquant-daily.service"
TARGET_DATE="$(TZ=Asia/Shanghai date +%F)"
MAX_ATTEMPTS="${RQUANT_RESEARCH_INGEST_MAX_ATTEMPTS:-4}"
RETRY_SECONDS="${RQUANT_RESEARCH_INGEST_RETRY_SECONDS:-900}"

if [[ ! "${MAX_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid RQUANT_RESEARCH_INGEST_MAX_ATTEMPTS=${MAX_ATTEMPTS}" >&2
    exit 1
fi
if [[ ! "${RETRY_SECONDS}" =~ ^[0-9]+$ ]]; then
    echo "invalid RQUANT_RESEARCH_INGEST_RETRY_SECONDS=${RETRY_SECONDS}" >&2
    exit 1
fi

attempt_once() {
    local daily_properties daily_active daily_result daily_status daily_exit
    local daily_exit_date
    daily_properties="$(
        systemctl show "${DAILY_UNIT}" \
            --property=ActiveState,Result,ExecMainStatus,ExecMainExitTimestamp
    )"
    daily_active="$(printf '%s\n' "${daily_properties}" | sed -n 's/^ActiveState=//p')"
    daily_result="$(printf '%s\n' "${daily_properties}" | sed -n 's/^Result=//p')"
    daily_status="$(printf '%s\n' "${daily_properties}" | sed -n 's/^ExecMainStatus=//p')"
    daily_exit="$(printf '%s\n' "${daily_properties}" | sed -n 's/^ExecMainExitTimestamp=//p')"
    daily_exit_date=""
    if [[ -n "${daily_exit}" ]]; then
        daily_exit_date="$(TZ=Asia/Shanghai date -d "${daily_exit}" +%F 2>/dev/null || true)"
    fi

    if [[ "${daily_active}" != "inactive" \
       || "${daily_result}" != "success" \
       || "${daily_status}" != "0" \
       || "${daily_exit_date}" != "${TARGET_DATE}" ]]; then
        echo "${DAILY_UNIT} did not complete successfully today" >&2
        echo "${daily_properties}" >&2
        return 1
    fi

    if ! "${RQUANT_BIN}" research-ingest-readiness --date "${TARGET_DATE}"; then
        echo "required read-only replica is not ready for ${TARGET_DATE}" >&2
        return 1
    fi
    "${RQUANT_BIN}" research-ingest --date "${TARGET_DATE}" --scheduled
}

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    echo "research ingest attempt ${attempt}/${MAX_ATTEMPTS} for ${TARGET_DATE}"
    set +e
    attempt_once
    status=$?
    set -e
    if [[ ${status} -eq 0 || ${status} -eq 2 || ${status} -eq 3 || ${status} -eq 75 ]]; then
        exit "${status}"
    fi
    if [[ ${attempt} -lt ${MAX_ATTEMPTS} ]]; then
        sleep "${RETRY_SECONDS}"
    fi
done

exit 1
