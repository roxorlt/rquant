#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RQUANT_MIGRATION_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"
SOURCE_DB="${RQUANT_MIGRATION_SOURCE_DB:-${PROJECT_DIR}/data/rquant.duckdb}"
RECOVERY_DIR="${RQUANT_MIGRATION_RECOVERY_DIR:-${PROJECT_DIR}/data/research_migration/recovery}"
BUNDLE_DIR="${RQUANT_MIGRATION_BUNDLE_DIR:-${PROJECT_DIR}/data/research_migration/bundles}"
ARTIFACT_DIR="${RQUANT_MIGRATION_ARTIFACT_DIR:-${PROJECT_DIR}/data/strategy_lab_runs}"
REMOTE="${RQUANT_MIGRATION_REMOTE:-lighthouse@82.156.0.68}"
REMOTE_REPO="${RQUANT_MIGRATION_REMOTE_REPO:-/home/lighthouse/rquant}"
REMOTE_STAGING="${REMOTE_REPO}/data/research-staging"
REMOTE_DATA_DIR="${REMOTE_REPO}/data"
PGREP_BIN="${RQUANT_MIGRATION_PGREP_BIN:-pgrep}"
SSH_BIN="${RQUANT_MIGRATION_SSH_BIN:-ssh}"
RSYNC_BIN="${RQUANT_MIGRATION_RSYNC_BIN:-rsync}"
SPACE_MULTIPLIER=2
SPACE_RESERVE_BYTES=1073741824
PUBLISH_TIMEOUT_SECONDS="${RQUANT_MIGRATION_PUBLISH_TIMEOUT_SECONDS:-21600}"

phase=""
snapshot_id=""
start_date=""
end_date=""
dry_run=0

usage() {
    cat <<'EOF'
Usage: scripts/migrate-research-to-cloud.sh \
  --phase prepare|upload|publish|all \
  --snapshot-id research-YYYYMMDDTHHMMSSZ-xxxxxxxx \
  [--start-date YYYY-MM-DD --end-date YYYY-MM-DD] [--dry-run]

prepare and all require both dates. upload resumes into a snapshot-specific
remote staging directory. publish verifies that staging directory before apply.
EOF
}

while (( $# > 0 )); do
    case "$1" in
        --phase)
            phase="${2:-}"
            shift 2
            ;;
        --snapshot-id)
            snapshot_id="${2:-}"
            shift 2
            ;;
        --start-date)
            start_date="${2:-}"
            shift 2
            ;;
        --end-date)
            end_date="${2:-}"
            shift 2
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! "${phase}" =~ ^(prepare|upload|publish|all)$ ]]; then
    printf 'Invalid or missing --phase\n' >&2
    exit 2
fi
if [[ ! "${snapshot_id}" =~ ^research-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$ ]]; then
    printf 'Invalid or missing --snapshot-id\n' >&2
    exit 2
fi
if [[ "${phase}" == "prepare" || "${phase}" == "all" ]]; then
    if [[ ! "${start_date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
            || [[ ! "${end_date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
            || [[ "${start_date}" > "${end_date}" ]]; then
        printf 'prepare requires a valid inclusive date range\n' >&2
        exit 2
    fi
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
    printf 'Migration Python is not executable: %s\n' "${PYTHON_BIN}" >&2
    exit 2
fi
if [[ ! "${PUBLISH_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
    printf 'RQUANT_MIGRATION_PUBLISH_TIMEOUT_SECONDS must be a positive integer\n' >&2
    exit 2
fi

export PYTHONPATH="${PROJECT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"
CLI=("${PYTHON_BIN}" -m rquant.cli)
BUNDLE_PATH="${BUNDLE_DIR}/${snapshot_id}"
SOURCE_SNAPSHOT="${RECOVERY_DIR}/${snapshot_id}/rquant.duckdb"
REMOTE_BUNDLE="${REMOTE_STAGING}/${snapshot_id}"

print_command() {
    printf '[dry-run]'
    printf ' %q' "$@"
    printf '\n'
}

run() {
    if (( dry_run == 1 )); then
        print_command "$@"
        return 0
    fi
    "$@"
}

remote_run() {
    local command="$1"
    if (( dry_run == 1 )); then
        printf '[dry-run] %q %q -- %s\n' "${SSH_BIN}" "${REMOTE}" "${command}"
        return 0
    fi
    "${SSH_BIN}" "${REMOTE}" "${command}"
}

regular_file_size() {
    "${PYTHON_BIN}" -c \
        'import pathlib,sys; p=pathlib.Path(sys.argv[1]); print(p.stat().st_size)' "$1"
}

local_available_bytes() {
    local probe="$1"
    local available_kb
    available_kb=$(df -Pk "${probe}" | awk 'NR == 2 {print $4}')
    if [[ ! "${available_kb}" =~ ^[0-9]+$ ]]; then
        printf 'Cannot determine local free space for %s\n' "${probe}" >&2
        exit 1
    fi
    printf '%s\n' "$(( available_kb * 1024 ))"
}

require_local_space() {
    local expected_bytes="$1"
    local available_bytes
    local required_bytes=$(( expected_bytes * SPACE_MULTIPLIER + SPACE_RESERVE_BYTES ))
    available_bytes=$(local_available_bytes "$(dirname -- "${SOURCE_DB}")")
    if (( available_bytes < required_bytes )); then
        printf 'Insufficient local space: available=%s required=%s\n' \
            "${available_bytes}" "${required_bytes}" >&2
        exit 1
    fi
}

require_prepare_window() {
    local weekday hhmm
    if (( dry_run == 1 )); then
        return 0
    fi
    if "${PGREP_BIN}" -f 'python.*rquant.*monitor|/rquant monitor' >/dev/null 2>&1; then
        printf 'Refusing migration prepare: local monitor is running\n' >&2
        exit 1
    fi
    if "${PGREP_BIN}" -f 'strategy_lab_worker|rquant.*lab.*worker' >/dev/null 2>&1; then
        printf 'Refusing migration prepare: strategy lab worker is running\n' >&2
        exit 1
    fi
    weekday=$(TZ=Asia/Shanghai date +%u)
    hhmm=$(TZ=Asia/Shanghai date +%H%M)
    if (( weekday <= 5 && 10#${hhmm} >= 915 && 10#${hhmm} <= 1510 )); then
        printf 'Refusing migration prepare during 09:15-15:10 Asia/Shanghai\n' >&2
        exit 1
    fi
}

code_commit() {
    local commit
    if (( dry_run == 0 )) \
            && { ! git -C "${PROJECT_DIR}" diff --quiet -- \
                || ! git -C "${PROJECT_DIR}" diff --cached --quiet --; }; then
        printf 'Refusing migration prepare from a dirty tracked worktree\n' >&2
        exit 1
    fi
    commit=$(git -C "${PROJECT_DIR}" rev-parse HEAD)
    if [[ ! "${commit}" =~ ^[0-9a-f]{40}$ ]]; then
        printf 'Cannot bind migration to a clean 40-character commit\n' >&2
        exit 1
    fi
    printf '%s\n' "${commit}"
}

run_prepare() {
    local source_size commit
    if [[ ! -f "${SOURCE_DB}" || -L "${SOURCE_DB}" ]]; then
        printf 'Source DuckDB is missing or not a regular file: %s\n' "${SOURCE_DB}" >&2
        exit 1
    fi
    require_prepare_window
    source_size=$(regular_file_size "${SOURCE_DB}")
    require_local_space "${source_size}"
    commit=$(code_commit)
    run "${CLI[@]}" research-migration snapshot \
        --source-database "${SOURCE_DB}" \
        --recovery-dir "${RECOVERY_DIR}" \
        --artifact-dir "${ARTIFACT_DIR}" \
        --snapshot-id "${snapshot_id}" \
        --code-commit "${commit}"
    run "${CLI[@]}" research-migration prepare \
        --source-snapshot "${SOURCE_SNAPSHOT}" \
        --bundle-dir "${BUNDLE_DIR}" \
        --artifact-dir "${ARTIFACT_DIR}" \
        --snapshot-id "${snapshot_id}" \
        --code-commit "${commit}" \
        --start-date "${start_date}" \
        --end-date "${end_date}"
    # research-migration verify is mandatory before transfer.
    run "${CLI[@]}" research-migration verify --bundle-path "${BUNDLE_PATH}"
}

require_remote_space() {
    local expected_bytes="$1"
    local required_bytes=$(( expected_bytes * SPACE_MULTIPLIER + SPACE_RESERVE_BYTES ))
    local remote_command available_kb available_bytes
    printf -v remote_command \
        'mkdir -p %q && df -Pk %q | awk '\''NR == 2 {print $4}'\''' \
        "${REMOTE_STAGING}" "${REMOTE_DATA_DIR}"
    if (( dry_run == 1 )); then
        remote_run "${remote_command}"
        return 0
    fi
    available_kb=$("${SSH_BIN}" "${REMOTE}" "${remote_command}")
    if [[ ! "${available_kb}" =~ ^[0-9]+$ ]]; then
        printf 'Cannot determine remote free space\n' >&2
        exit 1
    fi
    available_bytes=$(( available_kb * 1024 ))
    if (( available_bytes < required_bytes )); then
        printf 'Insufficient remote space: available=%s required=%s\n' \
            "${available_bytes}" "${required_bytes}" >&2
        exit 1
    fi
}

run_upload() {
    local expected_bytes
    if [[ -d "${BUNDLE_PATH}" ]]; then
        run "${CLI[@]}" research-migration verify --bundle-path "${BUNDLE_PATH}"
        expected_bytes=$(( $(du -sk "${BUNDLE_PATH}" | awk '{print $1}') * 1024 ))
    elif (( dry_run == 1 )); then
        expected_bytes=$(regular_file_size "${SOURCE_DB}")
    else
        printf 'Migration bundle is missing: %s\n' "${BUNDLE_PATH}" >&2
        exit 1
    fi
    require_remote_space "${expected_bytes}"
    run "${RSYNC_BIN}" --archive --partial --checksum \
        "${BUNDLE_PATH}/" "${REMOTE}:${REMOTE_BUNDLE}/"
}

run_publish() {
    local publish_command reserve_kb
    reserve_kb=$(( SPACE_RESERVE_BYTES / 1024 ))
    # publish performs the full bundle verification once, immediately after these remote guards.
    printf -v publish_command \
        'set -euo pipefail; weekday=$(TZ=Asia/Shanghai date +%%u); hhmm=$(TZ=Asia/Shanghai date +%%H%%M); if (( weekday <= 5 && 10#$hhmm <= 1510 )); then echo "Refusing migration publish outside the post-close window after 15:10 Asia/Shanghai" >&2; exit 1; fi; if systemctl is-active --quiet rquant-monitor.service; then echo "Refusing migration publish: rquant-monitor.service is active" >&2; exit 1; fi; bundle_kb=$(du -sk %q | awk '\''{print $1}'\''); available_kb=$(df -Pk %q | awk '\''NR == 2 {print $4}'\''); if [[ ! $bundle_kb =~ ^[0-9]+$ || ! $available_kb =~ ^[0-9]+$ ]]; then echo "Cannot determine remote publish space" >&2; exit 1; fi; required_kb=$(( bundle_kb * %d + %d )); if (( available_kb < required_kb )); then echo "Insufficient remote publish space: available_kb=$available_kb required_kb=$required_kb" >&2; exit 1; fi; cd %q && timeout --signal=TERM %d env PYTHONPATH=%q/src %q/.venv/bin/rquant research-migration publish --bundle-path %q --target-data-dir %q --apply' \
        "${REMOTE_BUNDLE}" "${REMOTE_DATA_DIR}" "${SPACE_MULTIPLIER}" "${reserve_kb}" \
        "${REMOTE_REPO}" "${PUBLISH_TIMEOUT_SECONDS}" "${REMOTE_REPO}" "${REMOTE_REPO}" \
        "${REMOTE_BUNDLE}" "${REMOTE_DATA_DIR}"
    remote_run "${publish_command}"
}

case "${phase}" in
    prepare)
        run_prepare
        ;;
    upload)
        run_upload
        ;;
    publish)
        run_publish
        ;;
    all)
        run_prepare
        run_upload
        run_publish
        ;;
esac
