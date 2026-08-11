#!/usr/bin/env bash
# Preview-first transaction for obsolete runtime-live/runtime-research instances.
set -Eeuo pipefail

readonly PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
unset CDPATH

MODE=preview
TEST_ROOT=
TEST_MODE_ACTIVE=0
replacement_subjects=()
replacement_units=()
legacy_units=()
installed_templates=()
saved_active_states=()
saved_unit_file_states=()
TRANSACTION_DIR=
MUTATION_STARTED=0
MIGRATION_LOCK_FD=

usage() {
    printf '%s\n' \
        'usage: migrate-legacy-runtime-slices.sh [--accept] --replacement OLD=NEW [...]' \
        'OLD may be an instance or rquant-runtime-{live,research}@.service template' \
        'NEW must be a concrete, loaded, active replacement instance' \
        'default mode is preview and never changes service state'
}

while (( $# > 0 )); do
    case "$1" in
        --accept)
            MODE=accept
            shift
            ;;
        --replacement)
            [[ $# -ge 2 && "$2" == *=* ]] || { usage >&2; exit 2; }
            old=${2%%=*}
            new=${2#*=}
            replacement_subjects+=("${old}")
            replacement_units+=("${new}")
            shift 2
            ;;
        --test-root)
            [[ $# -ge 2 && "$2" == /* ]] || { usage >&2; exit 2; }
            TEST_ROOT=$2
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

readonly TEST_ROOT_CANONICALIZER=/usr/bin/python3
canonicalize_test_root() {
    "${TEST_ROOT_CANONICALIZER}" -I - "$1" <<'PY'
import os
import stat
import sys

requested = sys.argv[1]
absolute = os.path.abspath(requested)
resolved = os.path.realpath(absolute)
if resolved == os.sep or absolute != resolved:
    print("unsafe --test-root: root or symlinked path", file=sys.stderr)
    raise SystemExit(2)
if not os.path.isdir(resolved):
    print("unsafe --test-root: canonical directory is unavailable", file=sys.stderr)
    raise SystemExit(2)

capability = os.path.join(resolved, ".rquant-migration-test-capability-v1")
try:
    capability_stat = os.lstat(capability)
except OSError as exc:
    print("unsafe --test-root: test capability is unavailable: {}".format(exc), file=sys.stderr)
    raise SystemExit(2)
if (
    not stat.S_ISREG(capability_stat.st_mode)
    or capability_stat.st_nlink != 1
    or capability_stat.st_uid != os.geteuid()
    or stat.S_IMODE(capability_stat.st_mode) != 0o600
):
    print("unsafe --test-root: test capability metadata is invalid", file=sys.stderr)
    raise SystemExit(2)
try:
    with open(capability, "r", encoding="ascii") as stream:
        capability_value = stream.read()
except OSError as exc:
    print("unsafe --test-root: cannot read test capability: {}".format(exc), file=sys.stderr)
    raise SystemExit(2)
if capability_value != "rquant-migration-test-capability-v1\n":
    print("unsafe --test-root: test capability token is invalid", file=sys.stderr)
    raise SystemExit(2)

derived = {
    "systemctl": ("usr/bin/systemctl", "/usr/bin/systemctl", "file"),
    "sync": ("usr/bin/sync", "/usr/bin/sync", "file"),
    "unit": ("etc/systemd/system", "/etc/systemd/system", "directory"),
    "state": ("state", "/state", "directory"),
}
for name, (relative, production, kind) in derived.items():
    lexical = os.path.normpath(os.path.join(resolved, relative))
    candidate = os.path.realpath(lexical)
    try:
        contained = os.path.commonpath((resolved, candidate)) == resolved
    except ValueError:
        contained = False
    if not contained or candidate == production or candidate != lexical:
        print(
            "unsafe --test-root: {} escapes canonical test root".format(name),
            file=sys.stderr,
        )
        raise SystemExit(2)
    try:
        metadata = os.lstat(lexical)
    except OSError as exc:
        print(
            "unsafe --test-root: {} path is unavailable: {}".format(name, exc),
            file=sys.stderr,
        )
        raise SystemExit(2)
    valid_kind = (
        stat.S_ISREG(metadata.st_mode)
        if kind == "file"
        else stat.S_ISDIR(metadata.st_mode)
    )
    if not valid_kind or metadata.st_nlink < 1:
        print(
            "unsafe --test-root: {} path type is invalid".format(name),
            file=sys.stderr,
        )
        raise SystemExit(2)

print(resolved)
PY
}

if [[ -n "${TEST_ROOT}" ]]; then
    if (( EUID == 0 )); then
        printf '%s\n' 'unsafe --test-root: root may not use the test capability' >&2
        exit 2
    fi
    if [[ ! -x "${TEST_ROOT_CANONICALIZER}" ]]; then
        printf 'unsafe --test-root: fixed canonicalizer is unavailable: %s\n' \
            "${TEST_ROOT_CANONICALIZER}" >&2
        exit 2
    fi
    if ! canonical_test_root=$(canonicalize_test_root "${TEST_ROOT}"); then
        exit 2
    fi
    TEST_ROOT=${canonical_test_root}
    TEST_MODE_ACTIVE=1
    readonly SYSTEMCTL="${TEST_ROOT}/usr/bin/systemctl"
    readonly REMOVE="${TEST_ROOT}/usr/bin/rm"
    readonly FLOCK="${TEST_ROOT}/usr/bin/flock"
    readonly FILESYSTEM_SYNC="${TEST_ROOT}/usr/bin/sync"
    readonly PYTHON="${TEST_ROOT}/usr/bin/python3"
    readonly UNIT_DIR="${TEST_ROOT}/etc/systemd/system"
    readonly JOURNAL_ROOT="${TEST_ROOT}/state/rquant-workload-migration"
else
    readonly SYSTEMCTL=/usr/bin/systemctl
    readonly REMOVE=/usr/bin/rm
    readonly FLOCK=/usr/bin/flock
    readonly FILESYSTEM_SYNC=/usr/bin/sync
    readonly PYTHON=/usr/bin/python3
    readonly UNIT_DIR=/etc/systemd/system
    readonly JOURNAL_ROOT=/var/lib/rquant/workload-isolation/migration
fi
readonly AWK=/usr/bin/awk
readonly CP=/bin/cp
readonly MKDIR=/bin/mkdir
readonly MKTEMP=/usr/bin/mktemp
readonly CLEANUP_RM=/bin/rm
readonly MV=/bin/mv
readonly STAT=/usr/bin/stat
readonly ACTIVE_JOURNAL="${JOURNAL_ROOT}/active"
readonly MIGRATION_LOCK="${JOURNAL_ROOT}/migration.lock"

for executable in "${SYSTEMCTL}" "${REMOVE}" "${AWK}" "${CP}" "${MKDIR}" \
    "${MKTEMP}" "${MV}" "${FLOCK}" "${FILESYSTEM_SYNC}" "${PYTHON}"; do
    if [[ ! -x "${executable}" ]]; then
        printf 'required fixed executable is unavailable: %s\n' "${executable}" >&2
        exit 2
    fi
done
if (( TEST_MODE_ACTIVE == 0 )) && \
      ( -n "${RQUANT_MIGRATION_TEST_SIGNAL_AFTER_DISABLE:-}" || \
        -n "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_PREPARED:-}" || \
        -n "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_DISABLE:-}" || \
        -n "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_COMMIT:-}" || \
        -n "${RQUANT_MIGRATION_TEST_FAIL_AFTER_UNIT_SYNC:-}" || \
        -n "${RQUANT_MIGRATION_TEST_KILL_ON_SYNC_CALL:-}" ); then
    printf '%s\n' 'migration fault injection is test-root only' >&2
    exit 2
fi

fsync_file() {
    "${PYTHON}" -c '
import os
import sys

descriptor = os.open(sys.argv[1], os.O_RDONLY)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
' "$1"
}

fsync_directory() {
    "${PYTHON}" -c '
import os
import sys

flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
descriptor = os.open(sys.argv[1], flags)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
' "$1"
}

publish_temp_file() {
    local temporary=$1
    local target=$2
    local directory=${target%/*}
    fsync_file "${temporary}"
    "${MV}" -f -- "${temporary}" "${target}"
    fsync_directory "${directory}"
}

atomic_write_file() {
    local source=$1
    local target=$2
    local directory=${target%/*}
    local name=${target##*/}
    local temporary
    temporary=$("${MKTEMP}" "${directory}/.${name}.tmp.XXXXXX")
    "${CP}" -a -- "${source}" "${temporary}"
    publish_temp_file "${temporary}" "${target}"
}

atomic_write_text() {
    local target=$1
    local value=$2
    local directory=${target%/*}
    local name=${target##*/}
    local temporary
    temporary=$("${MKTEMP}" "${directory}/.${name}.tmp.XXXXXX")
    printf '%s\n' "${value}" >"${temporary}"
    publish_temp_file "${temporary}" "${target}"
}

valid_phase_value() {
    local path=$1
    local value
    [[ -f "${path}" && ! -L "${path}" ]] || return 1
    value=$(<"${path}")
    case "${value}" in
        prepared|mutating|committed) printf '%s\n' "${value}" ;;
        *) return 1 ;;
    esac
}

resolve_transaction_phase() {
    local journal=$1
    local phase
    if phase=$(valid_phase_value "${journal}/phase"); then
        printf '%s\n' "${phase}"
        return 0
    fi
    if phase=$(valid_phase_value "${journal}/phase.last-good"); then
        printf '%s\n' "${phase}"
        return 0
    fi
    return 1
}

write_transaction_phase() {
    local journal=$1
    local next_phase=$2
    local previous_phase
    if previous_phase=$(valid_phase_value "${journal}/phase"); then
        atomic_write_text "${journal}/phase.last-good" "${previous_phase}"
    else
        atomic_write_text "${journal}/phase.last-good" "${next_phase}"
    fi
    atomic_write_text "${journal}/phase" "${next_phase}"
}

remove_journal() {
    local journal=$1
    "${CLEANUP_RM}" -rf -- "${journal}"
    fsync_directory "${JOURNAL_ROOT}"
}

filesystem_durability_barrier() {
    local context=$1
    if ! "${FILESYSTEM_SYNC}" -f "${UNIT_DIR}"; then
        printf 'filesystem durability barrier failed for %s: %s\n' \
            "${context}" "${UNIT_DIR}" >&2
        return 1
    fi
}

template_for_instance() {
    case "$1" in
        rquant-runtime-live@*.service) printf '%s\n' 'rquant-runtime-live@.service' ;;
        rquant-runtime-research@*.service) printf '%s\n' 'rquant-runtime-research@.service' ;;
        *) printf '%s\n' "$1" ;;
    esac
}

replacement_for() {
    local subject=$1
    local template
    local index
    template=$(template_for_instance "${subject}")
    for (( index=0; index<${#replacement_subjects[@]}; index++ )); do
        if [[ "${replacement_subjects[index]}" == "${subject}" ]]; then
            printf '%s\n' "${replacement_units[index]}"
            return
        fi
    done
    for (( index=0; index<${#replacement_subjects[@]}; index++ )); do
        if [[ "${replacement_subjects[index]}" == "${template}" ]]; then
            printf '%s\n' "${replacement_units[index]}"
            return
        fi
    done
    printf '\n'
}

property_value() {
    local properties=$1
    local name=$2
    printf '%s\n' "${properties}" | "${AWK}" -F= -v key="${name}" '$1==key{print $2}'
}

restore_unit_file_state() {
    local unit=$1
    local expected=$2
    case "${expected}" in
        enabled)
            "${SYSTEMCTL}" enable "${unit}"
            ;;
        enabled-runtime)
            "${SYSTEMCTL}" enable --runtime "${unit}"
            ;;
        disabled)
            "${SYSTEMCTL}" disable "${unit}"
            ;;
        *)
            printf 'rollback cannot restore unknown UnitFileState=%s for %s\n' \
                "${expected}" "${unit}" >&2
            return 1
            ;;
    esac
}

restore_active_state() {
    local unit=$1
    local expected=$2
    case "${expected}" in
        active)
            "${SYSTEMCTL}" start "${unit}"
            ;;
        inactive)
            "${SYSTEMCTL}" stop "${unit}"
            ;;
        *)
            printf 'rollback cannot restore unknown ActiveState=%s for %s\n' \
                "${expected}" "${unit}" >&2
            return 1
            ;;
    esac
}

verify_replacement_runtime() {
    local replacement=$1
    local replacement_state load_state active_state slice control_group
    local slice_control_group
    if ! replacement_state=$(
        "${SYSTEMCTL}" show "${replacement}" --no-pager \
            --property=LoadState,ActiveState,UnitFileState,Slice,ControlGroup
    ); then
        printf 'cannot query replacement runtime: %s\n' "${replacement}" >&2
        return 1
    fi
    printf '%s\n' "${replacement_state}"
    load_state=$(property_value "${replacement_state}" LoadState)
    active_state=$(property_value "${replacement_state}" ActiveState)
    slice=$(property_value "${replacement_state}" Slice)
    control_group=$(property_value "${replacement_state}" ControlGroup)
    case "${slice}" in
        rquant-live.slice|rquant-serving.slice) ;;
        rquant-research.slice)
            printf 'replacement %s is blocked: maintenance memory pending calibration\n' \
                "${replacement}" >&2
            return 1
            ;;
        *)
            printf 'replacement %s has invalid Slice=%s\n' \
                "${replacement}" "${slice}" >&2
            return 1
            ;;
    esac
    if ! slice_control_group=$(
        "${SYSTEMCTL}" show "${slice}" --value --property=ControlGroup
    ); then
        printf 'cannot resolve replacement Slice cgroup: %s\n' "${slice}" >&2
        return 1
    fi
    if [[ "${load_state}" != loaded || "${active_state}" != active ]]; then
        printf 'replacement %s is not loaded and active\n' "${replacement}" >&2
        return 1
    fi
    if [[ -z "${slice_control_group}" || \
          "${control_group}" != "${slice_control_group}"/* ]]; then
        printf 'replacement %s ControlGroup is outside resolved Slice cgroup\n' \
            "${replacement}" >&2
        return 1
    fi
}

load_transaction_journal() {
    local journal=$1
    local template unit active enabled
    legacy_units=()
    installed_templates=()
    saved_active_states=()
    saved_unit_file_states=()
    [[ -d "${journal}" && ! -L "${journal}" ]] || return 1
    [[ -f "${journal}/templates.list" && ! -L "${journal}/templates.list" && \
       -f "${journal}/legacy-states.tsv" && \
       ! -L "${journal}/legacy-states.tsv" ]] || return 1
    while IFS= read -r template; do
        [[ -n "${template}" ]] || continue
        case "${template}" in
            rquant-runtime-live@.service|rquant-runtime-research@.service) ;;
            *) return 1 ;;
        esac
        [[ -f "${journal}/files/${template}" && \
           ! -L "${journal}/files/${template}" ]] || return 1
        installed_templates+=("${template}")
    done <"${journal}/templates.list"
    while IFS=$'\t' read -r unit active enabled; do
        [[ -n "${unit}" ]] || continue
        case "${unit}" in
            rquant-runtime-live@*.service|rquant-runtime-research@*.service) ;;
            *) return 1 ;;
        esac
        case "${active}" in active|inactive) ;; *) return 1 ;; esac
        case "${enabled}" in enabled|enabled-runtime|disabled) ;; *) return 1 ;; esac
        legacy_units+=("${unit}")
        saved_active_states+=("${active}")
        saved_unit_file_states+=("${enabled}")
    done <"${journal}/legacy-states.tsv"
}

recover_transaction() {
    local journal=$1
    local rollback_failed=0
    local index
    local observed
    local actual_active
    local actual_enabled
    local recovery_phase=
    if recovery_phase=$(resolve_transaction_phase "${journal}") && \
       [[ "${recovery_phase}" == committed ]]; then
        remove_journal "${journal}" || return 1
        return 0
    fi
    if [[ -z "${recovery_phase}" ]]; then
        printf 'migration phase is torn; applying fail-safe rollback: %s\n' \
            "${journal}" >&2
    fi
    if ! load_transaction_journal "${journal}"; then
        printf 'persistent migration journal is invalid: %s\n' "${journal}" >&2
        return 1
    fi
    for (( index=0; index<${#installed_templates[@]}; index++ )); do
        template=${installed_templates[index]}
        atomic_write_file "${journal}/files/${template}" \
            "${UNIT_DIR}/${template}" || rollback_failed=1
    done
    "${SYSTEMCTL}" daemon-reload || rollback_failed=1
    for (( index=0; index<${#legacy_units[@]}; index++ )); do
        restore_unit_file_state \
            "${legacy_units[index]}" "${saved_unit_file_states[index]}" \
            || rollback_failed=1
        restore_active_state \
            "${legacy_units[index]}" "${saved_active_states[index]}" \
            || rollback_failed=1
    done
    for (( index=0; index<${#legacy_units[@]}; index++ )); do
        observed=$("${SYSTEMCTL}" show "${legacy_units[index]}" --no-pager \
            --property=ActiveState,UnitFileState) || { rollback_failed=1; continue; }
        actual_active=$(property_value "${observed}" ActiveState)
        actual_enabled=$(property_value "${observed}" UnitFileState)
        if [[ "${actual_active}" != "${saved_active_states[index]}" || \
              "${actual_enabled}" != "${saved_unit_file_states[index]}" ]]; then
            printf 'rollback verification failed for %s: active=%s enabled=%s\n' \
                "${legacy_units[index]}" "${actual_active}" "${actual_enabled}" >&2
            rollback_failed=1
        fi
    done
    if (( rollback_failed != 0 )); then
        printf '%s\n' 'rollback failed to restore legacy unit transaction' >&2
        return 1
    fi
    filesystem_durability_barrier rollback || return 1
    remove_journal "${journal}" || return 1
}

rollback_with_status() {
    local original_status=$1
    trap - ERR TERM INT HUP
    set +e
    if (( MUTATION_STARTED != 0 )); then
        recovery_phase=$(resolve_transaction_phase "${TRANSACTION_DIR}" || true)
        if ! recover_transaction "${TRANSACTION_DIR}"; then
            exit 5
        fi
        if [[ "${recovery_phase}" == committed ]]; then
            printf '%s\n' 'finalized committed legacy migration journal' >&2
        else
            printf '%s\n' 'rollback restored legacy files and unit state' >&2
        fi
    fi
    exit "${original_status}"
}

rollback() {
    rollback_with_status "$?"
}

rollback_term() {
    rollback_with_status 143
}

rollback_int() {
    rollback_with_status 130
}

rollback_hup() {
    rollback_with_status 129
}

[[ ! -L "${JOURNAL_ROOT}" ]] || {
    printf 'unsafe migration journal root: %s\n' "${JOURNAL_ROOT}" >&2
    exit 5
}
if [[ -n "${TEST_ROOT}" ]]; then
    "${MKDIR}" -p -- "${JOURNAL_ROOT}"
    /bin/chmod 0700 "${JOURNAL_ROOT}"
    if [[ ! -e "${MIGRATION_LOCK}" && ! -L "${MIGRATION_LOCK}" ]]; then
        (umask 077; : >"${MIGRATION_LOCK}")
    fi
    /bin/chmod 0600 "${MIGRATION_LOCK}"
else
    if (( EUID != 0 )); then
        printf '%s\n' 'legacy migration requires root' >&2
        exit 5
    fi
    if [[ ! -d "${JOURNAL_ROOT}" ]]; then
        printf 'migration journal root is not installed: %s\n' \
            "${JOURNAL_ROOT}" >&2
        exit 5
    fi
    root_owner=$("${STAT}" -c '%u' "${JOURNAL_ROOT}")
    root_mode=$("${STAT}" -c '%a' "${JOURNAL_ROOT}")
    if [[ "${root_owner}" != 0 || "${root_mode}" != 700 ]]; then
        printf 'migration journal root must be root:root mode 0700: %s\n' \
            "${JOURNAL_ROOT}" >&2
        exit 5
    fi
fi
if [[ ! -f "${MIGRATION_LOCK}" || -L "${MIGRATION_LOCK}" ]]; then
    printf 'unsafe migration lock file: %s\n' "${MIGRATION_LOCK}" >&2
    exit 5
fi
if [[ -z "${TEST_ROOT}" ]]; then
    lock_owner=$("${STAT}" -c '%u' "${MIGRATION_LOCK}")
    lock_mode=$("${STAT}" -c '%a' "${MIGRATION_LOCK}")
    lock_links=$("${STAT}" -c '%h' "${MIGRATION_LOCK}")
    if [[ "${lock_owner}" != 0 || "${lock_mode}" != 600 || \
          "${lock_links}" != 1 ]]; then
        printf 'migration lock must be root:root mode 0600 with one link: %s\n' \
            "${MIGRATION_LOCK}" >&2
        exit 5
    fi
fi
MIGRATION_LOCK_FD=9
exec 9<>"${MIGRATION_LOCK}"
if ! "${FLOCK}" -n "${MIGRATION_LOCK_FD}"; then
    printf '%s\n' 'migration transaction is busy' >&2
    exit 6
fi

if [[ -e "${ACTIVE_JOURNAL}" || -L "${ACTIVE_JOURNAL}" ]]; then
    recovery_phase=
    recovery_phase=$(resolve_transaction_phase "${ACTIVE_JOURNAL}" || true)
    if ! recover_transaction "${ACTIVE_JOURNAL}"; then
        exit 5
    fi
    if [[ "${recovery_phase}" == committed ]]; then
        printf '%s\n' 'finalized committed legacy migration journal'
    else
        printf '%s\n' 'recovered interrupted legacy migration transaction'
    fi
    legacy_units=()
    installed_templates=()
    saved_active_states=()
    saved_unit_file_states=()
fi

legacy_output=$(
    "${SYSTEMCTL}" list-units --all --type=service --plain --no-legend --no-pager \
        'rquant-runtime-live@*.service' 'rquant-runtime-research@*.service'
)
while IFS= read -r legacy; do
    [[ -n "${legacy}" ]] || continue
    legacy_units+=("${legacy}")
done <<< "$(printf '%s\n' "${legacy_output}" | "${AWK}" 'NF{print $1}')"
for template in rquant-runtime-live@.service rquant-runtime-research@.service; do
    if [[ -f "${UNIT_DIR}/${template}" ]]; then
        installed_templates+=("${template}")
    fi
done

if (( ${#legacy_units[@]} == 0 && ${#installed_templates[@]} == 0 )); then
    printf '%s\n' 'no loaded legacy instances or installed legacy templates found'
    exit 0
fi

printf 'legacy runtime migration mode=%s\n' "${MODE}"
for (( index=0; index<${#legacy_units[@]}; index++ )); do
    legacy=${legacy_units[index]}
    replacement=$(replacement_for "${legacy}")
    printf 'loaded legacy=%s replacement=%s\n' "${legacy}" "${replacement:-MISSING}"
    "${SYSTEMCTL}" show "${legacy}" --no-pager \
        --property=LoadState,ActiveState,UnitFileState,Slice,ControlGroup
done
for (( index=0; index<${#installed_templates[@]}; index++ )); do
    template=${installed_templates[index]}
    replacement=$(replacement_for "${template}")
    printf 'installed legacy template=%s replacement=%s\n' \
        "${UNIT_DIR}/${template}" "${replacement:-MISSING}"
done

if [[ "${MODE}" == preview ]]; then
    printf '%s\n' 'preview: no service state was changed'
    exit 3
fi

subjects=()
for (( index=0; index<${#legacy_units[@]}; index++ )); do
    subjects+=("${legacy_units[index]}")
done
for (( index=0; index<${#installed_templates[@]}; index++ )); do
    subjects+=("${installed_templates[index]}")
done
verified_replacements=()
for subject in "${subjects[@]}"; do
    replacement=$(replacement_for "${subject}")
    if [[ -z "${replacement}" || "${replacement}" == "${subject}" ]]; then
        printf 'refusing migration: replacement missing or is the same legacy unit for %s\n' \
            "${subject}" >&2
        exit 4
    fi
    if [[ "${replacement}" == *@.service ]]; then
        printf 'refusing migration: concrete replacement instance required, got %s\n' \
            "${replacement}" >&2
        exit 4
    fi
    if [[ ! "${replacement}" =~ ^rquant-[A-Za-z0-9_.:-]+@[A-Za-z0-9_.:-]+\.service$ ]]; then
        printf 'refusing invalid concrete replacement unit name: %s\n' \
            "${replacement}" >&2
        exit 4
    fi
    case "${replacement}" in
        rquant-runtime-live@*.service|rquant-runtime-research@*.service)
            printf 'refusing legacy-template-derived replacement: %s\n' \
                "${replacement}" >&2
            exit 4
            ;;
    esac
    for (( index=0; index<${#legacy_units[@]}; index++ )); do
        legacy=${legacy_units[index]}
        if [[ "${replacement}" == "${legacy}" ]]; then
            printf 'refusing loaded legacy unit as replacement: %s\n' \
                "${replacement}" >&2
            exit 4
        fi
    done
    already_verified=0
    if (( ${#verified_replacements[@]} > 0 )); then
        for verified in "${verified_replacements[@]}"; do
            if [[ "${verified}" == "${replacement}" ]]; then
                already_verified=1
                break
            fi
        done
    fi
    (( already_verified == 0 )) || continue
    if ! verify_replacement_runtime "${replacement}"; then
        exit 4
    fi
    verified_replacements+=("${replacement}")
done

STAGING_DIR=$("${MKTEMP}" -d "${JOURNAL_ROOT}/.prepare.XXXXXX")
"${MKDIR}" -p -- "${STAGING_DIR}/files"
templates_temporary=$("${MKTEMP}" "${STAGING_DIR}/.templates.list.tmp.XXXXXX")
states_temporary=$("${MKTEMP}" "${STAGING_DIR}/.legacy-states.tsv.tmp.XXXXXX")
: >"${templates_temporary}"
: >"${states_temporary}"
for (( index=0; index<${#installed_templates[@]}; index++ )); do
    template=${installed_templates[index]}
    "${CP}" -a -- "${UNIT_DIR}/${template}" "${STAGING_DIR}/files/${template}"
    fsync_file "${STAGING_DIR}/files/${template}"
    printf '%s\n' "${template}" >>"${templates_temporary}"
done
fsync_directory "${STAGING_DIR}/files"
for (( index=0; index<${#legacy_units[@]}; index++ )); do
    legacy=${legacy_units[index]}
    if ! old_state=$(
        "${SYSTEMCTL}" show "${legacy}" --no-pager \
            --property=LoadState,ActiveState,UnitFileState
    ); then
        printf 'cannot query legacy unit state for %s\n' "${legacy}" >&2
        "${CLEANUP_RM}" -rf -- "${STAGING_DIR}"
        fsync_directory "${JOURNAL_ROOT}"
        exit 4
    fi
    load_state=$(property_value "${old_state}" LoadState)
    active_state=$(property_value "${old_state}" ActiveState)
    unit_file_state=$(property_value "${old_state}" UnitFileState)
    if [[ "${load_state}" != loaded ]]; then
        printf 'cannot snapshot loaded legacy unit state for %s\n' "${legacy}" >&2
        "${CLEANUP_RM}" -rf -- "${STAGING_DIR}"
        fsync_directory "${JOURNAL_ROOT}"
        exit 4
    fi
    case "${active_state}" in
        active|inactive) ;;
        *)
            printf 'legacy unit is not in a recoverable state: %s ActiveState=%s\n' \
                "${legacy}" "${active_state}" >&2
            "${CLEANUP_RM}" -rf -- "${STAGING_DIR}"
            fsync_directory "${JOURNAL_ROOT}"
            exit 4
            ;;
    esac
    case "${unit_file_state}" in
        enabled|enabled-runtime|disabled) ;;
        *)
            printf 'legacy unit is not in a recoverable state: %s UnitFileState=%s\n' \
                "${legacy}" "${unit_file_state}" >&2
            "${CLEANUP_RM}" -rf -- "${STAGING_DIR}"
            fsync_directory "${JOURNAL_ROOT}"
            exit 4
            ;;
    esac
    saved_active_states+=("${active_state}")
    saved_unit_file_states+=("${unit_file_state}")
    printf '%s\t%s\t%s\n' "${legacy}" "${active_state}" "${unit_file_state}" \
        >>"${states_temporary}"
done

publish_temp_file "${templates_temporary}" "${STAGING_DIR}/templates.list"
publish_temp_file "${states_temporary}" "${STAGING_DIR}/legacy-states.tsv"
write_transaction_phase "${STAGING_DIR}" prepared
fsync_directory "${STAGING_DIR}"
"${MV}" -- "${STAGING_DIR}" "${ACTIVE_JOURNAL}"
fsync_directory "${JOURNAL_ROOT}"
TRANSACTION_DIR="${ACTIVE_JOURNAL}"
MUTATION_STARTED=1
trap rollback ERR
trap rollback_term TERM
trap rollback_int INT
trap rollback_hup HUP
if [[ "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_PREPARED:-}" == 1 ]]; then
    kill -KILL "$$"
fi
write_transaction_phase "${TRANSACTION_DIR}" mutating
for (( index=0; index<${#legacy_units[@]}; index++ )); do
    legacy=${legacy_units[index]}
    "${SYSTEMCTL}" disable --now "${legacy}"
    if [[ -n "${RQUANT_MIGRATION_TEST_SIGNAL_AFTER_DISABLE:-}" ]]; then
        kill -s "${RQUANT_MIGRATION_TEST_SIGNAL_AFTER_DISABLE}" "$$"
    fi
    if [[ "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_DISABLE:-}" == 1 ]]; then
        kill -KILL "$$"
    fi
done
for (( index=0; index<${#installed_templates[@]}; index++ )); do
    template=${installed_templates[index]}
    "${REMOVE}" -f -- "${UNIT_DIR}/${template}"
done
fsync_directory "${UNIT_DIR}"
"${SYSTEMCTL}" daemon-reload

for (( index=0; index<${#installed_templates[@]}; index++ )); do
    template=${installed_templates[index]}
    load_state=$(
        "${SYSTEMCTL}" show "${template}" --property=LoadState --value
    )
    if [[ "${load_state}" != not-found ]]; then
        printf 'legacy template still loaded after removal: %s\n' "${template}" >&2
        false
    fi
done
for (( index=0; index<${#legacy_units[@]}; index++ )); do
    legacy=${legacy_units[index]}
    old_state=$(
        "${SYSTEMCTL}" show "${legacy}" --no-pager \
            --property=ActiveState,UnitFileState
    )
    active_state=$(property_value "${old_state}" ActiveState)
    unit_file_state=$(property_value "${old_state}" UnitFileState)
    if [[ "${active_state}" != inactive || \
          "${unit_file_state}" != disabled && "${unit_file_state}" != not-found ]]; then
        printf 'legacy instance not disabled after migration: %s\n' "${legacy}" >&2
        false
    fi
done
for replacement in "${verified_replacements[@]}"; do
    verify_replacement_runtime "${replacement}"
done

filesystem_durability_barrier accept
if [[ "${RQUANT_MIGRATION_TEST_FAIL_AFTER_UNIT_SYNC:-}" == 1 ]]; then
    printf '%s\n' 'injected failure after unit filesystem durability barrier' >&2
    false
fi
write_transaction_phase "${TRANSACTION_DIR}" committed
if [[ "${RQUANT_MIGRATION_TEST_POWER_LOSS_AFTER_COMMIT:-}" == 1 ]]; then
    kill -KILL "$$"
fi
trap - ERR TERM INT HUP
remove_journal "${TRANSACTION_DIR}"
printf '%s\n' \
    'accepted: replacements verified; legacy instances disabled and legacy templates removed'
