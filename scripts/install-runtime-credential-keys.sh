#!/usr/bin/env bash
# Generate, rotate and verify the Ed25519 key material consumed by the four
# root-owned rQuant credential helpers.
#
#   init    creates the nine files described in docs/operations/runtime-credential-keys.md
#   rotate  replaces one active key and folds the retired public key into the manifest
#   verify  re-checks ownership/mode/nlink/schema and runs each consumer's own loader
#
# Deliberate constraints (see docs/operations/runtime-credential-keys.md):
#   * no `rquant` import and no virtualenv: only openssl(1) and the system python3
#     that already runs the helpers themselves;
#   * private keys are written 0600 through a tmp file + fsync + rename;
#   * `--prefix` mirrors `install-runtime-credential-infra.sh --test-root`, i.e. the
#     expected owner degrades from root to the invoking uid/effective gid.

set -Eeuo pipefail
umask 077
unset CDPATH

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly SCRIPT_DIR PROJECT_DIR

PYTHON_BIN=/usr/bin/python3
readonly PYTHON_BIN

COMMAND=""
PREFIX=""
KEY_SUFFIX="v1"
NEW_KEY_SUFFIX=""
ROTATE_TARGET=""
DRY_RUN=0
CALENDAR_COVERAGE_START=""
CALENDAR_COVERAGE_END=""
CALENDAR_OPEN_DATES=""
HELPER_DIR=""

TMP_PATHS=()

cleanup_temporaries() {
    local path
    for path in ${TMP_PATHS+"${TMP_PATHS[@]}"}; do
        /bin/rm -f -- "${path}" 2>/dev/null || true
    done
    TMP_PATHS=()
}

on_error() {
    local status=$?
    trap - ERR EXIT
    set +e
    cleanup_temporaries
    printf 'install-runtime-credential-keys: aborted (exit %s)\n' "${status}" >&2
    exit "${status}"
}
trap on_error ERR
trap cleanup_temporaries EXIT

fail() {
    printf 'install-runtime-credential-keys: %s\n' "$1" >&2
    exit "${2:-1}"
}

usage() {
    cat >&2 <<'USAGE'
usage:
  install-runtime-credential-keys.sh init   [--prefix DIR] [--key-suffix SUFFIX] [--dry-run]
                                            [--calendar-coverage-start YYYY-MM-DD]
                                            [--calendar-coverage-end YYYY-MM-DD]
                                            [--calendar-open-dates YYYY-MM-DD,...]
  install-runtime-credential-keys.sh rotate <highwater|canvas|shadow|daily>
                                            [--prefix DIR] [--new-key-suffix SUFFIX]
  install-runtime-credential-keys.sh verify [--prefix DIR] [--helper-dir DIR]

`--root` is accepted as a synonym of `--prefix`.
USAGE
    exit 2
}

chown_binary() {
    local candidate
    for candidate in /usr/sbin/chown /bin/chown /usr/bin/chown; do
        if [[ -x "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done
    fail "chown is unavailable"
}

openssl_binary() {
    local candidate
    # Same resolution order as the helpers themselves
    # (rquant-canvas-publication-signer:58-62), so the public key this script
    # derives is byte-identical to the one the consumer derives.
    for candidate in /opt/homebrew/bin/openssl /usr/bin/openssl; do
        if [[ -f "${candidate}" && -x "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done
    fail "openssl is unavailable"
}

PY_HELPER=$(
    cat <<'PYSRC'
"""Stdlib-only JSON/metadata worker for install-runtime-credential-keys.sh."""

import hashlib
import json
import os
import stat
import sys
from datetime import date, datetime, timedelta, timezone

GENESIS_HASH = "0" * 64
KIND_LAYOUT = {
    "highwater": {
        "directory": "lab-highwater",
        "manifest": "lab-highwater-keys.json",
        "keyring": "lab-highwater-trusted-keys.json",
        "key_prefix": "hw",
        "schema_version": 3,
        "chained": True,
    },
    "canvas": {
        "directory": "canvas-publication",
        "manifest": "canvas-publication-keys.json",
        "keyring": "canvas-publication-trusted-keys.json",
        "key_prefix": "canvas",
        "schema_version": 1,
        "chained": False,
    },
    "shadow": {
        "directory": "shadow-report",
        "manifest": "shadow-report-keys.json",
        "keyring": "shadow-report-trusted-keys.json",
        "key_prefix": "shadow",
        "schema_version": 2,
        "chained": False,
    },
    "daily": {
        "directory": "daily-receipt",
        "manifest": "daily-receipt-keys.json",
        "keyring": "daily-receipt-trusted-keys.json",
        "key_prefix": "daily",
        "schema_version": 2,
        "chained": True,
    },
}
KIND_ORDER = ("highwater", "canvas", "shadow", "daily")
CALENDAR_NAME = "legacy-recovery-calendar.json"
DIRECTORY_MODES = {"": 0o755}


class Failure(Exception):
    pass


def canonical(value):
    # Byte-for-byte identical to the helpers' `_canonical_bytes`
    # (rquant-daily-receipt-signer:49-56): ensure_ascii=True, sorted keys,
    # compact separators, and no trailing newline.
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Failure("duplicate JSON key")
        result[key] = value
    return result


def read_json(path):
    with open(path, "rb") as handle:
        payload = handle.read()
    try:
        return json.loads(payload, object_pairs_hook=unique_pairs), payload
    except ValueError as exc:
        raise Failure("%s is not valid JSON: %s" % (path, exc))


def etc_root(prefix):
    return os.path.join(prefix, "etc", "rquant") if prefix else "/etc/rquant"


def key_directory(prefix, kind):
    return os.path.join(etc_root(prefix), KIND_LAYOUT[kind]["directory"])


def manifest_path(prefix, kind):
    return os.path.join(etc_root(prefix), KIND_LAYOUT[kind]["manifest"])


def keyring_path(prefix, kind):
    return os.path.join(etc_root(prefix), KIND_LAYOUT[kind]["keyring"])


def private_key_path(prefix, kind, suffix):
    layout = KIND_LAYOUT[kind]
    name = "%s-%s.private.pem" % (layout["key_prefix"], suffix)
    return os.path.join(key_directory(prefix, kind), name)


def calendar_path(prefix):
    return os.path.join(key_directory(prefix, "shadow"), CALENDAR_NAME)


def directories(prefix):
    result = [(etc_root(prefix), 0o755)]
    for kind in KIND_ORDER:
        result.append((key_directory(prefix, kind), 0o700))
    return result


def planned_files(prefix, suffix):
    result = []
    for kind in KIND_ORDER:
        result.append(manifest_path(prefix, kind))
        result.append(private_key_path(prefix, kind, suffix))
        if kind == "shadow":
            result.append(calendar_path(prefix))
    return result


def require_normalized(path, label):
    if not os.path.isabs(path) or path != os.path.abspath(path):
        raise Failure("%s must be an absolute normalized path: %s" % (label, path))


def atomic_write(target, payload, mode, uid, gid):
    directory = os.path.dirname(target)
    temporary = "%s.tmp-%d" % (target, os.getpid())
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.write(descriptor, payload)
        os.fchmod(descriptor, mode)
        if uid >= 0:
            os.fchown(descriptor, uid, gid)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.rename(temporary, target)
    fsync_directory(directory)


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def finalize_existing(temporary, target, mode, uid, gid):
    descriptor = os.open(temporary, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            raise Failure("%s is not a private regular file" % temporary)
        os.fchmod(descriptor, mode)
        if uid >= 0:
            os.fchown(descriptor, uid, gid)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.rename(temporary, target)
    fsync_directory(os.path.dirname(target))


def genesis_manifest(prefix, kind, suffix):
    layout = KIND_LAYOUT[kind]
    private_key = private_key_path(prefix, kind, suffix)
    require_normalized(private_key, "active_private_key_path")
    key_id = "%s-%s" % (layout["key_prefix"], suffix)
    document = {
        "schema_version": layout["schema_version"],
        "active_key_id": key_id,
        "active_private_key_path": private_key,
        "previous_public_keys": {},
    }
    if layout["chained"]:
        document["generation"] = 1
        document["previous_manifest_hash"] = GENESIS_HASH
    if kind == "shadow":
        calendar = calendar_path(prefix)
        require_normalized(calendar, "legacy_recovery_calendar_path")
        document["legacy_recovery_calendar_path"] = calendar
    return document


def calendar_document(coverage_start, coverage_end, open_dates):
    if not coverage_start or not coverage_end:
        today = datetime.now(timezone(timedelta(hours=8))).date()
        coverage_start = coverage_start or today.isoformat()
        coverage_end = coverage_end or (today + timedelta(days=365)).isoformat()
    try:
        start = date.fromisoformat(coverage_start)
        end = date.fromisoformat(coverage_end)
        parsed = [date.fromisoformat(value) for value in open_dates]
    except ValueError as exc:
        raise Failure("recovery calendar dates are invalid: %s" % exc)
    if start > end:
        raise Failure("recovery calendar coverage_start is after coverage_end")
    if sorted(set(parsed)) != parsed:
        raise Failure("recovery calendar open_dates must be strictly ascending")
    if any(value < start or value > end for value in parsed):
        raise Failure("recovery calendar open_dates fall outside the coverage window")
    body = {
        "schema_version": 1,
        "exchange": "SSE",
        "coverage_start": start.isoformat(),
        "coverage_end": end.isoformat(),
        "open_dates": [value.isoformat() for value in parsed],
    }
    body["content_sha256"] = hashlib.sha256(canonical(
        {key: value for key, value in body.items() if key != "content_sha256"}
    )).hexdigest()
    return body


def public_keyring_manifest_hash(kind, document, active_public_key):
    layout = KIND_LAYOUT[kind]
    if not layout["chained"]:
        raise Failure("%s manifests are not chained" % kind)
    body = {
        "schema_version": layout["schema_version"],
        "generation": document["generation"],
        "previous_manifest_hash": document["previous_manifest_hash"],
        "active_key_id": document["active_key_id"],
        "active_public_key": active_public_key,
        "previous_public_keys": dict(sorted(document["previous_public_keys"].items())),
    }
    return hashlib.sha256(canonical(body)).hexdigest()


def command_plan(argv):
    prefix, suffix = argv[0], argv[1]
    for path, mode in directories(prefix):
        sys.stdout.write("directory %s %04o\n" % (path, mode))
    for path in planned_files(prefix, suffix):
        sys.stdout.write("file      %s 0600\n" % path)
    return 0


def command_check_absent(argv):
    prefix, suffix = argv[0], argv[1]
    existing = [path for path in planned_files(prefix, suffix) if os.path.lexists(path)]
    for path in existing:
        sys.stderr.write("refusing to overwrite existing key material: %s\n" % path)
    return 3 if existing else 0


def command_init_write(argv):
    prefix, suffix, uid, gid = argv[0], argv[1], int(argv[2]), int(argv[3])
    coverage_start, coverage_end, open_dates_csv = argv[4], argv[5], argv[6]
    open_dates = [value for value in open_dates_csv.split(",") if value]
    for kind in KIND_ORDER:
        temporary = "%s.tmp-%s" % (private_key_path(prefix, kind, suffix), argv[7])
        finalize_existing(temporary, private_key_path(prefix, kind, suffix), 0o600, uid, gid)
    calendar = calendar_document(coverage_start, coverage_end, open_dates)
    atomic_write(calendar_path(prefix), canonical(calendar), 0o600, uid, gid)
    for kind in KIND_ORDER:
        document = genesis_manifest(prefix, kind, suffix)
        atomic_write(manifest_path(prefix, kind), canonical(document), 0o600, uid, gid)
    return 0


def main(argv):
    command = argv[0]
    handlers = {
        "plan": command_plan,
        "check-absent": command_check_absent,
        "init-write": command_init_write,
    }
    handler = handlers.get(command)
    if handler is None:
        raise Failure("unknown worker command: %s" % command)
    return handler(argv[1:])


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Failure as error:
        sys.stderr.write("install-runtime-credential-keys: %s\n" % error)
        raise SystemExit(1)
PYSRC
)
readonly PY_HELPER

run_worker() {
    "${PYTHON_BIN}" -I -S -c "${PY_HELPER}" "$@"
}

require_absolute_prefix() {
    local value="$1"
    [[ "${value}" == /* && "${value}" != / ]] || fail "--prefix must be a non-root absolute path" 2
    [[ "${value}" != *".."* ]] || fail "--prefix must not contain '..'" 2
}

CONSUMED=0

parse_common_option() {
    # Sets CONSUMED to the number of arguments this option ate (0 = not mine).
    # It must never run in a subshell: the assignments below are the point.
    CONSUMED=0
    case "$1" in
        --prefix|--root)
            [[ $# -ge 2 ]] || usage
            require_absolute_prefix "$2"
            PREFIX="${2%/}"
            CONSUMED=2
            ;;
        --helper-dir)
            [[ $# -ge 2 ]] || usage
            [[ "$2" == /* ]] || fail "--helper-dir must be absolute" 2
            HELPER_DIR="${2%/}"
            CONSUMED=2
            ;;
    esac
}

validate_suffix() {
    local value="$1"
    [[ "${value}" =~ ^[a-z0-9][a-z0-9_.-]{0,100}$ ]] \
        || fail "key suffix must match ^[a-z0-9][a-z0-9_.-]{0,100}$: ${value}" 2
}

owner_uid() {
    if [[ -n "${PREFIX}" ]]; then
        /usr/bin/id -u
    else
        printf '0\n'
    fi
}

owner_gid() {
    if [[ -n "${PREFIX}" ]]; then
        /usr/bin/id -g
    else
        printf '0\n'
    fi
}

require_privilege() {
    if [[ -z "${PREFIX}" && "$(/usr/bin/id -u)" != "0" ]]; then
        fail "production mode requires root; pass --prefix DIR for a test root" 2
    fi
}

etc_root() {
    if [[ -n "${PREFIX}" ]]; then
        printf '%s/etc/rquant\n' "${PREFIX}"
    else
        printf '/etc/rquant\n'
    fi
}

ensure_directory() {
    local path="$1" mode="$2" uid="$3" gid="$4"
    if [[ -L "${path}" ]]; then
        fail "refusing to use a symlinked credential directory: ${path}"
    fi
    if [[ ! -d "${path}" ]]; then
        /bin/mkdir -p -- "${path}"
    fi
    /bin/chmod "${mode}" "${path}"
    "$(chown_binary)" "${uid}:${gid}" "${path}"
}

key_directory() {
    case "$1" in
        highwater) printf '%s/lab-highwater\n' "$(etc_root)" ;;
        canvas) printf '%s/canvas-publication\n' "$(etc_root)" ;;
        shadow) printf '%s/shadow-report\n' "$(etc_root)" ;;
        daily) printf '%s/daily-receipt\n' "$(etc_root)" ;;
        *) fail "unknown credential kind: $1" 2 ;;
    esac
}

key_id_prefix() {
    case "$1" in
        highwater) printf 'hw\n' ;;
        canvas) printf 'canvas\n' ;;
        shadow) printf 'shadow\n' ;;
        daily) printf 'daily\n' ;;
        *) fail "unknown credential kind: $1" 2 ;;
    esac
}

private_key_path() {
    printf '%s/%s-%s.private.pem\n' "$(key_directory "$1")" "$(key_id_prefix "$1")" "$2"
}

command_init() {
    while (( $# > 0 )); do
        parse_common_option "$@"
        if (( CONSUMED > 0 )); then
            shift "${CONSUMED}"
            continue
        fi
        case "$1" in
            --key-suffix)
                [[ $# -ge 2 ]] || usage
                KEY_SUFFIX="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=1
                shift
                ;;
            --calendar-coverage-start)
                [[ $# -ge 2 ]] || usage
                CALENDAR_COVERAGE_START="$2"
                shift 2
                ;;
            --calendar-coverage-end)
                [[ $# -ge 2 ]] || usage
                CALENDAR_COVERAGE_END="$2"
                shift 2
                ;;
            --calendar-open-dates)
                [[ $# -ge 2 ]] || usage
                CALENDAR_OPEN_DATES="$2"
                shift 2
                ;;
            *)
                usage
                ;;
        esac
    done
    validate_suffix "${KEY_SUFFIX}"
    require_privilege

    if (( DRY_RUN == 1 )); then
        run_worker plan "${PREFIX}" "${KEY_SUFFIX}"
        run_worker check-absent "${PREFIX}" "${KEY_SUFFIX}"
        printf 'dry run: nothing was written\n'
        return 0
    fi

    local status=0
    run_worker check-absent "${PREFIX}" "${KEY_SUFFIX}" || status=$?
    if (( status != 0 )); then
        exit "${status}"
    fi

    local uid gid openssl_bin kind target temporary
    uid="$(owner_uid)"
    gid="$(owner_gid)"
    openssl_bin="$(openssl_binary)"

    ensure_directory "$(etc_root)" 0755 "${uid}" "${gid}"
    for kind in highwater canvas shadow daily; do
        ensure_directory "$(key_directory "${kind}")" 0700 "${uid}" "${gid}"
    done

    for kind in highwater canvas shadow daily; do
        target="$(private_key_path "${kind}" "${KEY_SUFFIX}")"
        temporary="${target}.tmp-$$"
        TMP_PATHS+=("${temporary}")
        "${openssl_bin}" genpkey -algorithm ED25519 -out "${temporary}" >/dev/null
    done

    run_worker init-write \
        "${PREFIX}" \
        "${KEY_SUFFIX}" \
        "${uid}" \
        "${gid}" \
        "${CALENDAR_COVERAGE_START}" \
        "${CALENDAR_COVERAGE_END}" \
        "${CALENDAR_OPEN_DATES}" \
        "$$"
    TMP_PATHS=()

    printf 'created 9 credential files under %s\n' "$(etc_root)"
}

main() {
    (( $# > 0 )) || usage
    COMMAND="$1"
    shift
    case "${COMMAND}" in
        init) command_init "$@" ;;
        rotate) fail "rotate is not implemented yet" 2 ;;
        verify) fail "verify is not implemented yet" 2 ;;
        -h|--help|help) usage ;;
        *) usage ;;
    esac
}

main "$@"
