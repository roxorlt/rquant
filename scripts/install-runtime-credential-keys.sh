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
import re
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
MAX_KEY_FILE_BYTES = 64 * 1024
MAX_CALENDAR_FILE_BYTES = 4 * 1024 * 1024
KEY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ISO_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


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


def load_manifest(prefix, kind):
    path = manifest_path(prefix, kind)
    document, payload = read_json(path)
    if not isinstance(document, dict):
        raise Failure("%s is not a JSON object" % path)
    layout = KIND_LAYOUT[kind]
    required = {
        "schema_version",
        "active_key_id",
        "active_private_key_path",
        "previous_public_keys",
    }
    if layout["chained"]:
        required |= {"generation", "previous_manifest_hash"}
    if kind == "shadow":
        required |= {"legacy_recovery_calendar_path"}
    if set(document) != required:
        raise Failure("%s does not have the expected %s field set" % (path, kind))
    if document["schema_version"] != layout["schema_version"]:
        raise Failure("%s has an unexpected schema_version" % path)
    if not isinstance(document["previous_public_keys"], dict):
        raise Failure("%s has invalid previous_public_keys" % path)
    return document, payload


def command_rotate_current(argv):
    prefix, kind = argv[0], argv[1]
    document, _payload = load_manifest(prefix, kind)
    sys.stdout.write("%s\n" % document["active_private_key_path"])
    return 0


def command_rotate_write(argv):
    prefix, kind, suffix = argv[0], argv[1], argv[2]
    uid, gid = int(argv[3]), int(argv[4])
    retired_public_key_file, pid = argv[5], argv[6]
    layout = KIND_LAYOUT[kind]
    document, _payload = load_manifest(prefix, kind)

    retired_key_id = document["active_key_id"]
    retired_private_key = document["active_private_key_path"]
    active_key_id = "%s-%s" % (layout["key_prefix"], suffix)
    active_private_key = private_key_path(prefix, kind, suffix)
    require_normalized(active_private_key, "active_private_key_path")
    if active_key_id == retired_key_id:
        raise Failure("rotation must change the active key id (%s)" % active_key_id)
    if active_key_id in document["previous_public_keys"]:
        raise Failure("key id %s is already a retired key" % active_key_id)
    if active_private_key == retired_private_key:
        raise Failure("rotation must change the active private key path")
    if os.path.lexists(active_private_key):
        raise Failure("refusing to overwrite existing key material: %s" % active_private_key)

    with open(retired_public_key_file, "r") as handle:
        retired_public_key = handle.read()
    if "BEGIN PUBLIC KEY" not in retired_public_key:
        raise Failure("retired public key export is not a PEM public key")

    previous_public_keys = dict(document["previous_public_keys"])
    previous_public_keys[retired_key_id] = retired_public_key

    rotated = {
        "schema_version": layout["schema_version"],
        "active_key_id": active_key_id,
        "active_private_key_path": active_private_key,
        "previous_public_keys": previous_public_keys,
    }
    if kind == "shadow":
        rotated["legacy_recovery_calendar_path"] = document["legacy_recovery_calendar_path"]
    if layout["chained"]:
        chain_hash = public_keyring_manifest_hash(kind, document, retired_public_key)
        published = keyring_path(prefix, kind)
        if os.path.exists(published):
            current, _current_payload = read_json(published)
            if current.get("manifest_hash") != chain_hash:
                raise Failure(
                    "published keyring %s does not match the manifest it was exported "
                    "from; re-run install-runtime-credential-infra.sh before rotating"
                    % published
                )
        rotated["generation"] = int(document["generation"]) + 1
        rotated["previous_manifest_hash"] = chain_hash

    temporary = "%s.tmp-%s" % (active_private_key, pid)
    finalize_existing(temporary, active_private_key, 0o600, uid, gid)
    atomic_write(manifest_path(prefix, kind), canonical(rotated), 0o600, uid, gid)
    os.unlink(retired_private_key)
    fsync_directory(key_directory(prefix, kind))
    sys.stdout.write(
        "rotated %s: %s -> %s\n" % (kind, retired_key_id, active_key_id)
    )
    return 0


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


def inspect_path(path, expected_mode, uid, gid, label, problems, maximum_bytes, directory=False):
    try:
        observed = os.lstat(path)
    except OSError as exc:
        problems.append("%s is unavailable: %s" % (label, exc))
        return None
    if directory:
        if not stat.S_ISDIR(observed.st_mode):
            problems.append("%s is not a directory: %s" % (label, path))
            return None
    else:
        if not stat.S_ISREG(observed.st_mode):
            problems.append("%s is not a regular file: %s" % (label, path))
            return None
        if observed.st_nlink != 1:
            problems.append(
                "%s has nlink %d (expected 1): %s" % (label, observed.st_nlink, path)
            )
        if observed.st_size <= 0 or observed.st_size > maximum_bytes:
            problems.append(
                "%s has an unsafe size %d: %s" % (label, observed.st_size, path)
            )
    mode = stat.S_IMODE(observed.st_mode)
    if mode != expected_mode:
        problems.append(
            "%s has mode %04o (expected %04o): %s" % (label, mode, expected_mode, path)
        )
    if uid >= 0 and observed.st_uid != uid:
        problems.append(
            "%s has uid %d (expected %d): %s" % (label, observed.st_uid, uid, path)
        )
    if gid >= 0 and observed.st_gid != gid:
        problems.append(
            "%s has gid %d (expected %d): %s" % (label, observed.st_gid, gid, path)
        )
    return observed


def check_previous_public_keys(document, label, problems):
    previous = document.get("previous_public_keys")
    if not isinstance(previous, dict):
        problems.append("%s has invalid previous_public_keys" % label)
        return {}
    for key_id, public_key in previous.items():
        if not isinstance(key_id, str) or KEY_ID_PATTERN.match(key_id) is None:
            problems.append("%s has an invalid retired key id: %r" % (label, key_id))
        if key_id == document.get("active_key_id"):
            problems.append(
                "%s lists the active key %s as a retired key" % (label, key_id)
            )
        if not isinstance(public_key, str) or "BEGIN PUBLIC KEY" not in public_key:
            problems.append("%s has a retired key without a PEM public key: %s" % (label, key_id))
    return previous


def check_calendar(prefix, document, problems, uid, gid):
    path = document.get("legacy_recovery_calendar_path")
    expected = calendar_path(prefix)
    if path != expected:
        problems.append(
            "shadow manifest legacy_recovery_calendar_path is %r (expected %s)"
            % (path, expected)
        )
        return
    inspect_path(
        path,
        0o600,
        uid,
        gid,
        "shadow recovery calendar",
        problems,
        MAX_CALENDAR_FILE_BYTES,
    )
    if problems and problems[-1].startswith("shadow recovery calendar is unavailable"):
        return
    try:
        calendar, _payload = read_json(path)
    except Failure as exc:
        problems.append(str(exc))
        return
    expected_fields = {
        "schema_version",
        "exchange",
        "coverage_start",
        "coverage_end",
        "open_dates",
        "content_sha256",
    }
    if not isinstance(calendar, dict) or set(calendar) != expected_fields:
        problems.append("shadow recovery calendar does not have the six expected fields")
        return
    if calendar["schema_version"] != 1 or calendar["exchange"] != "SSE":
        problems.append("shadow recovery calendar schema_version/exchange are invalid")
    body = {key: value for key, value in calendar.items() if key != "content_sha256"}
    digest = hashlib.sha256(canonical(body)).hexdigest()
    if calendar.get("content_sha256") != digest:
        problems.append(
            "shadow recovery calendar content_sha256 does not match its own body"
        )
    open_dates = calendar.get("open_dates")
    start = calendar.get("coverage_start")
    end = calendar.get("coverage_end")
    for label, value in (("coverage_start", start), ("coverage_end", end)):
        if not isinstance(value, str) or ISO_DATE_PATTERN.match(value) is None:
            problems.append("shadow recovery calendar %s is invalid: %r" % (label, value))
            return
    if start > end:
        problems.append("shadow recovery calendar coverage_start is after coverage_end")
    if not isinstance(open_dates, list):
        problems.append("shadow recovery calendar open_dates is not a list")
        return
    if any(
        not isinstance(value, str) or ISO_DATE_PATTERN.match(value) is None
        for value in open_dates
    ):
        problems.append("shadow recovery calendar open_dates contains an invalid date")
        return
    if open_dates != sorted(set(open_dates)):
        problems.append(
            "shadow recovery calendar open_dates are not strictly ascending and unique"
        )
    if any(value < start or value > end for value in open_dates):
        problems.append(
            "shadow recovery calendar open_dates fall outside the coverage window"
        )


def command_verify_tree(argv):
    prefix, uid, gid = argv[0], int(argv[1]), int(argv[2])
    problems = []
    reported = []

    for path, mode in directories(prefix):
        inspect_path(path, mode, uid, gid, "credential directory", problems, 0, directory=True)

    for kind in KIND_ORDER:
        layout = KIND_LAYOUT[kind]
        path = manifest_path(prefix, kind)
        label = "%s key manifest" % kind
        if inspect_path(path, 0o600, uid, gid, label, problems, MAX_KEY_FILE_BYTES) is None:
            continue
        try:
            document, payload = read_json(path)
        except Failure as exc:
            problems.append(str(exc))
            continue
        if not isinstance(document, dict):
            problems.append("%s is not a JSON object" % label)
            continue
        expected_fields = {
            "schema_version",
            "active_key_id",
            "active_private_key_path",
            "previous_public_keys",
        }
        if layout["chained"]:
            expected_fields |= {"generation", "previous_manifest_hash"}
        if kind == "shadow":
            expected_fields |= {"legacy_recovery_calendar_path"}
        if set(document) != expected_fields:
            problems.append(
                "%s field set is %s (expected %s)"
                % (label, sorted(document), sorted(expected_fields))
            )
            continue
        if document["schema_version"] != layout["schema_version"]:
            problems.append(
                "%s schema_version is %r (expected %d)"
                % (label, document["schema_version"], layout["schema_version"])
            )
        active_key_id = document.get("active_key_id")
        if not isinstance(active_key_id, str) or KEY_ID_PATTERN.match(active_key_id) is None:
            problems.append("%s active_key_id is invalid: %r" % (label, active_key_id))
        previous = check_previous_public_keys(document, label, problems)
        if kind == "daily" and canonical(document) != payload:
            problems.append(
                "%s is not canonical JSON (ensure_ascii=True, sorted keys, no trailing newline)"
                % label
            )
        if layout["chained"]:
            generation = document.get("generation")
            chain = document.get("previous_manifest_hash")
            if type(generation) is not int or generation < 1:
                problems.append("%s generation is invalid: %r" % (label, generation))
                generation = None
            if not isinstance(chain, str) or HEX64_PATTERN.match(chain) is None:
                problems.append("%s previous_manifest_hash is invalid: %r" % (label, chain))
                chain = None
            if generation == 1 and (chain != GENESIS_HASH or previous):
                problems.append("%s genesis binding is invalid" % label)
            if generation is not None and generation > 1 and (chain == GENESIS_HASH or not previous):
                problems.append("%s rotation binding is invalid" % label)
        private_key = document.get("active_private_key_path")
        expected_directory = key_directory(prefix, kind)
        if (
            not isinstance(private_key, str)
            or not os.path.isabs(private_key)
            or private_key != os.path.abspath(private_key)
            or os.path.dirname(private_key) != expected_directory
        ):
            problems.append(
                "%s active_private_key_path must be a normalized path inside %s: %r"
                % (label, expected_directory, private_key)
            )
            reported.append(path)
            continue
        reported.append(path)
        key_label = "%s private key" % kind
        if (
            inspect_path(private_key, 0o600, uid, gid, key_label, problems, MAX_KEY_FILE_BYTES)
            is not None
        ):
            with open(private_key, "rb") as handle:
                head = handle.read(64)
            if not head.startswith(b"-----BEGIN PRIVATE KEY-----"):
                problems.append("%s is not a PKCS#8 PEM: %s" % (key_label, private_key))
        reported.append(private_key)
        if kind == "shadow":
            check_calendar(prefix, document, problems, uid, gid)
            reported.append(calendar_path(prefix))

    if problems:
        for problem in problems:
            sys.stderr.write("%s\n" % problem)
        return 1
    if len(reported) != 9:
        sys.stderr.write("expected nine credential files, inspected %d\n" % len(reported))
        return 1
    for path in reported:
        sys.stdout.write("OK %s\n" % path)
    return 0


def main(argv):
    command = argv[0]
    handlers = {
        "plan": command_plan,
        "check-absent": command_check_absent,
        "init-write": command_init_write,
        "rotate-current": command_rotate_current,
        "rotate-write": command_rotate_write,
        "verify-tree": command_verify_tree,
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

DAILY_STDIN_SEAM='from pathlib import Path; import runpy, sys; module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; operation(Path(sys.argv[2]))'
DAILY_VALIDATE_REQUEST='{"operation":"validate-key-material","schema_version":1}'
readonly DAILY_STDIN_SEAM DAILY_VALIDATE_REQUEST

resolve_helper_dir() {
    local candidate
    if [[ -n "${HELPER_DIR}" ]]; then
        printf '%s\n' "${HELPER_DIR}"
        return 0
    fi
    for candidate in "${PREFIX}/usr/local/libexec" "${PROJECT_DIR}/deploy/libexec"; do
        if [[ -f "${candidate}/rquant-canvas-publication-signer" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done
    fail "no credential helper directory found; pass --helper-dir DIR"
}

manifest_file() {
    case "$1" in
        highwater) printf '%s/lab-highwater-keys.json\n' "$(etc_root)" ;;
        canvas) printf '%s/canvas-publication-keys.json\n' "$(etc_root)" ;;
        shadow) printf '%s/shadow-report-keys.json\n' "$(etc_root)" ;;
        daily) printf '%s/daily-receipt-keys.json\n' "$(etc_root)" ;;
        *) fail "unknown credential kind: $1" 2 ;;
    esac
}

run_consumer_self_checks() {
    local helper_dir
    helper_dir="$(resolve_helper_dir)"
    if [[ "$(/usr/bin/id -u)" == "0" ]]; then
        # Root form: canvas/shadow/highwater take zero arguments and pin
        # /etc/rquant themselves; daily is stdin-only and also pins its path.
        "${PYTHON_BIN}" "${helper_dir}/rquant-lab-highwater-authority" \
            --validate-key-material >/dev/null
        "${PYTHON_BIN}" "${helper_dir}/rquant-canvas-publication-signer" \
            --validate-key-material >/dev/null
        "${PYTHON_BIN}" "${helper_dir}/rquant-shadow-report-signer" \
            --validate-key-material >/dev/null
        printf '%s' "${DAILY_VALIDATE_REQUEST}" \
            | "${PYTHON_BIN}" "${helper_dir}/rquant-daily-receipt-signer" >/dev/null
        printf 'consumer self-check passed (root form)\n' >&2
        return 0
    fi
    # Non-root form.  canvas/shadow accept `--keys-file <p> --validate-key-material`
    # only when euid != 0; the high-water authority has no non-root validate at all,
    # so it degrades to --export-public-keyring; the daily helper is zero-argument
    # with a module-level KEYS_FILE, so it is driven through the same runpy seam
    # scripts/install-runtime-credential-infra.sh already uses.
    "${PYTHON_BIN}" "${helper_dir}/rquant-canvas-publication-signer" \
        --keys-file "$(manifest_file canvas)" --validate-key-material >/dev/null
    "${PYTHON_BIN}" "${helper_dir}/rquant-shadow-report-signer" \
        --keys-file "$(manifest_file shadow)" --validate-key-material >/dev/null
    "${PYTHON_BIN}" "${helper_dir}/rquant-lab-highwater-authority" \
        --keys-file "$(manifest_file highwater)" --export-public-keyring >/dev/null
    printf '%s' "${DAILY_VALIDATE_REQUEST}" \
        | "${PYTHON_BIN}" -I -S -c "${DAILY_STDIN_SEAM}" \
            "${helper_dir}/rquant-daily-receipt-signer" \
            "$(manifest_file daily)" >/dev/null
    printf 'consumer self-check passed (non-root --prefix form)\n' >&2
}

command_verify() {
    while (( $# > 0 )); do
        parse_common_option "$@"
        if (( CONSUMED > 0 )); then
            shift "${CONSUMED}"
            continue
        fi
        usage
    done
    require_privilege
    if [[ -n "${PREFIX}" && "$(/usr/bin/id -u)" == "0" ]]; then
        fail "verify --prefix must run unprivileged: the helpers reject --keys-file for euid 0" 2
    fi
    run_worker verify-tree "${PREFIX}" "$(owner_uid)" "$(owner_gid)"
    run_consumer_self_checks
}

command_rotate() {
    (( $# > 0 )) || usage
    ROTATE_TARGET="$1"
    shift
    case "${ROTATE_TARGET}" in
        highwater|canvas|shadow|daily) ;;
        *) fail "rotate target must be one of highwater|canvas|shadow|daily" 2 ;;
    esac
    while (( $# > 0 )); do
        parse_common_option "$@"
        if (( CONSUMED > 0 )); then
            shift "${CONSUMED}"
            continue
        fi
        case "$1" in
            --new-key-suffix)
                [[ $# -ge 2 ]] || usage
                NEW_KEY_SUFFIX="$2"
                shift 2
                ;;
            *)
                usage
                ;;
        esac
    done
    [[ -n "${NEW_KEY_SUFFIX}" ]] || fail "rotate requires --new-key-suffix SUFFIX" 2
    validate_suffix "${NEW_KEY_SUFFIX}"
    require_privilege

    local uid gid openssl_bin retired_private target temporary public_export
    uid="$(owner_uid)"
    gid="$(owner_gid)"
    openssl_bin="$(openssl_binary)"

    retired_private="$(run_worker rotate-current "${PREFIX}" "${ROTATE_TARGET}")"
    [[ -f "${retired_private}" ]] || fail "retired private key is missing: ${retired_private}"

    target="$(private_key_path "${ROTATE_TARGET}" "${NEW_KEY_SUFFIX}")"
    temporary="${target}.tmp-$$"
    public_export="${target}.pub-$$"
    TMP_PATHS+=("${temporary}" "${public_export}")

    "${openssl_bin}" pkey -in "${retired_private}" -pubout -out "${public_export}" >/dev/null
    "${openssl_bin}" genpkey -algorithm ED25519 -out "${temporary}" >/dev/null

    run_worker rotate-write \
        "${PREFIX}" \
        "${ROTATE_TARGET}" \
        "${NEW_KEY_SUFFIX}" \
        "${uid}" \
        "${gid}" \
        "${public_export}" \
        "$$"
    /bin/rm -f -- "${public_export}"
    TMP_PATHS=()
}

main() {
    (( $# > 0 )) || usage
    COMMAND="$1"
    shift
    case "${COMMAND}" in
        init) command_init "$@" ;;
        rotate) command_rotate "$@" ;;
        verify) command_verify "$@" ;;
        -h|--help|help) usage ;;
        *) usage ;;
    esac
}

main "$@"
