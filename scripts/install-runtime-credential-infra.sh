#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-runtime-credential-sealer"
HIGHWATER_HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-lab-highwater-authority"
CANVAS_HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-canvas-publication-signer"
SHADOW_HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-shadow-report-signer"
DAILY_HELPER_SOURCE="${PROJECT_DIR}/deploy/libexec/rquant-daily-receipt-signer"
DAILY_AUTHORITY_SOURCE="${PROJECT_DIR}/deploy/root-runtime/daily_receipt_authority.py"
DAILY_SOCKET_UNIT_SOURCE="${PROJECT_DIR}/deploy/systemd/rquant-daily-receipt-signer.socket"
DAILY_SERVICE_UNIT_SOURCE="${PROJECT_DIR}/deploy/systemd/rquant-daily-receipt-signer.service"
SUDOERS_SOURCE="${PROJECT_DIR}/deploy/sudoers/rquant-production-deploy"
TEST_ROOT=""
FAIL_STEP=""
TEST_DAILY_AUTHORITY_SOURCE=""
TEST_SYSTEMCTL_BIN="${RQUANT_TEST_SYSTEMCTL_BIN:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --test-root)
            TEST_ROOT="${2:?missing --test-root value}"
            shift 2
            ;;
        --fail-step)
            FAIL_STEP="${2:?missing --fail-step value}"
            shift 2
            ;;
        --test-daily-authority-source)
            TEST_DAILY_AUTHORITY_SOURCE="${2:?missing --test-daily-authority-source value}"
            shift 2
            ;;
        --test-systemctl)
            TEST_SYSTEMCTL_BIN="${2:?missing --test-systemctl value}"
            shift 2
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done

if [[ -n "${FAIL_STEP}" && -z "${TEST_ROOT}" ]]; then
    printf '%s\n' '--fail-step is test-only' >&2
    exit 2
fi
if [[ -n "${TEST_DAILY_AUTHORITY_SOURCE}" && -z "${TEST_ROOT}" ]]; then
    printf '%s\n' '--test-daily-authority-source is test-only' >&2
    exit 2
fi
if [[ -n "${TEST_SYSTEMCTL_BIN}" && -z "${TEST_ROOT}" ]]; then
    printf '%s\n' '--test-systemctl is test-only' >&2
    exit 2
fi
if [[ -n "${TEST_SYSTEMCTL_BIN}" && "${TEST_SYSTEMCTL_BIN}" != /* ]]; then
    printf '%s\n' '--test-systemctl must be an absolute path' >&2
    exit 2
fi
if [[ -n "${TEST_ROOT}" ]]; then
    if [[ "${TEST_ROOT}" != /* || "${TEST_ROOT}" == "/" ]]; then
        printf '%s\n' '--test-root must be a non-root absolute path' >&2
        exit 2
    fi
    PREFIX="${TEST_ROOT}"
    VISUDO_BIN="/usr/bin/true"
else
    PREFIX=""
    VISUDO_BIN="/usr/sbin/visudo"
fi
if [[ -n "${TEST_DAILY_AUTHORITY_SOURCE}" ]]; then
    if [[ "${TEST_DAILY_AUTHORITY_SOURCE}" != /* ]]; then
        printf '%s\n' '--test-daily-authority-source must be an absolute path' >&2
        exit 2
    fi
    DAILY_AUTHORITY_SOURCE="${TEST_DAILY_AUTHORITY_SOURCE}"
fi

HELPER_DIR="${PREFIX}/usr/local/libexec"
HELPER_TARGET="${HELPER_DIR}/rquant-runtime-credential-sealer"
HIGHWATER_HELPER_TARGET="${HELPER_DIR}/rquant-lab-highwater-authority"
CANVAS_HELPER_TARGET="${HELPER_DIR}/rquant-canvas-publication-signer"
SHADOW_HELPER_TARGET="${HELPER_DIR}/rquant-shadow-report-signer"
DAILY_HELPER_TARGET="${HELPER_DIR}/rquant-daily-receipt-signer"
DAILY_AUTHORITY_ROOT="${HELPER_DIR}/rquant-daily-receipt-authority"
DAILY_AUTHORITY_RELEASES_DIR="${DAILY_AUTHORITY_ROOT}/releases"
DAILY_AUTHORITY_RELEASE_SHA=""
DAILY_AUTHORITY_RELEASE_DIR=""
DAILY_AUTHORITY_CURRENT="${DAILY_AUTHORITY_ROOT}/current"
DAILY_AUTHORITY_STAGE_DIR="${DAILY_AUTHORITY_RELEASES_DIR}/.stage-unset.$$"
DAILY_AUTHORITY_SWITCHED=0
DAILY_AUTHORITY_HAD_CURRENT=0
DAILY_AUTHORITY_OLD_TARGET=""
DAILY_AUTHORITY_SOCKET_WAS_ACTIVE=0
DAILY_AUTHORITY_SERVICE_WAS_ACTIVE=0
DAILY_AUTHORITY_SOCKET_WAS_ENABLED=0
DAILY_AUTHORITY_SERVICE_WAS_ENABLED=0
if [[ -n "${TEST_ROOT}" && -n "${TEST_SYSTEMCTL_BIN}" ]]; then
    SYSTEMD_LIFECYCLE_ENABLED=1
elif [[ -z "${TEST_ROOT}" ]]; then
    SYSTEMD_LIFECYCLE_ENABLED=1
else
    SYSTEMD_LIFECYCLE_ENABLED=0
fi
SYSTEMD_TARGET_DIR="${PREFIX}/etc/systemd/system"
DAILY_SOCKET_UNIT_TARGET="${SYSTEMD_TARGET_DIR}/rquant-daily-receipt-signer.socket"
DAILY_SERVICE_UNIT_TARGET="${SYSTEMD_TARGET_DIR}/rquant-daily-receipt-signer.service"
HIGHWATER_STATE_DIR="${PREFIX}/var/lib/rquant/lab-highwater"
DAILY_STATE_DIR="${PREFIX}/var/lib/rquant/daily-receipt-signer"
SHADOW_RECOVERY_STATE_DIR="${PREFIX}/var/lib/rquant/shadow-recovery"
HIGHWATER_KEYS_FILE="${PREFIX}/etc/rquant/lab-highwater-keys.json"
HIGHWATER_PUBLIC_KEYS_FILE="${PREFIX}/etc/rquant/lab-highwater-trusted-keys.json"
CANVAS_KEYS_FILE="${PREFIX}/etc/rquant/canvas-publication-keys.json"
CANVAS_PUBLIC_KEYS_FILE="${PREFIX}/etc/rquant/canvas-publication-trusted-keys.json"
SHADOW_KEYS_FILE="${PREFIX}/etc/rquant/shadow-report-keys.json"
SHADOW_PUBLIC_KEYS_FILE="${PREFIX}/etc/rquant/shadow-report-trusted-keys.json"
DAILY_KEYS_FILE="${PREFIX}/etc/rquant/daily-receipt-keys.json"
DAILY_PUBLIC_KEYS_FILE="${PREFIX}/etc/rquant/daily-receipt-trusted-keys.json"
SUDOERS_DIR="${PREFIX}/etc/sudoers.d"
SUDOERS_TARGET="${SUDOERS_DIR}/rquant-production-deploy"
HELPER_STAGING="${HELPER_TARGET}.tmp.$$"
HIGHWATER_HELPER_STAGING="${HIGHWATER_HELPER_TARGET}.tmp.$$"
CANVAS_HELPER_STAGING="${CANVAS_HELPER_TARGET}.tmp.$$"
SHADOW_HELPER_STAGING="${SHADOW_HELPER_TARGET}.tmp.$$"
DAILY_HELPER_STAGING="${DAILY_HELPER_TARGET}.tmp.$$"
DAILY_SOCKET_UNIT_STAGING="${DAILY_SOCKET_UNIT_TARGET}.tmp.$$"
DAILY_SERVICE_UNIT_STAGING="${DAILY_SERVICE_UNIT_TARGET}.tmp.$$"
if [[ -n "${TEST_ROOT}" ]]; then
    HIGHWATER_PUBLIC_EXPORT="${HIGHWATER_PUBLIC_KEYS_FILE}.export.$$"
    CANVAS_PUBLIC_EXPORT="${CANVAS_PUBLIC_KEYS_FILE}.export.$$"
    SHADOW_PUBLIC_EXPORT="${SHADOW_PUBLIC_KEYS_FILE}.export.$$"
    DAILY_PUBLIC_EXPORT="${DAILY_PUBLIC_KEYS_FILE}.export.$$"
else
    HIGHWATER_PUBLIC_EXPORT="/tmp/rquant-lab-highwater-trusted-keys.$$.json"
    CANVAS_PUBLIC_EXPORT="/tmp/rquant-canvas-publication-trusted-keys.$$.json"
    SHADOW_PUBLIC_EXPORT="/tmp/rquant-shadow-report-trusted-keys.$$.json"
    DAILY_PUBLIC_EXPORT="/tmp/rquant-daily-receipt-trusted-keys.$$.json"
fi
HIGHWATER_PUBLIC_STAGING="${HIGHWATER_PUBLIC_KEYS_FILE}.tmp.$$"
CANVAS_PUBLIC_STAGING="${CANVAS_PUBLIC_KEYS_FILE}.tmp.$$"
SHADOW_PUBLIC_STAGING="${SHADOW_PUBLIC_KEYS_FILE}.tmp.$$"
DAILY_PUBLIC_STAGING="${DAILY_PUBLIC_KEYS_FILE}.tmp.$$"
SUDOERS_STAGING="${SUDOERS_TARGET}.tmp.$$"
SUDOERS_BACKUP="${SUDOERS_TARGET}.backup"

cleanup() {
    local status=$?
    if [[ "${status}" -ne 0 && "${DAILY_AUTHORITY_SWITCHED}" -eq 1 ]]; then
        rollback_daily_authority_switch || true
    fi
    if [[ -n "${TEST_ROOT}" ]]; then
        /bin/rm -f \
            "${HELPER_STAGING}" \
            "${HIGHWATER_HELPER_STAGING}" \
            "${CANVAS_HELPER_STAGING}" \
            "${SHADOW_HELPER_STAGING}" \
            "${DAILY_HELPER_STAGING}" \
            "${DAILY_SOCKET_UNIT_STAGING}" \
            "${DAILY_SERVICE_UNIT_STAGING}" \
            "${HIGHWATER_PUBLIC_EXPORT}" \
            "${HIGHWATER_PUBLIC_STAGING}" \
            "${CANVAS_PUBLIC_EXPORT}" \
            "${CANVAS_PUBLIC_STAGING}" \
            "${SHADOW_PUBLIC_EXPORT}" \
            "${SHADOW_PUBLIC_STAGING}" \
            "${DAILY_PUBLIC_EXPORT}" \
            "${DAILY_PUBLIC_STAGING}" \
            "${SUDOERS_STAGING}"
        /bin/rm -rf "${DAILY_AUTHORITY_STAGE_DIR}"
    else
        /bin/rm -f "${HIGHWATER_PUBLIC_EXPORT}" || true
        sudo /bin/rm -f \
            "${HELPER_STAGING}" \
            "${HIGHWATER_HELPER_STAGING}" \
            "${CANVAS_HELPER_STAGING}" \
            "${SHADOW_HELPER_STAGING}" \
            "${DAILY_HELPER_STAGING}" \
            "${DAILY_SOCKET_UNIT_STAGING}" \
            "${DAILY_SERVICE_UNIT_STAGING}" \
            "${HIGHWATER_PUBLIC_STAGING}" \
            "${CANVAS_PUBLIC_STAGING}" \
            "${SHADOW_PUBLIC_STAGING}" \
            "${DAILY_PUBLIC_STAGING}" \
            "${SUDOERS_STAGING}" || true
        sudo /bin/rm -rf "${DAILY_AUTHORITY_STAGE_DIR}" || true
        /bin/rm -f "${CANVAS_PUBLIC_EXPORT}" || true
        /bin/rm -f "${SHADOW_PUBLIC_EXPORT}" || true
        /bin/rm -f "${DAILY_PUBLIC_EXPORT}" || true
    fi
    return "${status}"
}
trap cleanup EXIT

run_step() {
    local name="$1"
    shift
    if [[ ",${FAIL_STEP}," == *",${name},"* ]]; then
        printf 'Injected failure: %s\n' "${name}" >&2
        return 97
    fi
    "$@"
}

privileged() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

systemctl_run() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "${TEST_SYSTEMCTL_BIN}" "$@"
    else
        sudo /usr/bin/systemctl "$@"
    fi
}

install_helper_directory() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -d -m 0755 "$1"
    else
        sudo /usr/bin/install -d -o root -g root -m 0755 "$1"
    fi
}

install_private_directory() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -d -m 0700 "$1"
    else
        sudo /usr/bin/install -d -o root -g root -m 0700 "$1"
    fi
}

install_shadow_recovery_state_directory() {
    if privileged /bin/test -L "${SHADOW_RECOVERY_STATE_DIR}"; then
        printf '%s\n' 'Shadow recovery state directory must not be a symbolic link' >&2
        exit 1
    fi
    install_private_directory "${SHADOW_RECOVERY_STATE_DIR}"
}

install_systemd_directory() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -d -m 0755 "$1"
    else
        sudo /usr/bin/install -d -o root -g root -m 0755 "$1"
    fi
}

path_exists() {
    privileged /bin/test -e "$1"
}

test_stat_metadata() {
    local macos_format="$1"
    local linux_format="$2"
    local path="$3"
    case "$(/usr/bin/uname -s)" in
        Darwin)
            /usr/bin/stat -f "${macos_format}" "${path}"
            ;;
        Linux)
            /usr/bin/stat -c "${linux_format}" "${path}"
            ;;
        *)
            printf 'Unsupported test metadata platform\n' >&2
            exit 1
            ;;
    esac
}

ensure_sudoers_directory() {
    if ! path_exists "${SUDOERS_DIR}"; then
        if [[ -n "${TEST_ROOT}" ]]; then
            /usr/bin/install -d -m 0750 "${SUDOERS_DIR}"
        else
            sudo /usr/bin/install -d -o root -g root -m 0750 "${SUDOERS_DIR}"
        fi
        return
    fi

    local actual expected_owner mode
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${SUDOERS_DIR}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${SUDOERS_DIR}")"
        expected_owner="0:0"
    fi
    mode="${actual##*:}"
    if [[ "${actual%:*}" != "${expected_owner}" || ( "${mode}" != "750" && "${mode}" != "700" ) ]]; then
        printf 'Unsafe sudoers directory state: %s (expected owner %s and mode 750 or 700)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

recover_stale_sudoers_backup() {
    if ! path_exists "${SUDOERS_BACKUP}"; then
        return
    fi
    if ! privileged "${VISUDO_BIN}" -cf "${SUDOERS_BACKUP}"; then
        printf 'Preserved invalid sudoers backup for manual recovery: %s\n' \
            "${SUDOERS_BACKUP}" >&2
        exit 1
    fi
    privileged /bin/mv -f "${SUDOERS_BACKUP}" "${SUDOERS_TARGET}"
}

install_file() {
    local mode="$1"
    local source="$2"
    local target="$3"
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -m "${mode}" "${source}" "${target}"
    else
        sudo /usr/bin/install -o root -g root -m "${mode}" "${source}" "${target}"
    fi
}

daily_authority_sha256_file() {
    local path="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "${path}" | /usr/bin/awk '{print $1}'
    else
        /usr/bin/shasum -a 256 "${path}" | /usr/bin/awk '{print $1}'
    fi
}

capture_daily_authority_source() {
    local target="$1"
    local -a command
    if [[ -n "${TEST_ROOT}" ]]; then
        command=(/usr/bin/python3 -I -S)
    else
        command=(sudo /usr/bin/python3 -I -S)
    fi
    "${command[@]}" - "${DAILY_AUTHORITY_SOURCE}" "${target}" <<'PY'
import os
import stat
import sys

source = sys.argv[1]
target = sys.argv[2]
source_fd = -1
target_fd = -1
try:
    source_fd = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    before = os.fstat(source_fd)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise RuntimeError("Daily authority source is not a single-link regular file")
    target_fd = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o444,
    )
    while True:
        chunk = os.read(source_fd, 1024 * 1024)
        if not chunk:
            break
        view = memoryview(chunk)
        while view:
            written = os.write(target_fd, view)
            view = view[written:]
    os.fsync(target_fd)
    after = os.fstat(source_fd)
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise RuntimeError("Daily authority source changed while it was captured")
    os.fchmod(target_fd, 0o444)
finally:
    if target_fd >= 0:
        os.close(target_fd)
    if source_fd >= 0:
        os.close(source_fd)
PY
}

daily_authority_owner_and_mode() {
    local path="$1"
    if [[ -n "${TEST_ROOT}" ]]; then
        test_stat_metadata '%u:%g:%Lp:%l' '%u:%g:%a:%h' "${path}"
    else
        sudo /usr/bin/stat -c '%u:%g:%a:%h' "${path}"
    fi
}

validate_daily_authority_ancestor_chain() {
    local allow_missing_authority="${1:-0}"
    local -a command
    if [[ -n "${TEST_ROOT}" ]]; then
        command=(/usr/bin/python3 -I -S)
    else
        command=(sudo /usr/bin/python3 -I -S)
    fi
    "${command[@]}" - "${PREFIX}" "${DAILY_AUTHORITY_ROOT}" "${allow_missing_authority}" <<'PY'
import os
import stat
import sys

prefix = sys.argv[1]
authority_root = sys.argv[2]
allow_missing_authority = sys.argv[3] == "1"
expected_uid = os.geteuid()
if not prefix:
    prefix = "/"
    expected_uid = 0
components = (
    "usr",
    "local",
    "libexec",
    "rquant-daily-receipt-authority",
    "releases",
)
expected_authority_root = os.path.join(prefix, *components[:-1])
if os.path.normpath(authority_root) != os.path.normpath(expected_authority_root):
    raise SystemExit("Unsafe Daily authority ancestor path")

parent_fd = os.open(prefix, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
try:
    for component in components:
        try:
            named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            child_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            if allow_missing_authority:
                break
            raise SystemExit(
                f"Unsafe Daily authority ancestor: {os.path.join(prefix, component)}"
            ) from None
        except OSError as exc:
            raise SystemExit(
                f"Unsafe Daily authority ancestor: {os.path.join(prefix, component)}"
            ) from exc
        try:
            opened = os.fstat(child_fd)
            if (
                not stat.S_ISDIR(named.st_mode)
                or stat.S_ISLNK(named.st_mode)
                or named.st_uid != expected_uid
                or named.st_mode & 0o022
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise SystemExit(
                    f"Unsafe Daily authority ancestor: {os.path.join(prefix, component)}"
                )
        except BaseException:
            os.close(child_fd)
            raise
        os.close(parent_fd)
        parent_fd = child_fd
        prefix = os.path.join(prefix, component)
finally:
    os.close(parent_fd)
PY
}

replace_daily_authority_pointer() {
    local staging="$1"
    local target="$2"
    local -a command
    if [[ -n "${TEST_ROOT}" ]]; then
        command=(/usr/bin/python3 -I -S)
    else
        command=(sudo /usr/bin/python3 -I -S)
    fi
    "${command[@]}" - "${staging}" "${target}" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
}

validate_daily_authority_zipapp() {
    local artifact="$1"
    local expected_sha="$2"
    local -a command
    if [[ -n "${TEST_ROOT}" ]]; then
        command=(/usr/bin/python3 -I -S)
    else
        command=(sudo /usr/bin/python3 -I -S)
    fi
    "${command[@]}" - "${artifact}" "${expected_sha}" <<'PY'
from hashlib import sha256
import sys
import zipfile

artifact, expected_sha = sys.argv[1:]
try:
    with zipfile.ZipFile(artifact) as bundle:
        if bundle.namelist() != ["__main__.py"]:
            raise RuntimeError("Daily authority zipapp has unexpected contents")
        source = bundle.read("__main__.py")
except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
    raise SystemExit(f"Daily authority zipapp is invalid: {exc}") from exc
if sha256(source).hexdigest() != expected_sha:
    raise SystemExit("Daily authority zipapp source does not match release SHA")
PY
}

validate_daily_authority_release() {
    local artifact="${DAILY_AUTHORITY_RELEASE_DIR}/authority.pyz"
    local source_hash="${DAILY_AUTHORITY_RELEASE_DIR}/source.sha256"
    local expected_owner actual content
    if [[ -n "${TEST_ROOT}" ]]; then
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        expected_owner="0:0"
    fi
    if privileged /bin/test -L "${DAILY_AUTHORITY_RELEASE_DIR}" || \
        privileged /bin/test -L "${artifact}" || \
        privileged /bin/test -L "${source_hash}"; then
        printf '%s\n' 'Unsafe Daily authority release symlink' >&2
        exit 1
    fi
    actual="$(daily_authority_owner_and_mode "${DAILY_AUTHORITY_RELEASE_DIR}")"
    if [[ "${actual}" != "${expected_owner}:755:"* ]]; then
        printf 'Unsafe Daily authority release directory: %s\n' "${actual}" >&2
        exit 1
    fi
    actual="$(daily_authority_owner_and_mode "${artifact}")"
    if [[ "${actual}" != "${expected_owner}:555:1" ]]; then
        printf 'Unsafe Daily authority artifact: %s\n' "${actual}" >&2
        exit 1
    fi
    actual="$(daily_authority_owner_and_mode "${source_hash}")"
    if [[ "${actual}" != "${expected_owner}:444:1" ]]; then
        printf 'Unsafe Daily authority source hash: %s\n' "${actual}" >&2
        exit 1
    fi
    content="$(privileged /bin/cat "${source_hash}")"
    if [[ "${content}" != "${DAILY_AUTHORITY_RELEASE_SHA}" ]]; then
        printf '%s\n' 'Daily authority source hash does not bind its release directory' >&2
        exit 1
    fi
    validate_daily_authority_zipapp "${artifact}" "${DAILY_AUTHORITY_RELEASE_SHA}"
}

validate_daily_authority_current_target() {
    local expected_target="$1"
    local current_target links expected_owner actual
    if [[ -n "${TEST_ROOT}" ]]; then
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        expected_owner="0:0"
    fi
    if ! privileged /bin/test -L "${DAILY_AUTHORITY_CURRENT}"; then
        printf '%s\n' 'Daily authority current pointer is not a symlink' >&2
        return 1
    fi
    if privileged /bin/test -L "${DAILY_AUTHORITY_ROOT}" || \
        privileged /bin/test -L "${DAILY_AUTHORITY_RELEASES_DIR}"; then
        printf '%s\n' 'Daily authority root or releases directory is a symlink' >&2
        return 1
    fi
    current_target="$(privileged /usr/bin/readlink "${DAILY_AUTHORITY_CURRENT}")"
    if [[ "${current_target}" != "${expected_target}" ]]; then
        printf 'Daily authority current pointer is not the expected immutable release: %s\n' \
            "${expected_target}" >&2
        return 1
    fi
    links="$(privileged /usr/bin/find "${DAILY_AUTHORITY_ROOT}" -maxdepth 1 -type l -print | /usr/bin/wc -l | /usr/bin/tr -d ' ')"
    if [[ "${links}" != "1" ]]; then
        printf '%s\n' 'Daily authority root must contain exactly one symlink' >&2
        return 1
    fi
    actual="$(daily_authority_owner_and_mode "${DAILY_AUTHORITY_ROOT}")"
    if [[ "${actual}" != "${expected_owner}:755:"* ]]; then
        printf 'Unsafe Daily authority root: %s\n' "${actual}" >&2
        return 1
    fi
    actual="$(daily_authority_owner_and_mode "${DAILY_AUTHORITY_RELEASES_DIR}")"
    if [[ "${actual}" != "${expected_owner}:755:"* ]]; then
        printf 'Unsafe Daily authority releases directory: %s\n' "${actual}" >&2
        return 1
    fi
    validate_daily_authority_ancestor_chain
}

validate_daily_authority_current() {
    validate_daily_authority_current_target "releases/${DAILY_AUTHORITY_RELEASE_SHA}"
}

prepare_daily_authority_release() {
    local build_dir artifact_staging hash_staging source_staging
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -d -m 0755 "${DAILY_AUTHORITY_ROOT}" "${DAILY_AUTHORITY_RELEASES_DIR}"
        /usr/bin/install -d -m 0755 "${DAILY_AUTHORITY_STAGE_DIR}"
    else
        sudo /usr/bin/install -d -o root -g root -m 0755 \
            "${DAILY_AUTHORITY_ROOT}" "${DAILY_AUTHORITY_RELEASES_DIR}"
        sudo /usr/bin/install -d -o root -g root -m 0755 "${DAILY_AUTHORITY_STAGE_DIR}"
    fi
    validate_daily_authority_ancestor_chain
    source_staging="${DAILY_AUTHORITY_STAGE_DIR}/source.py"
    capture_daily_authority_source "${source_staging}"
    DAILY_AUTHORITY_RELEASE_SHA="$(daily_authority_sha256_file "${source_staging}")"
    if [[ ! "${DAILY_AUTHORITY_RELEASE_SHA}" =~ ^[0-9a-f]{64}$ ]]; then
        printf '%s\n' 'Daily authority source hash is invalid' >&2
        exit 1
    fi
    DAILY_AUTHORITY_RELEASE_DIR="${DAILY_AUTHORITY_RELEASES_DIR}/${DAILY_AUTHORITY_RELEASE_SHA}"
    if path_exists "${DAILY_AUTHORITY_RELEASE_DIR}"; then
        validate_daily_authority_release
    else
        build_dir="${DAILY_AUTHORITY_STAGE_DIR}/build"
        artifact_staging="${DAILY_AUTHORITY_STAGE_DIR}/authority.pyz"
        hash_staging="${DAILY_AUTHORITY_STAGE_DIR}/source.sha256"
        if [[ -n "${TEST_ROOT}" ]]; then
            /usr/bin/install -d -m 0755 "${build_dir}"
            /bin/mv "${source_staging}" "${build_dir}/__main__.py"
            /usr/bin/python3 -I -S -m zipapp "${build_dir}" -o "${artifact_staging}"
            /bin/chmod 0555 "${artifact_staging}"
            printf '%s\n' "${DAILY_AUTHORITY_RELEASE_SHA}" >"${hash_staging}"
            /bin/chmod 0444 "${hash_staging}"
            /bin/rm -rf "${build_dir}"
            /bin/mv "${DAILY_AUTHORITY_STAGE_DIR}" "${DAILY_AUTHORITY_RELEASE_DIR}"
        else
            sudo /usr/bin/install -d -o root -g root -m 0755 "${build_dir}"
            sudo /bin/mv "${source_staging}" "${build_dir}/__main__.py"
            sudo /usr/bin/python3 -I -S -m zipapp "${build_dir}" -o "${artifact_staging}"
            sudo /bin/chmod 0555 "${artifact_staging}"
            printf '%s\n' "${DAILY_AUTHORITY_RELEASE_SHA}" | sudo /usr/bin/tee "${hash_staging}" >/dev/null
            sudo /bin/chmod 0444 "${hash_staging}"
            sudo /bin/rm -rf "${build_dir}"
            sudo /bin/mv "${DAILY_AUTHORITY_STAGE_DIR}" "${DAILY_AUTHORITY_RELEASE_DIR}"
        fi
        DAILY_AUTHORITY_STAGE_DIR="${DAILY_AUTHORITY_RELEASES_DIR}/.stage-complete.$$"
        validate_daily_authority_release
    fi
}

record_daily_authority_service_state() {
    if [[ "${SYSTEMD_LIFECYCLE_ENABLED}" -ne 1 ]]; then
        return
    fi
    if systemctl_run is-active --quiet rquant-daily-receipt-signer.socket; then
        DAILY_AUTHORITY_SOCKET_WAS_ACTIVE=1
    fi
    if systemctl_run is-active --quiet rquant-daily-receipt-signer.service; then
        DAILY_AUTHORITY_SERVICE_WAS_ACTIVE=1
    fi
    if systemctl_run is-enabled --quiet rquant-daily-receipt-signer.socket; then
        DAILY_AUTHORITY_SOCKET_WAS_ENABLED=1
    fi
    if systemctl_run is-enabled --quiet rquant-daily-receipt-signer.service; then
        DAILY_AUTHORITY_SERVICE_WAS_ENABLED=1
    fi
}

daily_authority_runtime_identity() {
    local socket_endpoint keyring_owner openssl_path
    if [[ -n "${TEST_ROOT}" ]]; then
        socket_endpoint="${RQUANT_TEST_DAILY_AUTHORITY_SOCKET:-}"
        if [[ -z "${socket_endpoint}" || "${socket_endpoint}" != /* ]]; then
            printf '%s\n' 'Daily authority test identity socket is not configured' >&2
            return 1
        fi
        keyring_owner="$(/usr/bin/id -u)"
    else
        socket_endpoint="/run/rquant/daily-receipt-signer.sock"
        keyring_owner=0
    fi
    openssl_path="/usr/bin/openssl"
    if [[ -n "${TEST_ROOT}" && -x /opt/homebrew/bin/openssl ]]; then
        openssl_path="/opt/homebrew/bin/openssl"
    fi

    # The only identity source is the root authority's fixed socket.  The
    # response is bound to a fresh nonce and protocol, so a systemd property,
    # command line, or installer input can never self-assert the SHA.
    local -a command
    if [[ -n "${TEST_ROOT}" || "$(/usr/bin/id -un)" == "lighthouse" ]]; then
        command=(/usr/bin/python3 -I -S)
    else
        command=(sudo -n -u lighthouse /usr/bin/python3 -I -S)
    fi
    "${command[@]}" - "${socket_endpoint}" "${DAILY_PUBLIC_KEYS_FILE}" \
        "${keyring_owner}" "${openssl_path}" <<'PY'
import base64
import hashlib
import json
import os
import re
import stat
import secrets
import socket
import subprocess
import sys
import tempfile

KEY_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")

endpoint = sys.argv[1]
keyring_path = sys.argv[2]
expected_keyring_uid = int(sys.argv[3])
openssl_path = os.path.realpath(sys.argv[4])
if not endpoint.startswith("/"):
    raise SystemExit("Daily authority identity endpoint is invalid")

def canonical(value, *, ensure_ascii=False):
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

def recv_exact(connection, size):
    chunks = []
    while size:
        chunk = connection.recv(size)
        if not chunk:
            raise SystemExit("Daily authority identity response is truncated")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result

def read_fixed_keyring(path, expected_uid):
    path = os.path.abspath(path)
    if path != os.path.realpath(path) and os.path.islink(path):
        raise SystemExit("Daily authority trusted keyring must not be a symlink")
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise SystemExit("Daily authority trusted keyring is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        named = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != expected_uid
            or stat.S_IMODE(opened.st_mode) != 0o444
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or not 0 < opened.st_size <= 64 * 1024
        ):
            raise SystemExit("Daily authority trusted keyring is unsafe")
        chunks = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                raise SystemExit("Daily authority trusted keyring is truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if (
            len(b"".join(chunks)) != opened.st_size
            or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        ):
            raise SystemExit("Daily authority trusted keyring changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)

def root_owned_openssl():
    path = openssl_path
    try:
        observed = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise SystemExit("root-owned /usr/bin/openssl is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid not in ({0} if expected_keyring_uid == 0 else {0, expected_keyring_uid})
        or observed.st_nlink != 1
        or observed.st_mode & 0o022
    ):
        raise SystemExit("root-owned /usr/bin/openssl is unsafe")
    return path

def validate_public_key(public_key_pem, label):
    if not isinstance(public_key_pem, str) or not public_key_pem or len(public_key_pem) > 8192:
        raise SystemExit(f"{label} is invalid")
    try:
        result = subprocess.run(
            (
                root_owned_openssl(),
                "pkey",
                "-pubin",
                "-pubcheck",
                "-text_pub",
                "-noout",
            ),
            input=public_key_pem.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"{label} is not a valid Ed25519 key") from exc
    if result.returncode != 0 or b"ED25519" not in result.stdout.upper():
        raise SystemExit(f"{label} is not a valid Ed25519 key")

def verify_ed25519(public_key_pem, payload, signature):
    if len(signature) != 64:
        raise SystemExit("Daily authority identity signature length is invalid")
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-daily-identity-", dir="/tmp") as raw:
            root = os.path.abspath(raw)
            os.chmod(root, 0o700)
            public_path = os.path.join(root, "public.pem")
            payload_path = os.path.join(root, "payload.bin")
            signature_path = os.path.join(root, "signature.bin")
            for path, value in (
                (public_path, public_key_pem.encode("utf-8")),
                (payload_path, payload),
                (signature_path, signature),
            ):
                with open(path, "wb") as handle:
                    handle.write(value)
                os.chmod(path, 0o600)
            result = subprocess.run(
                (
                    root_owned_openssl(),
                    "pkeyutl",
                    "-verify",
                    "-rawin",
                    "-pubin",
                    "-inkey",
                    public_path,
                    "-in",
                    payload_path,
                    "-sigfile",
                    signature_path,
                ),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=5,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit("Daily authority identity signature verification failed") from exc
    if result.returncode != 0:
        raise SystemExit("Daily authority identity signature verification failed")

def load_trusted_keyring():
    payload = read_fixed_keyring(keyring_path, expected_keyring_uid)
    try:
        document = json.loads(payload, object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit("Daily authority trusted keyring is invalid JSON") from exc
    if payload != canonical(document, ensure_ascii=True):
        raise SystemExit("Daily authority trusted keyring is not canonical")
    expected = {
        "schema_version", "generation", "previous_manifest_hash", "active_key_id",
        "active_public_key", "previous_public_keys", "manifest_hash", "signature",
    }
    if not isinstance(document, dict) or set(document) != expected:
        raise SystemExit("Daily authority trusted keyring shape is invalid")
    active_key_id = document.get("active_key_id")
    active_public_key = document.get("active_public_key")
    generation = document.get("generation")
    previous_manifest_hash = document.get("previous_manifest_hash")
    previous_public_keys = document.get("previous_public_keys")
    manifest_hash = document.get("manifest_hash")
    if (
        document.get("schema_version") != 2
        or type(generation) is not int
        or generation < 1
        or not isinstance(previous_manifest_hash, str)
        or len(previous_manifest_hash) != 64
        or any(character not in "0123456789abcdef" for character in previous_manifest_hash)
        or not isinstance(active_key_id, str)
        or KEY_ID.fullmatch(active_key_id) is None
        or not isinstance(active_public_key, str)
        or not isinstance(previous_public_keys, dict)
        or not isinstance(manifest_hash, str)
        or len(manifest_hash) != 64
        or any(character not in "0123456789abcdef" for character in manifest_hash)
    ):
        raise SystemExit("Daily authority trusted keyring fields are invalid")
    validate_public_key(active_public_key, "Daily active public key")
    if generation == 1 and (previous_manifest_hash != "0" * 64 or previous_public_keys):
        raise SystemExit("Daily authority trusted keyring genesis binding is invalid")
    if generation > 1 and (previous_manifest_hash == "0" * 64 or not previous_public_keys):
        raise SystemExit("Daily authority trusted keyring rotation binding is invalid")
    for previous_key_id, previous_public_key in previous_public_keys.items():
        if (
            not isinstance(previous_key_id, str)
            or KEY_ID.fullmatch(previous_key_id) is None
            or previous_key_id == active_key_id
            or not isinstance(previous_public_key, str)
        ):
            raise SystemExit("Daily authority trusted previous key is invalid")
        validate_public_key(previous_public_key, "Daily previous public key")
    body = {
        key: document[key]
        for key in expected
        if key not in {"manifest_hash", "signature"}
    }
    expected_manifest_hash = hashlib.sha256(
        canonical(body, ensure_ascii=True)
    ).hexdigest()
    if manifest_hash != expected_manifest_hash:
        raise SystemExit("Daily authority trusted keyring manifest hash is invalid")
    try:
        manifest_signature = base64.b64decode(document["signature"], validate=True)
    except (TypeError, ValueError) as exc:
        raise SystemExit("Daily authority trusted keyring manifest signature is invalid") from exc
    verify_ed25519(active_public_key, manifest_hash.encode("ascii"), manifest_signature)
    return active_key_id, active_public_key

active_key_id, active_public_key = load_trusted_keyring()

nonce = secrets.token_hex(32)
request = {
    "version": 1,
    "operation": "identity",
    "protocol": "rquant-daily-receipt-authority.identity",
    "nonce": nonce,
}
payload = canonical(request)
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
    connection.settimeout(10)
    connection.connect(endpoint)
    connection.sendall(len(payload).to_bytes(4, "big") + payload)
    size = int.from_bytes(recv_exact(connection, 4), "big")
    if not 0 < size <= 2 * 1024 * 1024:
        raise SystemExit("Daily authority identity response size is invalid")
    response_payload = recv_exact(connection, size)

try:
    response = json.loads(
        response_payload,
        object_pairs_hook=unique_object,
    )
except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
    raise SystemExit("Daily authority identity response is invalid JSON") from exc
if response_payload != canonical(response):
    raise SystemExit("Daily authority identity response is not canonical")
if not isinstance(response, dict) or set(response) != {
    "version", "operation", "protocol", "nonce", "source_sha256", "key_id", "signature"
}:
    raise SystemExit("Daily authority identity response shape is invalid")
if (
    response.get("version") != 1
    or response.get("operation") != "identity"
    or response.get("protocol") != "rquant-daily-receipt-authority.identity"
    or response.get("nonce") != nonce
    or response.get("key_id") != active_key_id
):
    raise SystemExit("Daily authority identity response protocol binding is invalid")
source_sha = response.get("source_sha256")
if not isinstance(source_sha, str) or len(source_sha) != 64 or any(
    character not in "0123456789abcdef" for character in source_sha
):
    raise SystemExit("Daily authority identity source SHA is invalid")
if not isinstance(response.get("key_id"), str):
    raise SystemExit("Daily authority identity response key id is invalid")
try:
    signature = base64.b64decode(response["signature"], validate=True)
except (TypeError, ValueError) as exc:
    raise SystemExit("Daily authority identity response authentication is invalid") from exc
envelope = {
    "version": response["version"],
    "operation": response["operation"],
    "protocol": response["protocol"],
    "nonce": response["nonce"],
    "source_sha256": response["source_sha256"],
    "key_id": response["key_id"],
}
verify_ed25519(active_public_key, canonical(envelope), signature)
print(source_sha)
PY
}

validate_running_daily_authority_identity() {
    local expected_target="$1"
    local expected_sha="${expected_target##*/}"
    local actual
    actual="$(daily_authority_runtime_identity)"
    if [[ "${actual}" != "${expected_sha}" ]]; then
        printf 'Daily signer runtime identity mismatch: running=%s expected=%s\n' \
            "${actual}" "${expected_sha}" >&2
        return 1
    fi
}

validate_daily_authority_systemd_inputs() {
    validate_metadata "${DAILY_SOCKET_UNIT_TARGET}" 644 \
        "Daily receipt signer socket unit"
    validate_metadata "${DAILY_SERVICE_UNIT_TARGET}" 644 \
        "Daily receipt signer service unit"
    validate_daily_key_material
    validate_daily_public_keyring_metadata
    privileged "${VISUDO_BIN}" -cf "${SUDOERS_TARGET}"
}

verify_daily_authority_expected_state() {
    local expected_socket="$1"
    local expected_service="$2"
    if [[ "${expected_socket}" -eq 1 ]]; then
        systemctl_run is-active --quiet rquant-daily-receipt-signer.socket
    else
        if systemctl_run is-active --quiet rquant-daily-receipt-signer.socket; then
            printf '%s\n' 'Daily signer socket should be inactive' >&2
            return 1
        fi
    fi
    if [[ "${expected_service}" -eq 1 ]]; then
        systemctl_run is-active --quiet rquant-daily-receipt-signer.service
    else
        if systemctl_run is-active --quiet rquant-daily-receipt-signer.service; then
            printf '%s\n' 'Daily signer service should be inactive' >&2
            return 1
        fi
    fi
}

restore_daily_authority_enabled_state() {
    if [[ "${DAILY_AUTHORITY_SOCKET_WAS_ENABLED}" -eq 1 ]]; then
        systemctl_run enable rquant-daily-receipt-signer.socket
    else
        systemctl_run disable rquant-daily-receipt-signer.socket
    fi
    if [[ "${DAILY_AUTHORITY_SERVICE_WAS_ENABLED}" -eq 1 ]]; then
        systemctl_run enable rquant-daily-receipt-signer.service
    else
        systemctl_run disable rquant-daily-receipt-signer.service
    fi
}

rollback_daily_authority_switch() {
    local rollback_staging rollback_failed=0
    if [[ "${DAILY_AUTHORITY_SWITCHED}" -ne 1 ]]; then
        return
    fi

    if [[ "${SYSTEMD_LIFECYCLE_ENABLED}" -ne 1 ]]; then
        rollback_staging="${DAILY_AUTHORITY_ROOT}/.current-rollback.$$"
        if [[ "${DAILY_AUTHORITY_HAD_CURRENT}" -eq 1 ]]; then
            privileged /bin/ln -s "${DAILY_AUTHORITY_OLD_TARGET}" "${rollback_staging}"
            replace_daily_authority_pointer "${rollback_staging}" "${DAILY_AUTHORITY_CURRENT}"
            validate_daily_authority_current_target "${DAILY_AUTHORITY_OLD_TARGET}"
        else
            privileged /bin/rm -f "${DAILY_AUTHORITY_CURRENT}"
        fi
        DAILY_AUTHORITY_SWITCHED=0
        return
    fi

    # Keep the switched flag set until every rollback assertion has passed.
    if ! systemctl_run stop \
        rquant-daily-receipt-signer.service \
        rquant-daily-receipt-signer.socket; then
        rollback_failed=1
    fi

    rollback_staging="${DAILY_AUTHORITY_ROOT}/.current-rollback.$$"
    if [[ "${DAILY_AUTHORITY_HAD_CURRENT}" -eq 1 ]]; then
        if ! privileged /bin/ln -s "${DAILY_AUTHORITY_OLD_TARGET}" "${rollback_staging}" || \
            ! replace_daily_authority_pointer "${rollback_staging}" "${DAILY_AUTHORITY_CURRENT}"; then
            rollback_failed=1
        fi
    elif ! privileged /bin/rm -f "${DAILY_AUTHORITY_CURRENT}"; then
        rollback_failed=1
    fi

    if ! systemctl_run daemon-reload; then
        rollback_failed=1
    fi

    # Restart the old processes explicitly.  A plain start is insufficient when
    # a failed activation left the new process active under the old pointer.
    if [[ "${DAILY_AUTHORITY_SOCKET_WAS_ACTIVE}" -eq 1 ]]; then
        if ! systemctl_run restart rquant-daily-receipt-signer.socket; then
            rollback_failed=1
        fi
    elif ! systemctl_run stop rquant-daily-receipt-signer.socket; then
        rollback_failed=1
    fi
    if [[ "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}" -eq 1 ]]; then
        if ! systemctl_run restart rquant-daily-receipt-signer.service; then
            rollback_failed=1
        fi
    elif ! systemctl_run stop rquant-daily-receipt-signer.service; then
        rollback_failed=1
    fi

    if ! restore_daily_authority_enabled_state; then
        rollback_failed=1
    fi
    if [[ "${DAILY_AUTHORITY_HAD_CURRENT}" -eq 1 ]]; then
        if ! validate_daily_authority_current_target "${DAILY_AUTHORITY_OLD_TARGET}"; then
            rollback_failed=1
        fi
    elif privileged /bin/test -e "${DAILY_AUTHORITY_CURRENT}" || \
        privileged /bin/test -L "${DAILY_AUTHORITY_CURRENT}"; then
        printf '%s\n' 'Daily authority current pointer should be absent after rollback' >&2
        rollback_failed=1
    fi
    if ! verify_daily_authority_expected_state \
        "${DAILY_AUTHORITY_SOCKET_WAS_ACTIVE}" \
        "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}"; then
        rollback_failed=1
    fi
    if [[ "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}" -eq 1 &&
        "${DAILY_AUTHORITY_HAD_CURRENT}" -eq 1 ]]; then
        if ! validate_running_daily_authority_identity "${DAILY_AUTHORITY_OLD_TARGET}"; then
            rollback_failed=1
        fi
    fi
    if [[ "${rollback_failed}" -ne 0 ]]; then
        printf '%s\n' 'Daily authority rollback could not prove the old runtime state' >&2
        return 1
    fi
    DAILY_AUTHORITY_SWITCHED=0
}

publish_daily_authority_current() {
    local current_staging
    if path_exists "${DAILY_AUTHORITY_CURRENT}" || privileged /bin/test -L "${DAILY_AUTHORITY_CURRENT}"; then
        DAILY_AUTHORITY_HAD_CURRENT=1
        DAILY_AUTHORITY_OLD_TARGET="$(privileged /usr/bin/readlink "${DAILY_AUTHORITY_CURRENT}")"
    fi
    record_daily_authority_service_state
    # Arm rollback before stopping either unit; a partial stop must not leave
    # the previously active signer offline if the pointer switch is never reached.
    DAILY_AUTHORITY_SWITCHED=1
    if [[ "${SYSTEMD_LIFECYCLE_ENABLED}" -eq 1 ]]; then
        systemctl_run stop \
            rquant-daily-receipt-signer.service \
            rquant-daily-receipt-signer.socket
    fi
    current_staging="${DAILY_AUTHORITY_ROOT}/.current-${DAILY_AUTHORITY_RELEASE_SHA}.$$"
    if [[ -n "${TEST_ROOT}" ]]; then
        /bin/ln -s "releases/${DAILY_AUTHORITY_RELEASE_SHA}" "${current_staging}"
        replace_daily_authority_pointer "${current_staging}" "${DAILY_AUTHORITY_CURRENT}"
    else
        sudo /bin/ln -s "releases/${DAILY_AUTHORITY_RELEASE_SHA}" "${current_staging}"
        replace_daily_authority_pointer "${current_staging}" "${DAILY_AUTHORITY_CURRENT}"
    fi
    validate_daily_authority_current
    run_step daily_authority_switch_after /usr/bin/true
}

activate_daily_authority_candidate() {
    if [[ "${SYSTEMD_LIFECYCLE_ENABLED}" -ne 1 ]]; then
        return
    fi

    # The pointer is already on the candidate, but no signer process is allowed
    # to run until systemd, key material, sudoers, and the immutable release have
    # all been checked against that candidate.
    run_step systemd_daemon_reload systemctl_run daemon-reload
    run_step daily_authority_inputs_validate validate_daily_authority_systemd_inputs
    run_step daily_socket_enable systemctl_run enable --now \
        rquant-daily-receipt-signer.socket
    run_step daily_service_start systemctl_run start \
        rquant-daily-receipt-signer.service
    run_step daily_socket_health systemctl_run is-active --quiet \
        rquant-daily-receipt-signer.socket
    run_step daily_service_health systemctl_run is-active --quiet \
        rquant-daily-receipt-signer.service
    run_step daily_runtime_identity validate_running_daily_authority_identity \
        "releases/${DAILY_AUTHORITY_RELEASE_SHA}"

    # Preserve the pre-install active/inactive contract.  A previously active
    # signer stays active on the new immutable release; an inactive signer is
    # stopped again after the candidate has passed health and identity checks.
    if [[ "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}" -eq 0 ]]; then
        run_step daily_service_restore_inactive systemctl_run stop \
            rquant-daily-receipt-signer.service
    fi
    if [[ "${DAILY_AUTHORITY_SOCKET_WAS_ACTIVE}" -eq 0 ]]; then
        run_step daily_socket_restore_inactive systemctl_run stop \
            rquant-daily-receipt-signer.socket
    fi
    run_step daily_authority_state_validate verify_daily_authority_expected_state \
        "${DAILY_AUTHORITY_SOCKET_WAS_ACTIVE}" \
        "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}"
    if [[ "${DAILY_AUTHORITY_SERVICE_WAS_ACTIVE}" -eq 1 ]]; then
        run_step daily_authority_identity_validate validate_running_daily_authority_identity \
            "releases/${DAILY_AUTHORITY_RELEASE_SHA}"
    fi
}

install_public_keyring() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -m 0444 "${HIGHWATER_PUBLIC_EXPORT}" "${HIGHWATER_PUBLIC_STAGING}"
    else
        sudo /usr/bin/install \
            -o root \
            -g root \
            -m 0444 \
            "${HIGHWATER_PUBLIC_EXPORT}" \
            "${HIGHWATER_PUBLIC_STAGING}"
    fi
}

install_canvas_public_keyring() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -m 0444 "${CANVAS_PUBLIC_EXPORT}" "${CANVAS_PUBLIC_STAGING}"
    else
        sudo /usr/bin/install \
            -o root \
            -g root \
            -m 0444 \
            "${CANVAS_PUBLIC_EXPORT}" \
            "${CANVAS_PUBLIC_STAGING}"
    fi
}

install_shadow_public_keyring() {
    if [[ -n "${TEST_ROOT}" ]]; then
        /usr/bin/install -m 0444 "${SHADOW_PUBLIC_EXPORT}" "${SHADOW_PUBLIC_STAGING}"
    else
        sudo /usr/bin/install \
            -o root \
            -g root \
            -m 0444 \
            "${SHADOW_PUBLIC_EXPORT}" \
            "${SHADOW_PUBLIC_STAGING}"
    fi
}

validate_metadata() {
    local path="$1"
    local expected_mode="$2"
    local label="$3"
    local actual expected_owner
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${path}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${path}")"
        expected_owner="0:0"
    fi
    if [[ "${actual%:*}" != "${expected_owner}" || "${actual##*:}" != "${expected_mode}" ]]; then
        printf 'Unsafe %s: %s (expected owner %s and mode %s)\n' \
            "${label}" "${actual}" "${expected_owner}" "${expected_mode}" >&2
        exit 1
    fi
}

validate_highwater_key_material() {
    if [[ -n "${TEST_ROOT}" ]]; then
        printf '%s' '{"schema_version":1,"operation":"status","stable_identity":"installer-validation","nonce":"0000000000000000000000000000000000000000000000000000000000000000"}' | \
            "${HIGHWATER_HELPER_TARGET}" \
                --state-root "${HIGHWATER_STATE_DIR}" \
                --keys-file "${HIGHWATER_KEYS_FILE}" >/dev/null
    else
        sudo "${HIGHWATER_HELPER_TARGET}" --validate-key-material >/dev/null
    fi
}

validate_canvas_key_material() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "${CANVAS_HELPER_TARGET}" \
            --keys-file "${CANVAS_KEYS_FILE}" \
            --validate-key-material >/dev/null
    else
        sudo "${CANVAS_HELPER_TARGET}" --validate-key-material >/dev/null
    fi
}

validate_shadow_key_material() {
    if [[ -n "${TEST_ROOT}" ]]; then
        "${SHADOW_HELPER_TARGET}" \
            --keys-file "${SHADOW_KEYS_FILE}" \
            --validate-key-material >/dev/null
    else
        sudo "${SHADOW_HELPER_TARGET}" --validate-key-material >/dev/null
    fi
}

validate_daily_key_material() {
    local request='{"operation":"validate-key-material","schema_version":1}'
    if [[ -n "${TEST_ROOT}" ]]; then
        printf '%s' "${request}" | /usr/bin/python3 -c \
            'from pathlib import Path; import runpy, sys; module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; operation.__globals__["PUBLIC_KEYS_FILE"] = Path(sys.argv[3]); operation(Path(sys.argv[2]))' \
            "${DAILY_HELPER_TARGET}" \
            "${DAILY_KEYS_FILE}" \
            "${DAILY_PUBLIC_KEYS_FILE}" >/dev/null
    else
        printf '%s' "${request}" | sudo "${DAILY_HELPER_TARGET}" >/dev/null
    fi
}

export_highwater_public_keyring() {
    umask 077
    if [[ -n "${TEST_ROOT}" ]]; then
        local -a command=(
            "${HIGHWATER_HELPER_TARGET}"
            --keys-file "${HIGHWATER_KEYS_FILE}"
            --export-public-keyring
        )
        if path_exists "${HIGHWATER_PUBLIC_KEYS_FILE}"; then
            command+=(--current-keyring "${HIGHWATER_PUBLIC_KEYS_FILE}")
        fi
        "${command[@]}" >"${HIGHWATER_PUBLIC_EXPORT}"
    else
        sudo "${HIGHWATER_HELPER_TARGET}" --export-public-keyring >"${HIGHWATER_PUBLIC_EXPORT}"
    fi
    /bin/chmod 0444 "${HIGHWATER_PUBLIC_EXPORT}"
}

export_canvas_public_keyring() {
    umask 077
    if [[ -n "${TEST_ROOT}" ]]; then
        "${CANVAS_HELPER_TARGET}" \
            --keys-file "${CANVAS_KEYS_FILE}" \
            --export-public-keyring >"${CANVAS_PUBLIC_EXPORT}"
    else
        sudo "${CANVAS_HELPER_TARGET}" --export-public-keyring >"${CANVAS_PUBLIC_EXPORT}"
    fi
    /bin/chmod 0444 "${CANVAS_PUBLIC_EXPORT}"
}

export_shadow_public_keyring() {
    umask 077
    if [[ -n "${TEST_ROOT}" ]]; then
        "${SHADOW_HELPER_TARGET}" \
            --keys-file "${SHADOW_KEYS_FILE}" \
            --export-public-keyring >"${SHADOW_PUBLIC_EXPORT}"
    else
        sudo "${SHADOW_HELPER_TARGET}" --export-public-keyring >"${SHADOW_PUBLIC_EXPORT}"
    fi
    /bin/chmod 0444 "${SHADOW_PUBLIC_EXPORT}"
}

export_daily_public_keyring() {
    umask 077
    local request='{"operation":"export-public-keyring","schema_version":1}'
    if [[ -n "${TEST_ROOT}" ]]; then
        printf '%s' "${request}" | /usr/bin/python3 -c \
            'from pathlib import Path; import runpy, sys; module = runpy.run_path(sys.argv[1]); operation = module["_stdin_operation"]; operation.__globals__["PUBLIC_KEYS_FILE"] = Path(sys.argv[3]); operation(Path(sys.argv[2]))' \
            "${DAILY_HELPER_TARGET}" \
            "${DAILY_KEYS_FILE}" \
            "${DAILY_PUBLIC_KEYS_FILE}" >"${DAILY_PUBLIC_EXPORT}"
    else
        printf '%s' "${request}" | sudo "${DAILY_HELPER_TARGET}" >"${DAILY_PUBLIC_EXPORT}"
    fi
    /bin/chmod 0444 "${DAILY_PUBLIC_EXPORT}"
}

validate_highwater_public_keyring_metadata() {
    local actual expected_owner
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${HIGHWATER_PUBLIC_KEYS_FILE}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${HIGHWATER_PUBLIC_KEYS_FILE}")"
        expected_owner="0:0"
    fi
    if [[ "${actual%:*}" != "${expected_owner}" || "${actual##*:}" != "444" ]]; then
        printf 'Unsafe high-water public keyring: %s (expected owner %s and mode 444)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

validate_canvas_public_keyring_metadata() {
    local actual expected_owner
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${CANVAS_PUBLIC_KEYS_FILE}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${CANVAS_PUBLIC_KEYS_FILE}")"
        expected_owner="0:0"
    fi
    if [[ "${actual%:*}" != "${expected_owner}" || "${actual##*:}" != "444" ]]; then
        printf 'Unsafe Canvas public keyring: %s (expected owner %s and mode 444)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

validate_shadow_public_keyring_metadata() {
    local actual expected_owner
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${SHADOW_PUBLIC_KEYS_FILE}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${SHADOW_PUBLIC_KEYS_FILE}")"
        expected_owner="0:0"
    fi
    if [[ "${actual%:*}" != "${expected_owner}" || "${actual##*:}" != "444" ]]; then
        printf 'Unsafe Shadow public keyring: %s (expected owner %s and mode 444)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

validate_daily_public_keyring_metadata() {
    local actual expected_owner
    if [[ -n "${TEST_ROOT}" ]]; then
        actual="$(test_stat_metadata '%u:%g:%Lp' '%u:%g:%a' "${DAILY_PUBLIC_KEYS_FILE}")"
        expected_owner="$(/usr/bin/id -u):$(/usr/bin/id -g)"
    else
        actual="$(sudo /usr/bin/stat -c '%u:%g:%a' "${DAILY_PUBLIC_KEYS_FILE}")"
        expected_owner="0:0"
    fi
    if [[ "${actual%:*}" != "${expected_owner}" || "${actual##*:}" != "444" ]]; then
        printf 'Unsafe Daily public keyring: %s (expected owner %s and mode 444)\n' \
            "${actual}" "${expected_owner}" >&2
        exit 1
    fi
}

validate_daily_authority_ancestor_chain 1
run_step libexec_dir install_helper_directory "${HELPER_DIR}"
run_step systemd_dir install_systemd_directory "${SYSTEMD_TARGET_DIR}"
run_step highwater_state_dir install_private_directory "${HIGHWATER_STATE_DIR}"
run_step daily_state_dir install_private_directory "${DAILY_STATE_DIR}"
run_step shadow_recovery_state_dir install_shadow_recovery_state_directory
ensure_sudoers_directory
recover_stale_sudoers_backup
run_step helper_install install_file 0755 "${HELPER_SOURCE}" "${HELPER_STAGING}"
run_step helper_publish privileged /bin/mv -f "${HELPER_STAGING}" "${HELPER_TARGET}"
run_step highwater_helper_install install_file 0755 "${HIGHWATER_HELPER_SOURCE}" "${HIGHWATER_HELPER_STAGING}"
run_step highwater_helper_publish privileged /bin/mv -f \
    "${HIGHWATER_HELPER_STAGING}" "${HIGHWATER_HELPER_TARGET}"
run_step canvas_helper_install install_file 0755 "${CANVAS_HELPER_SOURCE}" "${CANVAS_HELPER_STAGING}"
run_step canvas_helper_publish privileged /bin/mv -f \
    "${CANVAS_HELPER_STAGING}" "${CANVAS_HELPER_TARGET}"
run_step shadow_helper_install install_file 0755 "${SHADOW_HELPER_SOURCE}" "${SHADOW_HELPER_STAGING}"
run_step shadow_helper_publish privileged /bin/mv -f \
    "${SHADOW_HELPER_STAGING}" "${SHADOW_HELPER_TARGET}"
run_step daily_helper_install install_file 0755 "${DAILY_HELPER_SOURCE}" "${DAILY_HELPER_STAGING}"
run_step daily_helper_publish privileged /bin/mv -f \
    "${DAILY_HELPER_STAGING}" "${DAILY_HELPER_TARGET}"
run_step daily_authority_release prepare_daily_authority_release
run_step daily_authority_build_after /usr/bin/true
validate_metadata "${HELPER_TARGET}" 755 "runtime credential helper"
validate_metadata "${HIGHWATER_HELPER_TARGET}" 755 "high-water helper"
validate_metadata "${CANVAS_HELPER_TARGET}" 755 "Canvas signer helper"
validate_metadata "${SHADOW_HELPER_TARGET}" 755 "Shadow signer helper"
validate_metadata "${DAILY_HELPER_TARGET}" 755 "Daily receipt signer helper"
run_step daily_socket_unit_install install_file 0644 \
    "${DAILY_SOCKET_UNIT_SOURCE}" "${DAILY_SOCKET_UNIT_STAGING}"
run_step daily_socket_unit_publish privileged /bin/mv -f \
    "${DAILY_SOCKET_UNIT_STAGING}" "${DAILY_SOCKET_UNIT_TARGET}"
run_step daily_service_unit_install install_file 0644 \
    "${DAILY_SERVICE_UNIT_SOURCE}" "${DAILY_SERVICE_UNIT_STAGING}"
run_step daily_service_unit_publish privileged /bin/mv -f \
    "${DAILY_SERVICE_UNIT_STAGING}" "${DAILY_SERVICE_UNIT_TARGET}"
validate_metadata "${DAILY_SOCKET_UNIT_TARGET}" 644 "Daily receipt signer socket unit"
validate_metadata "${DAILY_SERVICE_UNIT_TARGET}" 644 "Daily receipt signer service unit"
validate_metadata "${HIGHWATER_STATE_DIR}" 700 "high-water state directory"
validate_metadata "${DAILY_STATE_DIR}" 700 "Daily receipt signer state directory"
validate_metadata "${SHADOW_RECOVERY_STATE_DIR}" 700 "Shadow recovery state directory"
validate_metadata "${HIGHWATER_KEYS_FILE}" 600 "high-water private key manifest"
validate_metadata "${CANVAS_KEYS_FILE}" 600 "Canvas private key manifest"
validate_metadata "${SHADOW_KEYS_FILE}" 600 "Shadow private key manifest"
validate_metadata "${DAILY_KEYS_FILE}" 600 "Daily private key manifest"
run_step highwater_key_material validate_highwater_key_material
run_step highwater_public_export export_highwater_public_keyring
run_step highwater_public_install install_public_keyring
run_step highwater_public_publish privileged /bin/mv -f \
    "${HIGHWATER_PUBLIC_STAGING}" "${HIGHWATER_PUBLIC_KEYS_FILE}"
validate_highwater_public_keyring_metadata
run_step canvas_key_material validate_canvas_key_material
run_step canvas_public_export export_canvas_public_keyring
run_step canvas_public_install install_canvas_public_keyring
run_step canvas_public_publish privileged /bin/mv -f \
    "${CANVAS_PUBLIC_STAGING}" "${CANVAS_PUBLIC_KEYS_FILE}"
validate_canvas_public_keyring_metadata
run_step shadow_key_material validate_shadow_key_material
run_step shadow_public_export export_shadow_public_keyring
run_step shadow_public_install install_shadow_public_keyring
run_step shadow_public_publish privileged /bin/mv -f \
    "${SHADOW_PUBLIC_STAGING}" "${SHADOW_PUBLIC_KEYS_FILE}"
validate_shadow_public_keyring_metadata
run_step daily_key_material validate_daily_key_material
run_step daily_public_export export_daily_public_keyring
run_step daily_public_install install_file 0444 "${DAILY_PUBLIC_EXPORT}" "${DAILY_PUBLIC_STAGING}"
run_step daily_public_publish privileged /bin/mv -f \
    "${DAILY_PUBLIC_STAGING}" "${DAILY_PUBLIC_KEYS_FILE}"
validate_daily_public_keyring_metadata
run_step sudoers_install install_file 0440 "${SUDOERS_SOURCE}" "${SUDOERS_STAGING}"
run_step sudoers_validate_staging privileged "${VISUDO_BIN}" -cf "${SUDOERS_STAGING}"

if path_exists "${SUDOERS_TARGET}"; then
    privileged /bin/cp -p "${SUDOERS_TARGET}" "${SUDOERS_BACKUP}"
fi
run_step sudoers_publish privileged /bin/mv -f "${SUDOERS_STAGING}" "${SUDOERS_TARGET}"
if ! run_step sudoers_validate_final privileged "${VISUDO_BIN}" -cf "${SUDOERS_TARGET}"; then
    if path_exists "${SUDOERS_BACKUP}"; then
        if ! run_step sudoers_restore privileged /bin/mv -f \
            "${SUDOERS_BACKUP}" "${SUDOERS_TARGET}"; then
            printf 'Sudoers restore failed; preserved backup: %s\n' \
                "${SUDOERS_BACKUP}" >&2
            exit 1
        fi
        if ! privileged "${VISUDO_BIN}" -cf "${SUDOERS_TARGET}"; then
            printf 'Restored sudoers file failed validation: %s\n' \
                "${SUDOERS_TARGET}" >&2
            exit 1
        fi
    else
        privileged /bin/rm -f "${SUDOERS_TARGET}"
    fi
    exit 1
fi
privileged /bin/rm -f "${SUDOERS_BACKUP}"
run_step daily_authority_switch publish_daily_authority_current
activate_daily_authority_candidate
DAILY_AUTHORITY_SWITCHED=0

printf 'Runtime credential infrastructure installed and validated.\n'
