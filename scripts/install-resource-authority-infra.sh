#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

MODE="dry-run"
COMMAND="install"
RELEASE_SHA=""
CANDIDATE_ROOT=""
BOOTSTRAP_VERIFY_PATH=""
BOOTSTRAP_VERIFY_SHA256=""
BOOTSTRAP_VERIFY_UID=""
BOOTSTRAP_VERIFY_GID=""
BOOTSTRAP_TRUSTED_ROOT="/"
while [[ $# -gt 0 ]]; do
  case "$1" in
    "--apply")
      MODE="apply"
      shift
      ;;
    "--dry-run")
      MODE="dry-run"
      shift
      ;;
    --release-sha)
      RELEASE_SHA="${2:?missing --release-sha value}"
      shift 2
      ;;
    --prepare-payload)
      COMMAND="prepare-payload"
      CANDIDATE_ROOT="${2:?missing --prepare-payload value}"
      shift 2
      ;;
    --verify-bootstrap-only)
      COMMAND="verify-bootstrap"
      BOOTSTRAP_VERIFY_PATH="${2:?missing --verify-bootstrap-only value}"
      shift 2
      ;;
    --expected-bootstrap-sha256)
      BOOTSTRAP_VERIFY_SHA256="${2:?missing --expected-bootstrap-sha256 value}"
      shift 2
      ;;
    --expected-bootstrap-uid)
      BOOTSTRAP_VERIFY_UID="${2:?missing --expected-bootstrap-uid value}"
      shift 2
      ;;
    --expected-bootstrap-gid)
      BOOTSTRAP_VERIFY_GID="${2:?missing --expected-bootstrap-gid value}"
      shift 2
      ;;
    --bootstrap-trusted-root)
      BOOTSTRAP_TRUSTED_ROOT="${2:?missing --bootstrap-trusted-root value}"
      shift 2
      ;;
    *)
      echo "usage: $0 [--dry-run|--apply] --release-sha <full-40-char-sha>" >&2
      exit 2
      ;;
  esac
done
if [[ "$COMMAND" != "verify-bootstrap" && ! "$RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "resource authority runtime requires an exact 40-character release SHA" >&2
  exit 2
fi
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
ROOT_USER="rquant-external-root"
ROOT_CLIENT_GROUP="rquant-root-client"
RESOURCE_USER="rquant-resource-authority"
RESOURCE_CLIENT_GROUP="rquant-resource-client"
APPLICATION_USER="lighthouse"
RUNTIME_ROOT="/usr/local/libexec/rquant-authority-runtime"
RUNTIME_GENERATIONS="/usr/local/libexec/rquant-authority-runtime/generations"
RUNTIME_BUILDS="/usr/local/libexec/rquant-authority-runtime/builds"
RUNTIME_RELEASE="$RUNTIME_GENERATIONS/$RELEASE_SHA"
RUNTIME_CURRENT="$RUNTIME_ROOT/current"
RUNTIME_KEY_ROOT="/etc/rquant/keys/authority-runtime"
RUNTIME_SIGNING_PRIVATE="$RUNTIME_KEY_ROOT/runtime.private.pem"
RUNTIME_SIGNING_PUBLIC="$RUNTIME_KEY_ROOT/runtime.public.pem"
BOOTSTRAP_PUBLISHER="/usr/libexec/rquant-authority-runtime-publisher"
BOOTSTRAP_EXPECTED_SHA256="e1b96190f56544a31306a43f417e101c7aa2e0463da78dde2134656b119a19df"
BOOTSTRAP_VERSION="rquant-authority-runtime-publisher/v2"

run() {
  if [[ "$MODE" == "dry-run" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
    return
  fi
  "$@"
}

verify_bootstrap_path() {
  local path=$1 expected_sha256=$2 expected_uid=$3 expected_gid=$4 trusted_root=$5
  /usr/bin/python3 -I -S - \
    "$path" "$expected_sha256" "$expected_uid" "$expected_gid" "$trusted_root" <<'PY'
import hashlib
import os
import stat
import sys
from pathlib import PurePosixPath

path, expected_hash, uid_text, gid_text, trusted_root = sys.argv[1:]
expected_uid = int(uid_text)
expected_gid = int(gid_text)
if (
    not path.startswith("/")
    or not trusted_root.startswith("/")
    or os.path.normpath(path) != path
    or os.path.normpath(trusted_root) != trusted_root
    or len(expected_hash) != 64
    or any(character not in "0123456789abcdef" for character in expected_hash)
):
    raise SystemExit("authority runtime bootstrap path or expected hash is invalid")
try:
    relative = PurePosixPath(path).relative_to(PurePosixPath(trusted_root))
except ValueError as exc:
    raise SystemExit("authority runtime bootstrap escapes trusted root") from exc
parts = relative.parts
if not parts or any(part in {"", ".", ".."} for part in parts):
    raise SystemExit("authority runtime bootstrap path is unsafe")

directory_flags = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)


def require_directory(metadata: os.stat_result, label: str) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_gid != expected_gid
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise SystemExit(f"authority runtime bootstrap {label} owner or mode is unsafe")


opened = []
try:
    current = os.open(trusted_root, directory_flags)
    opened.append(current)
    require_directory(os.fstat(current), "trusted root")
    for component in parts[:-1]:
        named = os.stat(component, dir_fd=current, follow_symlinks=False)
        if stat.S_ISLNK(named.st_mode):
            raise SystemExit("authority runtime bootstrap ancestor is a symlink")
        child = os.open(component, directory_flags, dir_fd=current)
        opened.append(child)
        actual = os.fstat(child)
        require_directory(actual, "ancestor")
        if (named.st_dev, named.st_ino) != (actual.st_dev, actual.st_ino):
            raise SystemExit("authority runtime bootstrap ancestor identity changed")
        current = child
    name = parts[-1]
    named = os.stat(name, dir_fd=current, follow_symlinks=False)
    if stat.S_ISLNK(named.st_mode):
        raise SystemExit("authority runtime bootstrap is a symlink")
    descriptor = os.open(name, file_flags, dir_fd=current)
    opened.append(descriptor)
    actual = os.fstat(descriptor)
    if (
        not stat.S_ISREG(actual.st_mode)
        or actual.st_nlink != 1
        or actual.st_uid != expected_uid
        or actual.st_gid != expected_gid
        or stat.S_IMODE(actual.st_mode) != 0o555
        or (named.st_dev, named.st_ino) != (actual.st_dev, actual.st_ino)
    ):
        raise SystemExit("authority runtime bootstrap owner, mode, or type is unsafe")
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 64 * 1024):
        digest.update(chunk)
    rebound = os.stat(name, dir_fd=current, follow_symlinks=False)
    after = os.fstat(descriptor)
    if (
        (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or (after.st_dev, after.st_ino) != (rebound.st_dev, rebound.st_ino)
    ):
        raise SystemExit("authority runtime bootstrap identity changed while hashing")
    if digest.hexdigest() != expected_hash:
        raise SystemExit("authority runtime bootstrap hash mismatch")
finally:
    for descriptor in reversed(opened):
        try:
            os.close(descriptor)
        except OSError:
            pass
PY
}

print_bootstrap_install_command() {
  printf 'BOOTSTRAP_INSTALL_COMMAND='
  printf ' %q' /usr/bin/install -o root -g root -m 0555 \
    "$PROJECT_ROOT/scripts/publish-authority-runtime.py" "$BOOTSTRAP_PUBLISHER"
  printf '\n'
  printf 'BOOTSTRAP_EXPECTED_SHA256=%s\n' "$BOOTSTRAP_EXPECTED_SHA256"
}

verify_runtime_bootstrap() {
  if ! verify_bootstrap_path \
    "$BOOTSTRAP_PUBLISHER" "$BOOTSTRAP_EXPECTED_SHA256" 0 0 /; then
    echo "authority runtime bootstrap is absent or untrusted" >&2
    print_bootstrap_install_command >&2
    return 1
  fi
}

ensure_group() {
  local name=$1
  if ! getent group "$name" >/dev/null; then
    run groupadd --system "$name"
  fi
}

ensure_user() {
  local name=$1
  local primary_group=$2
  if ! getent passwd "$name" >/dev/null; then
    run useradd --system --gid "$primary_group" --home-dir /nonexistent \
      --shell /sbin/nologin "$name"
    return
  fi
  local expected_gid actual_gid
  expected_gid=$(getent group "$primary_group" | cut -d: -f3)
  actual_gid=$(id -g "$name")
  if [[ "$actual_gid" != "$expected_gid" ]]; then
    echo "$name exists with an unexpected primary group" >&2
    exit 1
  fi
}

install_env_if_absent() {
  local source=$1 destination=$2
  if [[ ! -e "$destination" ]]; then
    run install -m 0444 -o root -g root "$source" "$destination"
  else
    run chown root:root "$destination"
    run chmod 0444 "$destination"
  fi
}

verify_release_commit() {
  local observed
  observed=$(git -C "$PROJECT_ROOT" rev-parse --verify "$RELEASE_SHA^{commit}")
  if [[ "$observed" != "$RELEASE_SHA" ]]; then
    echo "release SHA does not resolve to the exact requested commit" >&2
    exit 1
  fi
}

prepare_runtime_candidate() {
  local candidate=$1 source payload venv archive uv_bin source_uid source_gid
  if [[ $(id -u) -eq 0 ]]; then
    echo "authority runtime payload preparation must not run as root" >&2
    exit 1
  fi
  if [[ -z "$candidate" || ! -d "$candidate" || -L "$candidate" ]]; then
    echo "authority runtime candidate directory is unavailable" >&2
    exit 1
  fi
  if [[ -n "$(find "$candidate" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "authority runtime candidate directory must be empty" >&2
    exit 1
  fi
  source="$candidate/source"
  payload="$candidate/payload"
  venv="$payload/venv"
  archive="$candidate/release.tar"
  install -d -m 0700 "$source" "$payload"
  git -C "$PROJECT_ROOT" archive --format=tar --output="$archive" "$RELEASE_SHA"
  tar -xf "$archive" -C "$source"
  rm -f "$archive"
  source_uid=$(id -u)
  source_gid=$(id -g)
  "$BOOTSTRAP_PUBLISHER" \
    --operation validate-build-input \
    --tree-root "$source" \
    --source-uid "$source_uid" \
    --source-gid "$source_gid"
  uv_bin=$(command -v uv || true)
  if [[ -z "$uv_bin" ]]; then
    echo "uv is required to build the immutable authority runtime" >&2
    exit 1
  fi
  python3 -m venv --copies "$venv"
  PYTHONDONTWRITEBYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT="$venv" \
    "$uv_bin" sync --project "$source" --python "$venv/bin/python" \
      --frozen --no-dev --no-editable
  rm -rf "$source"
  "$BOOTSTRAP_PUBLISHER" \
    --operation prepare-candidate \
    --candidate-root "$candidate" \
    --release-sha "$RELEASE_SHA" \
    --source-uid "$source_uid" \
    --source-gid "$source_gid" \
    --publisher-sha256 "$BOOTSTRAP_EXPECTED_SHA256" \
    --publisher-version "$BOOTSTRAP_VERSION" \
    --build-root-bytes "$candidate" \
    --runtime-release-bytes "$RUNTIME_RELEASE"
}

require_runtime_signing_key() {
  local private_metadata public_metadata
  if [[ ! -f "$RUNTIME_SIGNING_PRIVATE" || -L "$RUNTIME_SIGNING_PRIVATE" || \
        ! -f "$RUNTIME_SIGNING_PUBLIC" || -L "$RUNTIME_SIGNING_PUBLIC" ]]; then
    echo "authority runtime signing keypair must be provisioned before apply" >&2
    exit 1
  fi
  private_metadata=$(stat -c '%u:%g:%a:%h' "$RUNTIME_SIGNING_PRIVATE")
  public_metadata=$(stat -c '%u:%g:%a:%h' "$RUNTIME_SIGNING_PUBLIC")
  if [[ "$private_metadata" != "0:0:400:1" || "$public_metadata" != "0:0:444:1" ]]; then
    echo "authority runtime signing keypair ownership or mode is unsafe" >&2
    exit 1
  fi
}

build_runtime_release() {
  local build_root candidate application_uid application_gid
  if [[ -e "$RUNTIME_RELEASE" || -L "$RUNTIME_RELEASE" ]]; then
    echo "authority runtime generation already exists; refusing to replace immutable content" >&2
    exit 1
  fi
  application_uid=$(id -u "$APPLICATION_USER")
  application_gid=$(id -g "$APPLICATION_USER")
  build_root=$(mktemp -d "$RUNTIME_BUILDS/.build-$RELEASE_SHA.XXXXXX")
  candidate="$build_root/candidate"
  trap 'rm -rf -- "$build_root"' RETURN
  chmod 0711 "$build_root"
  install -d -m 0700 -o "$APPLICATION_USER" -g "$application_gid" "$candidate"
  runuser --user "$APPLICATION_USER" -- \
    /bin/bash "$PROJECT_ROOT/scripts/install-resource-authority-infra.sh" \
      --prepare-payload "$candidate" --release-sha "$RELEASE_SHA"
  "$BOOTSTRAP_PUBLISHER" \
    --operation publish \
    --candidate-root "$candidate" \
    --generations-root "$RUNTIME_GENERATIONS" \
    --release-sha "$RELEASE_SHA" \
    --signing-private-key "$RUNTIME_SIGNING_PRIVATE" \
    --source-uid "$application_uid" \
    --source-gid "$application_gid" \
    --publisher-sha256 "$BOOTSTRAP_EXPECTED_SHA256" \
    --publisher-version "$BOOTSTRAP_VERSION" \
    --published-uid 0 \
    --published-gid 0
  rm -rf -- "$build_root"
  trap - RETURN
}

select_runtime_release() {
  local temporary="$RUNTIME_ROOT/.current-$RELEASE_SHA.$$"
  ln -s "generations/$RELEASE_SHA" "$temporary"
  mv -Tf "$temporary" "$RUNTIME_CURRENT"
}

if [[ "$COMMAND" == "verify-bootstrap" ]]; then
  if [[ ! "$BOOTSTRAP_VERIFY_SHA256" =~ ^[0-9a-f]{64}$ || \
        ! "$BOOTSTRAP_VERIFY_UID" =~ ^[0-9]+$ || \
        ! "$BOOTSTRAP_VERIFY_GID" =~ ^[0-9]+$ ]]; then
    echo "bootstrap verification contract is incomplete" >&2
    exit 2
  fi
  verify_bootstrap_path \
    "$BOOTSTRAP_VERIFY_PATH" "$BOOTSTRAP_VERIFY_SHA256" \
    "$BOOTSTRAP_VERIFY_UID" "$BOOTSTRAP_VERIFY_GID" "$BOOTSTRAP_TRUSTED_ROOT"
  echo "authority runtime bootstrap verification passed"
  exit 0
fi

verify_release_commit
if [[ "$COMMAND" == "prepare-payload" ]]; then
  verify_runtime_bootstrap
  prepare_runtime_candidate "$CANDIDATE_ROOT"
  echo "resource authority nonprivileged payload preparation complete for $RELEASE_SHA"
  exit 0
fi
if [[ "$MODE" == "dry-run" ]]; then
  printf '+ nonprivileged payload build as %q for %q\n' "$APPLICATION_USER" "$RELEASE_SHA"
  printf '+ verify preinstalled bootstrap %q sha256=%q\n' \
    "$BOOTSTRAP_PUBLISHER" "$BOOTSTRAP_EXPECTED_SHA256"
  printf '+ descriptor-bound root publication via %q into %q\n' \
    "$BOOTSTRAP_PUBLISHER" "$RUNTIME_RELEASE"
  print_bootstrap_install_command
  echo "resource authority infrastructure dry-run complete for release $RELEASE_SHA"
  exit 0
fi
if [[ $(id -u) -ne 0 ]]; then
  echo "resource authority infrastructure installation requires root" >&2
  exit 2
fi
verify_runtime_bootstrap
ensure_group "$ROOT_CLIENT_GROUP"
ensure_group "$RESOURCE_CLIENT_GROUP"
ensure_user "$ROOT_USER" "$ROOT_CLIENT_GROUP"
ensure_user "$RESOURCE_USER" "$RESOURCE_CLIENT_GROUP"
if ! getent passwd "$APPLICATION_USER" >/dev/null; then
  echo "$APPLICATION_USER does not exist" >&2
  exit 1
fi
run usermod --append --groups "$ROOT_CLIENT_GROUP" "$RESOURCE_USER"
run usermod --append --groups "$RESOURCE_CLIENT_GROUP" "$APPLICATION_USER"

run install -d -m 0755 -o root -g root /etc/rquant /etc/rquant/keys
run install -d -m 0755 -o root -g root "$RUNTIME_KEY_ROOT"
run install -d -m 0755 -o root -g root \
  "$RUNTIME_ROOT" "$RUNTIME_GENERATIONS" "$RUNTIME_BUILDS"
run install -d -m 0750 -o "$ROOT_USER" -g "$ROOT_CLIENT_GROUP" \
  /etc/rquant/keys/external-root
run install -d -m 0750 -o "$RESOURCE_USER" -g "$RESOURCE_CLIENT_GROUP" \
  /etc/rquant/keys/resource-authority
run install -d -m 0700 -o "$ROOT_USER" -g "$ROOT_CLIENT_GROUP" \
  /var/lib/rquant-external-root
run install -d -m 0700 -o "$RESOURCE_USER" -g "$RESOURCE_CLIENT_GROUP" \
  /var/lib/rquant-resource-authority

install_env_if_absent \
  "$PROJECT_ROOT/deploy/env/external-root.env.example" \
  /etc/rquant/external-root.env
install_env_if_absent \
  "$PROJECT_ROOT/deploy/env/resource-authority.env.example" \
  /etc/rquant/resource-authority.env

for config in \
  /etc/rquant/external-monotonic-root.json \
  /etc/rquant/resource-authority.json; do
  if [[ -e "$config" ]]; then
    run chown root:root "$config"
    run chmod 0444 "$config"
  fi
done

if [[ -e /etc/rquant/keys/external-root/root.private.pem ]]; then
  run chown "$ROOT_USER":"$ROOT_CLIENT_GROUP" \
    /etc/rquant/keys/external-root/root.private.pem
  run chmod 0400 /etc/rquant/keys/external-root/root.private.pem
fi
if [[ -e /etc/rquant/keys/external-root/root.public.pem ]]; then
  run chown root:"$ROOT_CLIENT_GROUP" /etc/rquant/keys/external-root/root.public.pem
  run chmod 0440 /etc/rquant/keys/external-root/root.public.pem
fi
if [[ -e /etc/rquant/keys/resource-authority/operation.private.pem ]]; then
  run chown "$RESOURCE_USER":"$RESOURCE_CLIENT_GROUP" \
    /etc/rquant/keys/resource-authority/operation.private.pem
  run chmod 0400 /etc/rquant/keys/resource-authority/operation.private.pem
fi
if [[ -e /etc/rquant/keys/resource-authority/operation.public.pem ]]; then
  run chown "$RESOURCE_USER":"$RESOURCE_CLIENT_GROUP" \
    /etc/rquant/keys/resource-authority/operation.public.pem
  run chmod 0440 /etc/rquant/keys/resource-authority/operation.public.pem
fi

require_runtime_signing_key
build_runtime_release
select_runtime_release

echo "resource authority infrastructure $MODE complete for release $RELEASE_SHA"
