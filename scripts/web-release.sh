#!/usr/bin/env bash
# Publish the React front end (web/dist) and the read-only web API from one exact tag.
#
# Runs on the cloud host 82.156.0.68 as lighthouse, next to (not through) the production
# deployer: until the deployer can publish the web, the web has its own tree
# (docs/deploy/web-app.md, plan v2 §2.1):
#
#   /home/lighthouse/rquant-web/
#     repo.git/             bare clone, fetched from the production checkout's origin
#     releases/<tag>/       one git worktree per tag, with its own .venv
#     current -> releases/<tag>            WorkingDirectory of rquant-web.service
#     app     -> releases/<tag>/web/dist   nginx's static root for /app/
#     previous -> releases/<tag>           what --rollback switches back to
#     releases.jsonl        one line per release / rollback
#
#   bash scripts/web-release.sh --target v0.34.0 --dry-run   # show the plan, change nothing
#   bash scripts/web-release.sh --target v0.34.0             # publish (idempotent)
#   bash scripts/web-release.sh --target v0.34.0 --no-restart  # before rquant-web.service exists
#   bash scripts/web-release.sh --rollback                   # back to the previous release
#   bash scripts/web-release.sh --status
#
# A release: fetch → the tag must be on main → worktree at the tag → uv sync --frozen
# --no-dev → `rquant web-serve --self-check` → nginx (user www) gets read access to web/dist
# only (ACL, chmod fallback) → switch `current` atomically → restart the API → wait for
# /api/v1/meta → switch `app` atomically. If the API does not come back, `current` goes
# back to the previous release and the API is restarted on it. Re-running a published
# target changes nothing. Restarting rquant-web is not restricted to market hours: it is a
# new read-only service, not a Route A or legacy resident unit (CLAUDE.md 「不按交易时段排期」).
#
# Test seams (tests/unit/test_web_release_script.py): RQUANT_WEB_HOME, RQUANT_WEB_REPO_URL,
# RQUANT_WEB_SERVING_ROOT, RQUANT_WEB_API_URL, RQUANT_WEB_NGINX_USER, RQUANT_WEB_UV,
# RQUANT_WEB_HEALTH_SECONDS; sudo, curl, systemctl, setfacl and getfacl come from PATH.
set -euo pipefail
# Everything this script creates is lighthouse-only; nginx gets exactly what grant_nginx
# gives it (never chmod after setfacl: chmod rewrites the ACL mask).
umask 0077

WEB_HOME="${RQUANT_WEB_HOME:-/home/lighthouse/rquant-web}"
PROD_CHECKOUT="${RQUANT_WEB_PROD_CHECKOUT:-/home/lighthouse/rquant}"
SERVING_ROOT="${RQUANT_WEB_SERVING_ROOT:-/home/lighthouse/rquant/data/runtime/serving}"
API_URL="${RQUANT_WEB_API_URL:-http://127.0.0.1:8768/api/v1/meta}"
NGINX_USER="${RQUANT_WEB_NGINX_USER:-www}"
UV_BIN="${RQUANT_WEB_UV:-uv}"
HEALTH_SECONDS="${RQUANT_WEB_HEALTH_SECONDS:-45}"
FETCH_SECONDS="${RQUANT_WEB_FETCH_SECONDS:-300}"
KEEP=3
UNIT="rquant-web.service"
# The sudoers drop-in (deploy/sudoers/rquant-web) allows exactly this command.
RESTART=(sudo -n /usr/bin/systemctl restart "${UNIT}")

REPO="${WEB_HOME}/repo.git"
RELEASES="${WEB_HOME}/releases"
LOG="${WEB_HOME}/releases.jsonl"
LOCK="${WEB_HOME}/.release.lock"

MODE=""
TARGET=""
DRY_RUN=0
RESTART_API=1

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed -e 's/^# \{0,1\}//' -e '/^set -euo/d' >&2
  exit 2
}

say() { printf 'web-release: %s\n' "$*"; }
fail() {
  printf 'web-release: FAILED: %s\n' "$*" >&2
  exit 1
}
plan() { printf 'web-release: [dry-run] %s\n' "$*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --target)
      [ $# -ge 2 ] || usage
      MODE="release"
      TARGET="$2"
      shift 2
      ;;
    --rollback) MODE="rollback"; shift ;;
    --status) MODE="status"; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --no-restart) RESTART_API=0; shift ;;
    -h | --help) usage ;;
    *) usage ;;
  esac
done
[ -n "${MODE}" ] || usage

if [ "$(id -u)" = "0" ]; then
  fail "run as lighthouse, not root (sudo is used for the one restart command only)"
fi

# ------------------------------------------------------------------ helpers

run_bounded() {
  local seconds="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "${seconds}" "$@"
  else
    "$@"
  fi
}

python_bin() {
  if command -v python3 >/dev/null 2>&1; then
    echo python3
  else
    echo /usr/bin/python3.11
  fi
}

# Replace a symlink in one rename(2), so nginx and systemd never see it missing.
switch_link() {
  local name="$1" target="$2"
  local next="${WEB_HOME}/.${name}.next"
  ln -sfn "${target}" "${next}"
  "$(python_bin)" -c 'import os, sys; os.replace(sys.argv[1], sys.argv[2])' \
    "${next}" "${WEB_HOME}/${name}"
}

link_release() {
  # Release name a link points at ("" when the link is missing).
  local link="${WEB_HOME}/$1"
  if [ -L "${link}" ]; then
    basename "$(readlink "${link}")"
  fi
}

json_line() {
  "$(python_bin)" -c '
import json, sys, datetime
keys = sys.argv[1::2]
values = sys.argv[2::2]
record = {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
record.update(zip(keys, values))
print(json.dumps(record, ensure_ascii=False, sort_keys=True))
' "$@" >>"${LOG}"
}

repo_url() {
  if [ -n "${RQUANT_WEB_REPO_URL:-}" ]; then
    echo "${RQUANT_WEB_REPO_URL}"
  else
    git -C "${PROD_CHECKOUT}" remote get-url origin
  fi
}

marker_commit() {
  local marker="$1/.rquant-web-release"
  if [ -f "${marker}" ]; then
    sed -n 's/^commit=//p' "${marker}"
  fi
}

api_healthy() {
  local body
  body="$(curl -fsS --max-time 3 "${API_URL}" 2>/dev/null)" || return 1
  printf '%s' "${body}" | "$(python_bin)" -c '
import json, sys
body = json.load(sys.stdin)
sys.exit(0 if isinstance(body.get("data"), dict) and "serving" in body else 1)
'
}

wait_for_api() {
  local waited=0
  while [ "${waited}" -lt "${HEALTH_SECONDS}" ]; do
    if api_healthy; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  api_healthy
}

restart_api() {
  if [ "${RESTART_API}" = "0" ]; then
    say "--no-restart: not restarting ${UNIT}; start it once it is installed"
    return 0
  fi
  "${RESTART[@]}"
  wait_for_api
}

# nginx (user www) may traverse the parents and read web/dist, nothing else.
grant_nginx() {
  local release="$1"
  local dirs=("${WEB_HOME}" "${RELEASES}" "${release}" "${release}/web")
  local parent
  parent="$(dirname "${WEB_HOME}")"
  if [ -O "${parent}" ]; then
    dirs=("${parent}" "${dirs[@]}")
  fi
  if setfacl -m "u:${NGINX_USER}:--x" "${dirs[@]}" 2>/dev/null &&
    setfacl -R -m "u:${NGINX_USER}:rX" "${release}/web/dist" 2>/dev/null; then
    ACCESS="acl"
  else
    say "setfacl is not available here; falling back to world-traversable dirs and a world-readable web/dist"
    chmod o+x "${dirs[@]}"
    chmod -R o+rX "${release}/web/dist"
    ACCESS="chmod"
  fi
  local index="${release}/web/dist/index.html"
  if [ "${ACCESS}" = "acl" ]; then
    local entry
    entry="$(getfacl -cp "${index}" 2>/dev/null | grep -E "^user:${NGINX_USER}:" || true)"
    # "user:www:r--" grants read unless the mask cuts it ("#effective:---").
    if ! [[ "${entry}" =~ ^user:${NGINX_USER}:r ]] || [[ "${entry}" == *"#effective:-"* ]]; then
      fail "ACL on ${index} does not grant ${NGINX_USER} read access (${entry:-no entry})"
    fi
  else
    [ -n "$(find "${index}" -maxdepth 0 -perm -o=r)" ] || fail "${index} is not world-readable"
  fi
}

prune() {
  local current previous name
  current="$(link_release current)"
  previous="$(link_release previous)"
  local kept=0
  # Newest first by the release marker's time.
  for marker in $(ls -1t "${RELEASES}"/*/.rquant-web-release 2>/dev/null); do
    name="$(basename "$(dirname "${marker}")")"
    kept=$((kept + 1))
    if [ "${kept}" -le "${KEEP}" ] || [ "${name}" = "${current}" ] || [ "${name}" = "${previous}" ]; then
      continue
    fi
    say "pruning old release ${name}"
    git -C "${REPO}" worktree remove --force "${RELEASES}/${name}" || rm -rf "${RELEASES:?}/${name}"
  done
  git -C "${REPO}" worktree prune
}

acquire_lock() {
  mkdir -p "${WEB_HOME}"
  if ! mkdir "${LOCK}" 2>/dev/null; then
    local holder
    holder="$(cat "${LOCK}/pid" 2>/dev/null || true)"
    if [ -n "${holder}" ] && kill -0 "${holder}" 2>/dev/null; then
      fail "another release is running (pid ${holder})"
    fi
    say "removing a stale lock left by pid ${holder:-unknown}"
    rm -rf "${LOCK}"
    mkdir "${LOCK}"
  fi
  echo "$$" >"${LOCK}/pid"
  trap 'rm -rf "${LOCK}"' EXIT
}

# ------------------------------------------------------------------ status

if [ "${MODE}" = "status" ]; then
  echo "current:  $(link_release current)"
  echo "app:      $(readlink "${WEB_HOME}/app" 2>/dev/null || true)"
  echo "previous: $(link_release previous)"
  ls -1 "${RELEASES}" 2>/dev/null | sed 's/^/release:  /'
  tail -n 5 "${LOG}" 2>/dev/null || true
  if api_healthy; then echo "api:      healthy (${API_URL})"; else echo "api:      not answering (${API_URL})"; fi
  exit 0
fi

# ------------------------------------------------------------------ rollback

if [ "${MODE}" = "rollback" ]; then
  previous="$(link_release previous)"
  current="$(link_release current)"
  [ -n "${previous}" ] || fail "no previous release to roll back to"
  [ -f "${RELEASES}/${previous}/.rquant-web-release" ] || fail "previous release ${previous} is incomplete"
  if [ "${DRY_RUN}" = "1" ]; then
    plan "switch current and app from ${current:-none} to ${previous}, restart ${UNIT}, wait for ${API_URL}"
    exit 0
  fi
  acquire_lock
  switch_link current "releases/${previous}"
  restart_api || fail "API did not answer on ${previous} either; see: journalctl -u ${UNIT} -n 50"
  switch_link app "releases/${previous}/web/dist"
  [ -n "${current}" ] && switch_link previous "releases/${current}"
  json_line action rollback target "${previous}" from "${current}" result ok
  say "rolled back to ${previous}"
  exit 0
fi

# ------------------------------------------------------------------ release

[[ "${TARGET}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "--target must be an exact tag like v0.34.0, got '${TARGET}'"
RELEASE="${RELEASES}/${TARGET}"
URL="$(repo_url)"

if [ "${DRY_RUN}" = "1" ]; then
  remote_tag="$(run_bounded "${FETCH_SECONDS}" git ls-remote --tags "${URL}" "refs/tags/${TARGET}" || true)"
  [ -n "${remote_tag}" ] || fail "tag ${TARGET} is not on ${URL}"
  plan "tag ${TARGET} exists on ${URL}"
  [ -d "${REPO}" ] || plan "create the bare clone ${REPO}"
  plan "fetch main and tags into ${REPO}; refuse ${TARGET} unless it is on main"
  if [ "$(link_release current)" = "${TARGET}" ] && [ -f "${RELEASE}/.rquant-web-release" ]; then
    plan "${TARGET} is already current: only re-check nginx access and the API"
    exit 0
  fi
  plan "worktree ${RELEASE}; ${UV_BIN} sync --frozen --python 3.11 --no-dev; rquant web-serve --self-check"
  plan "grant ${NGINX_USER} traverse on the parents and read on ${RELEASE}/web/dist (setfacl, else chmod)"
  was="$(link_release current)"
  plan "switch current -> releases/${TARGET} (now: ${was:-none})"
  if [ "${RESTART_API}" = "1" ]; then
    plan "${RESTART[*]}; wait up to ${HEALTH_SECONDS}s for ${API_URL}; on failure switch back"
  else
    plan "--no-restart: leave ${UNIT} alone"
  fi
  plan "switch app -> releases/${TARGET}/web/dist; append ${LOG}; keep the newest ${KEEP} releases"
  exit 0
fi

acquire_lock
mkdir -p "${RELEASES}"

if [ ! -d "${REPO}" ]; then
  say "cloning ${URL} into ${REPO}"
  run_bounded "${FETCH_SECONDS}" git clone --quiet --bare "${URL}" "${REPO}"
fi
say "fetching main and tags"
run_bounded "${FETCH_SECONDS}" git -C "${REPO}" fetch --quiet --prune --tags "${URL}" \
  "+refs/heads/main:refs/heads/main" || fail "git fetch did not finish in ${FETCH_SECONDS}s"
COMMIT="$(git -C "${REPO}" rev-parse --verify --quiet "refs/tags/${TARGET}^{commit}")" ||
  fail "tag ${TARGET} does not exist"
git -C "${REPO}" merge-base --is-ancestor "${COMMIT}" refs/heads/main ||
  fail "tag ${TARGET} (${COMMIT:0:12}) is not on main"

PREVIOUS="$(link_release current)"
if [ "${PREVIOUS}" = "${TARGET}" ] && [ -f "${RELEASE}/.rquant-web-release" ] &&
  [ "$(marker_commit "${RELEASE}")" != "${COMMIT}" ]; then
  fail "tag ${TARGET} now points at ${COMMIT:0:12}, not what is running; publish a new tag instead"
fi
if [ "${PREVIOUS}" = "${TARGET}" ] && [ "$(marker_commit "${RELEASE}")" = "${COMMIT}" ]; then
  say "${TARGET} is already current; re-checking nginx access and the API"
  grant_nginx "${RELEASE}"
  if [ "${RESTART_API}" = "1" ] && ! api_healthy; then
    say "the API is not answering; restarting it"
    restart_api || fail "API did not come back; see: journalctl -u ${UNIT} -n 50"
  fi
  say "nothing to do"
  exit 0
fi

if [ "$(marker_commit "${RELEASE}")" != "${COMMIT}" ]; then
  if [ -e "${RELEASE}" ]; then
    say "removing the incomplete release ${TARGET}"
    git -C "${REPO}" worktree remove --force "${RELEASE}" 2>/dev/null || rm -rf "${RELEASE:?}"
    git -C "${REPO}" worktree prune
  fi
  say "checking out ${TARGET} (${COMMIT:0:12})"
  git -C "${REPO}" worktree add --quiet --detach "${RELEASE}" "${COMMIT}"
  say "installing the Python environment"
  (cd "${RELEASE}" && "${UV_BIN}" sync --quiet --frozen --python 3.11 --no-dev)
  [ -s "${RELEASE}/web/dist/index.html" ] || fail "${TARGET} has no built web/dist/index.html"
  say "self-check against ${SERVING_ROOT}"
  env -i PATH="/usr/bin:/bin" HOME="${HOME}" RQUANT_DISABLE_DOTENV=1 \
    RQUANT_SERVING_ROOT="${SERVING_ROOT}" \
    "${RELEASE}/.venv/bin/rquant" web-serve --self-check ||
    fail "rquant web-serve --self-check failed for ${TARGET}"
  printf 'target=%s\ncommit=%s\n' "${TARGET}" "${COMMIT}" >"${RELEASE}/.rquant-web-release"
fi

ACCESS=""
grant_nginx "${RELEASE}"

say "switching current to ${TARGET}"
switch_link current "releases/${TARGET}"
if ! restart_api; then
  if [ -n "${PREVIOUS}" ] && [ -f "${RELEASES}/${PREVIOUS}/.rquant-web-release" ]; then
    say "the API did not answer on ${TARGET}; switching back to ${PREVIOUS}"
    switch_link current "releases/${PREVIOUS}"
    if restart_api; then
      json_line action release target "${TARGET}" commit "${COMMIT}" previous "${PREVIOUS}" \
        result rolled_back access "${ACCESS}"
      fail "${TARGET} did not start; rolled back to ${PREVIOUS}. See: journalctl -u ${UNIT} -n 50"
    fi
    json_line action release target "${TARGET}" commit "${COMMIT}" previous "${PREVIOUS}" \
      result rollback_failed access "${ACCESS}"
    fail "${TARGET} did not start and ${PREVIOUS} did not come back either; see: journalctl -u ${UNIT} -n 50"
  fi
  json_line action release target "${TARGET}" commit "${COMMIT}" previous "" \
    result failed access "${ACCESS}"
  fail "${TARGET} did not start and there is no previous release; stop it with: sudo systemctl stop ${UNIT}"
fi

switch_link app "releases/${TARGET}/web/dist"
[ -r "${WEB_HOME}/app/index.html" ] || fail "${WEB_HOME}/app/index.html is not readable after the switch"
if [ -n "${PREVIOUS}" ]; then
  switch_link previous "releases/${PREVIOUS}"
fi
json_line action release target "${TARGET}" commit "${COMMIT}" previous "${PREVIOUS}" \
  result ok access "${ACCESS}"
prune
say "published ${TARGET} (${COMMIT:0:12}); previous: ${PREVIOUS:-none}; nginx access: ${ACCESS}"
