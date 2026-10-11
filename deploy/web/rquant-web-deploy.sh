#!/usr/bin/env bash
# Deploy one web release on the server (run as lighthouse; needs passwordless sudo for
# `systemctl restart rquant-web` only). See docs/deploy/web-release-pipeline.md.
#
#   rquant-web-deploy.sh latest            # newest GitHub release tagged web-*
#   rquant-web-deploy.sh 1a2b3c4d          # release web-1a2b3c4d
#   rquant-web-deploy.sh --tarball /tmp/rquant-web-1a2b3c4d.tar.gz
#   rquant-web-deploy.sh --rollback        # switch back to the previous release
#
# Steps: download (+sha256 verify) -> extract to ~/rquant-web-rel/<sha8> -> uv sync --frozen
# -> switch `current` atomically -> restart rquant-web -> health check -> on failure switch
# back to the previous target and restart again. Static files: ~/rquant-web/app is a
# symlink to current/web/dist, so it switches together with the API.
set -euo pipefail

REPO="${RQUANT_WEB_REPO:-roxorlt/rquant}"
REL_ROOT="${RQUANT_WEB_REL_ROOT:-$HOME/rquant-web-rel}"
STATIC_LINK="${RQUANT_WEB_STATIC_LINK:-$HOME/rquant-web/app}"
SERVICE="${RQUANT_WEB_SERVICE:-rquant-web}"
HEALTH_URL="${RQUANT_WEB_HEALTH_URL:-http://127.0.0.1:8768/api/v1/meta}"
UV="${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}"
PYTHON="${RQUANT_WEB_PYTHON:-python3.11}"

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

exec 9>"$REL_ROOT/.deploy.lock"
flock -n 9 || die "another deploy is running"

healthy() {
  local i code
  for i in $(seq 1 30); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$HEALTH_URL" || true)"
    if [[ "$code" == 200 ]] && systemctl is-active --quiet "$SERVICE"; then
      [[ -f "$STATIC_LINK/index.html" ]] && return 0
      log "static $STATIC_LINK/index.html missing"; return 1
    fi
    sleep 1
  done
  log "health check failed (last HTTP $code)"; return 1
}

switch_to() {  # atomic symlink swap of `current`
  ln -sfn "$1" "$REL_ROOT/.current.tmp"
  mv -T "$REL_ROOT/.current.tmp" "$REL_ROOT/current"
  sudo -n systemctl restart "$SERVICE"
}

ensure_static_link() {
  if [[ -L "$STATIC_LINK" ]]; then return 0; fi
  if [[ -e "$STATIC_LINK" ]]; then
    local keep="$STATIC_LINK.pre-pipeline-$(date +%Y%m%d%H%M%S)"
    log "one-time migration: $STATIC_LINK (directory) -> $keep, replaced by symlink"
    mv "$STATIC_LINK" "$keep"
  fi
  ln -sfn "$REL_ROOT/current/web/dist" "$STATIC_LINK"
}

PREV="$(readlink -f "$REL_ROOT/current" || true)"

if [[ "${1:-}" == "--rollback" ]]; then
  [[ -f "$REL_ROOT/.previous" ]] || die "no recorded previous release"
  target="$(cat "$REL_ROOT/.previous")"
  [[ -d "$target/.venv" ]] || die "previous release $target is not installed"
  log "rolling back $PREV -> $target"
  switch_to "$target"; ensure_static_link
  healthy || die "rollback target is unhealthy too; investigate"
  echo "$PREV" > "$REL_ROOT/.previous"
  log "rolled back to $target"; exit 0
fi

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
if [[ "${1:-}" == "--tarball" ]]; then
  tarball="${2:?tarball path}"
else
  want="${1:-latest}"
  if [[ "$want" == latest ]]; then
    tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases?per_page=30" \
      | "$PYTHON" -c 'import json,sys; print(next(r["tag_name"] for r in json.load(sys.stdin) if r["tag_name"].startswith("web-")))')"
  else
    tag="web-${want:0:8}"
  fi
  short="${tag#web-}"
  base="https://github.com/$REPO/releases/download/$tag"
  log "downloading $tag"
  curl -fsSL --retry 3 -o "$tmp/rquant-web-$short.tar.gz" "$base/rquant-web-$short.tar.gz"
  curl -fsSL --retry 3 -o "$tmp/rquant-web-$short.tar.gz.sha256" "$base/rquant-web-$short.tar.gz.sha256"
  (cd "$tmp" && sha256sum -c --quiet "rquant-web-$short.tar.gz.sha256") || die "sha256 mismatch"
  tarball="$tmp/rquant-web-$short.tar.gz"
fi

mkdir -p "$tmp/x"; tar -xzf "$tarball" -C "$tmp/x"
short="$("$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["short"])' "$tmp/x/RELEASE.json")"
[[ "$short" =~ ^[0-9a-f]{8}$ ]] || die "bad RELEASE.json"
[[ -f "$tmp/x/uv.lock" && -f "$tmp/x/web/dist/index.html" ]] || die "tarball incomplete"
DEST="$REL_ROOT/$short"

if [[ "$(readlink -f "$DEST")" == "$PREV" ]]; then
  log "$short is already current"; healthy && exit 0; die "current release is unhealthy"
fi
if [[ -e "$DEST" ]]; then
  mv "$DEST" "$DEST.replaced-$(date +%Y%m%d%H%M%S)"
fi
mv "$tmp/x" "$DEST"
chmod a+x "$DEST" "$DEST/web"   # nginx (www) must traverse to web/dist
chmod -R a+rX "$DEST/web/dist"

log "uv sync --frozen into $DEST/.venv"
(cd "$DEST" && UV_PROJECT_ENVIRONMENT="$DEST/.venv" "$UV" sync --frozen --no-dev \
  --python "$PYTHON" --no-progress -q) || die "uv sync failed; current untouched"
"$DEST/.venv/bin/python" -c 'import rquant.web.app' || die "import check failed; current untouched"

log "switching current: ${PREV:-<none>} -> $DEST"
switch_to "$DEST"
ensure_static_link
if healthy; then
  [[ -n "$PREV" ]] && echo "$PREV" > "$REL_ROOT/.previous"
  log "deployed $short"
  exit 0
fi

log "new release unhealthy; journal tail:"
sudo -n journalctl -u "$SERVICE" -n 30 --no-pager >&2 || true
[[ -n "$PREV" ]] || die "no previous release to roll back to"
log "auto-rollback -> $PREV"
switch_to "$PREV"
healthy && die "deploy of $short failed; rolled back to $PREV"
die "deploy of $short failed AND rollback unhealthy; investigate now"
