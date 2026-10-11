#!/usr/bin/env bash
# Box-side driver: the server cannot reach GitHub release downloads (times out from CN),
# so the box fetches the release with `gh`, verifies sha256, copies it over ssh and runs
# the server script with --tarball. Usage: deploy/web/deploy-from-box.sh [latest|<sha8>|--rollback]
set -euo pipefail
REPO="${RQUANT_WEB_REPO:-roxorlt/rquant}"
HOST="${RQUANT_WEB_HOST:-lighthouse@82.156.0.68}"
KEY="${RQUANT_WEB_SSH_KEY:-$HOME/.ssh/rquant_deploy}"
SSH=(ssh -i "$KEY" -o BatchMode=yes "$HOST")

want="${1:-latest}"
if [[ "$want" == --rollback ]]; then exec "${SSH[@]}" '~/bin/rquant-web-deploy --rollback'; fi
if [[ "$want" == latest ]]; then
  tag="$(gh release list -R "$REPO" -L 50 --json tagName,createdAt \
    -q '[.[] | select(.tagName | startswith("web-"))] | sort_by(.createdAt) | last | .tagName')"
else
  tag="web-${want:0:8}"
fi
[[ -n "$tag" ]] || { echo "no web release found" >&2; exit 1; }
short="${tag#web-}"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
gh release download "$tag" -R "$REPO" -D "$tmp" -p "rquant-web-$short.tar.gz*"
(cd "$tmp" && sha256sum -c --quiet "rquant-web-$short.tar.gz.sha256")
scp -q -i "$KEY" "$tmp/rquant-web-$short.tar.gz" "$HOST:/tmp/rquant-web-$short.tar.gz"
"${SSH[@]}" "~/bin/rquant-web-deploy --tarball /tmp/rquant-web-$short.tar.gz; rc=\$?; rm -f /tmp/rquant-web-$short.tar.gz; exit \$rc"
