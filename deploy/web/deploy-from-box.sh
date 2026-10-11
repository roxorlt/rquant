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
# resumable upload: append 512 KiB chunks from the remote file's current size (the uplink can
# stall mid-transfer), then check the sha256 on the server before installing
local_tar="$tmp/rquant-web-$short.tar.gz"
remote_tar="/tmp/rquant-web-$short.tar.gz"
size=$(stat -c %s "$local_tar")
for attempt in $(seq 1 "${RQUANT_UPLOAD_TRIES:-400}"); do
  have=$("${SSH[@]}" "stat -c %s $remote_tar 2>/dev/null || echo 0" </dev/null) || { sleep 5; continue; }
  if (( have > size )); then "${SSH[@]}" "rm -f $remote_tar" </dev/null; continue; fi
  (( have == size )) && break
  tail -c +"$((have + 1))" "$local_tar" | head -c 524288 \
    | timeout "${RQUANT_CHUNK_TIMEOUT:-120}" "${SSH[@]}" "cat >> $remote_tar" || sleep 5
  echo "[$(date "+%F %T")] upload $have/$size (try $attempt)"
done
want_sum=$(cut -d' ' -f1 "$local_tar.sha256" 2>/dev/null || sha256sum "$local_tar" | cut -d' ' -f1)
got_sum=$("${SSH[@]}" "sha256sum $remote_tar | cut -d' ' -f1" </dev/null)
[[ "$got_sum" == "$want_sum" ]] || { "${SSH[@]}" "rm -f $remote_tar" </dev/null; echo "upload checksum mismatch" >&2; exit 1; }
"${SSH[@]}" "~/bin/rquant-web-deploy --tarball /tmp/rquant-web-$short.tar.gz; rc=\$?; rm -f /tmp/rquant-web-$short.tar.gz; exit \$rc"
