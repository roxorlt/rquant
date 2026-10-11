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
# resumable upload (the uplink can stall mid-transfer): 512 KiB parts, each written to a temp
# name and renamed only when complete, so a killed transfer never leaves a bad part behind;
# parts already on the server are skipped; then concatenate and check the sha256 there
local_tar="$tmp/rquant-web-$short.tar.gz"
remote_tar="/tmp/rquant-web-$short.tar.gz"
parts="/tmp/rquant-web-$short.parts"
chunk=524288
size=$(stat -c %s "$local_tar")
n=$(( (size + chunk - 1) / chunk ))
"${SSH[@]}" "mkdir -p $parts" </dev/null
for attempt in $(seq 1 "${RQUANT_UPLOAD_TRIES:-60}"); do
  done_list=$("${SSH[@]}" "ls $parts" </dev/null) || { sleep 10; continue; }
  missing=0
  for ((i = 0; i < n; i++)); do
    grep -qx "p$i" <<<"$done_list" && continue
    missing=1
    part_size=$(( i < n - 1 ? chunk : size - i * chunk ))
    tail -c +"$((i * chunk + 1))" "$local_tar" | head -c "$chunk" \
      | timeout "${RQUANT_CHUNK_TIMEOUT:-120}" "${SSH[@]}" \
          "t=$parts/t$i.\$\$; cat > \$t; if [ \$(stat -c %s \$t) = $part_size ]; then mv \$t $parts/p$i; else rm -f \$t; exit 1; fi" \
      || sleep 5
    echo "[$(date "+%F %T")] part $((i + 1))/$n (pass $attempt)"
  done
  (( missing == 0 )) && break
done
"${SSH[@]}" "cd $parts && cat \$(for ((i = 0; i < $n; i++)); do echo p\$i; done) > $remote_tar && rm -rf $parts" </dev/null
want_sum=$(cut -d' ' -f1 "$local_tar.sha256" 2>/dev/null || sha256sum "$local_tar" | cut -d' ' -f1)
got_sum=$("${SSH[@]}" "sha256sum $remote_tar | cut -d' ' -f1" </dev/null)
[[ "$got_sum" == "$want_sum" ]] || { "${SSH[@]}" "rm -f $remote_tar" </dev/null; echo "upload checksum mismatch" >&2; exit 1; }
"${SSH[@]}" "~/bin/rquant-web-deploy --tarball /tmp/rquant-web-$short.tar.gz; rc=\$?; rm -f /tmp/rquant-web-$short.tar.gz; exit \$rc"
