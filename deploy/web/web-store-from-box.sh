#!/usr/bin/env bash
# Box side: run deploy/web/web-store.sh <cmd> on the server over ssh (same host/key as
# deploy-from-box.sh). `backup <dir>` copies the sqlite file to the box first (read-only there).
set -euo pipefail
HOST="${RQUANT_WEB_HOST:-lighthouse@82.156.0.68}"
KEY="${RQUANT_WEB_SSH_KEY:-$HOME/.ssh/rquant_deploy}"
SSH=(ssh -i "$KEY" -o BatchMode=yes "$HOST")
here="$(cd "$(dirname "$0")" && pwd)"
cmd="${1:?install|create|status|remove|purge|backup <dir>}"
if [[ "$cmd" == backup ]]; then
  out="${2:?backup dir}"; mkdir -p "$out"
  "${SSH[@]}" 'f=~/rquant-web-store/web_store.sqlite3; [ -f $f ] && cat $f' > "$out/web_store.sqlite3" \
    || { echo "no web store file on the server"; rm -f "$out/web_store.sqlite3"; exit 0; }
  sha256sum "$out/web_store.sqlite3"; exit 0
fi
"${SSH[@]}" "bash -s $cmd" < "$here/web-store.sh"
