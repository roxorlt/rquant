#!/usr/bin/env bash
# Web store on the server: one small sqlite file for the web app's own saved settings
# (tables screener_scheme + ui_preference). Not the market / alert / watchlist / paper data.
# Run as lighthouse on the server (the box sends it: deploy/web/web-store-from-box.sh <cmd>).
#
#   install  mkdir ~/rquant-web-store (700) + systemd drop-in rquant-web.service.d/web-store.conf
#            (RQUANT_SCREENER_SCHEMES_DB + ReadWritePaths) + daemon-reload. No restart:
#            the next `rquant-web-deploy` restart picks it up.
#   create   create the sqlite file and both tables with the current release's code (idempotent)
#   status   read-only: drop-in, file, tables, row counts, sha256
#   remove   delete the drop-in + daemon-reload (rollback step 1, before rquant-web-deploy --rollback)
#   purge    delete ~/rquant-web-store (rollback step 3, after the rollback is healthy)
set -euo pipefail
ROOT="${HOME%/home/lighthouse}"            # "" on the server; the replica prefix on the box
STORE_DIR="$HOME/rquant-web-store"
DB="$STORE_DIR/web_store.sqlite3"
DROPIN="$ROOT/etc/systemd/system/rquant-web.service.d/web-store.conf"
PY="$HOME/rquant-web-rel/current/.venv/bin/python"
log() { printf '[%s] web-store %s\n' "$(date '+%F %T')" "$*" >&2; }
want_dropin() {
  printf '[Service]\n# web store (screener schemes + page settings); see deploy/web/web-store.sh\nEnvironment=RQUANT_SCREENER_SCHEMES_DB=%s\nReadWritePaths=-%s\n' "$DB" "$STORE_DIR"
}
case "${1:-status}" in
  install)
    umask 077; mkdir -p "$STORE_DIR"; chmod 700 "$STORE_DIR"
    if [[ -e "$DROPIN" ]] && ! cmp -s <(want_dropin) "$DROPIN"; then
      log "ERROR: $DROPIN exists with other content"; exit 1
    fi
    want_dropin | sudo -n tee "$DROPIN" >/dev/null; sudo -n chmod 644 "$DROPIN"
    sudo -n systemctl daemon-reload
    log "installed $DROPIN and $STORE_DIR" ;;
  create)
    umask 077
    RQUANT_SCREENER_SCHEMES_DB="$DB" "$PY" - "$DB" <<'PY'
import sqlite3, sys
from pathlib import Path
from rquant.web.screener_schemes import SCHEMA, SqliteSchemeStore
SqliteSchemeStore(Path(sys.argv[1]))
con = sqlite3.connect(sys.argv[1])
for name, sql in con.execute("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(f"table {name}: {sql}")
PY
    ls -l "$DB"; log "created $DB" ;;
  status)
    echo "dropin: $( [[ -f "$DROPIN" ]] && sha256sum "$DROPIN" | cut -c1-12 || echo absent )"
    echo "dir: $( [[ -d "$STORE_DIR" ]] && stat -c '%a %U' "$STORE_DIR" || echo absent )"
    if [[ -f "$DB" ]]; then
      ls -l "$DB"; sha256sum "$DB" | cut -c1-12
      "$PY" - "$DB" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(name, con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0], "rows")
PY
    else echo "db: absent"; fi ;;
  remove)
    if [[ -e "$DROPIN" ]]; then sudo -n rm -f "$DROPIN"; fi
    sudo -n systemctl daemon-reload; log "removed $DROPIN" ;;
  purge)
    if [[ -d "$STORE_DIR" ]]; then
      extra=$(find "$STORE_DIR" -mindepth 1 ! -name 'web_store.sqlite3*' | head -1)
      [[ -z "$extra" ]] || { log "ERROR: unexpected file $extra, not deleting"; exit 1; }
      rm -rf "$STORE_DIR"
    fi
    log "purged $STORE_DIR" ;;
  *) echo "usage: $0 install|create|status|remove|purge" >&2; exit 2 ;;
esac
