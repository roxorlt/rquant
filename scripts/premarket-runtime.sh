#!/usr/bin/env bash
# Pre-market runtime start + checks for the reference-slow chain (source -> publisher ->
# serving-authority -> serving). Never bypasses the 09:25 rule: it only ever (re)starts
# units before 09:20 and refuses to restart anything afterwards.
#
#   premarket-runtime.sh preflight   08:30  audit only, alert if anything is off
#   premarket-runtime.sh start       08:45  restart source+publisher, then serving
#   premarket-runtime.sh retry       09:00/09:10  restart again only if the chain is unhealthy
#   premarket-runtime.sh verify      09:27  alert if today's reference batch/serving-authority is missing
set -uo pipefail

MODE="${1:?usage: $0 preflight|start|retry|verify}"
ROOT=/home/lighthouse/rquant
RT="$ROOT/data/runtime"
ALERT="$ROOT/.venv/bin/rquant"
TODAY="$(date +%F)"
NOW="$(date +%H%M)"
problems=()

units() { systemctl list-units --all --plain --no-legend "$1" | awk '{print $1}'; }
SRC="$(units 'rquant-runtime-reference-slow-source@*.service')"
PUB="$(units 'rquant-runtime-reference-slow-publisher@*.service')"
SRV="$(units 'rquant-runtime-serving@*.service')"

alert() {
  local subject="$1" body="$2"
  echo "ALERT: $subject"; echo "$body"
  [ -n "${PREMARKET_DRY_RUN:-}" ] && return 0
  sudo -n -u lighthouse --preserve-env "$ALERT" alert --subject "$subject" --body "$body" \
    --dedup-key "premarket-runtime:$MODE:$TODAY" --cooldown-seconds 600 \
    || echo "alert send failed" >&2
}

is_trading_day() {
  local cal
  cal="$(ls -t "$RT"/authorities/market-calendar/generations/*.json 2>/dev/null | head -1)"
  [ -n "$cal" ] && grep -q "\"$TODAY\"" "$cal"
}

check_chain() {  # fills problems[]
  [ -n "$SRC" ] || problems+=("reference-slow source unit not loaded")
  [ -n "$PUB" ] || problems+=("reference-slow publisher unit not loaded")
  [ -n "$SRV" ] || problems+=("serving unit not loaded")
  for u in $SRC $PUB $SRV; do
    systemctl is-active -q "$u" || problems+=("$u not active ($(systemctl is-active "$u"))")
    local st; st="$(ps -o stat= -p "$(systemctl show -p MainPID --value "$u")" 2>/dev/null | tr -d ' ')"
    [[ "$st" == D* ]] && problems+=("$u main process in D state")
  done
  for u in $SRC; do
    local mh; mh="$(systemctl show -p MemoryHigh --value "$u")"
    [ "$mh" = infinity ] || [ "$mh" -ge $((1000*1024*1024)) ] || problems+=("$u MemoryHigh=$mh (<1000M; capture read gets throttled)")
  done
  local sh sc; sh="$(systemctl show -p MemoryHigh --value rquant-live-runtime.slice)"; sc="$(systemctl show -p MemoryCurrent --value rquant-live-runtime.slice)"
  [ "$sh" = infinity ] || [ $((sc * 100 / sh)) -lt 90 ] || problems+=("rquant-live-runtime.slice at $((sc/1048576))M of $((sh/1048576))M MemoryHigh")
  local avail; avail="$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)"
  [ "$avail" -ge 2048 ] || problems+=("host MemAvailable ${avail}M < 2048M")
  local free; free="$(df -Pm "$ROOT/data" | awk 'NR==2{print $4}')"
  [ "$free" -ge 15360 ] || problems+=("disk free ${free}M < 15G")
  timedatectl show -p NTPSynchronized --value | grep -q yes || problems+=("clock not NTP-synchronized")
  [ "$(date +%Z)" = CST ] || problems+=("timezone is $(date +%Z), expected CST")
  is_trading_day || problems+=("$TODAY not an open date in the newest market-calendar generation")
  local ro="$ROOT/data/rquant_ro.duckdb"
  [ -f "$ro" ] && [ $(( $(date +%s) - $(stat -c %Y "$ro") )) -lt 86400 ] || problems+=("read-only replica missing or >24h old")
  for d in "$RT/live/reference-slow" "$RT/live/reference-slow/serving-authority" "$RT/authorities/reference-slow" "$RT/serving"; do
    [ -d "$d" ] && [ "$(stat -c %U "$d")" = lighthouse ] || problems+=("$d missing or not owned by lighthouse")
  done
  for u in $SRC $PUB; do
    local inst="${u#*@}"; inst="${inst%.service}"
    [ -s "/etc/credstore.encrypted/rquant-runtime/instances/$inst/current.cred" ] || problems+=("credential missing for $inst")
  done
  local hb fails
  for hb in "$RT"/control/reference-slow-sources/*/heartbeats/*.json "$RT"/control/serving-publishers/*/heartbeats/*.json; do
    [ -f "$hb" ] || continue
    [ $(( $(date +%s) - $(stat -c %Y "$hb") )) -lt 300 ] || problems+=("stale heartbeat $(basename "$(dirname "$(dirname "$hb")")")")
  done
}

case "$MODE" in
  preflight)
    is_trading_day || { echo "not a trading day"; exit 0; }
    check_chain
    systemctl is-enabled -q rquant-premarket-runtime-restart.timer || problems+=("premarket restart timer disabled")
    ;;
  start|retry)
    is_trading_day || { echo "not a trading day"; exit 0; }
    if [ "$NOW" -ge 0918 ]; then
      alert "🚨 [RQ] premarket $MODE skipped at $(date +%T)" "Too close to the 09:20-09:25 capture window; not restarting anything."
      exit 1
    fi
    if [ "$MODE" = retry ]; then
      check_chain
      if [ ${#problems[@]} -eq 0 ]; then echo "chain healthy, no retry needed"; exit 0; fi
      echo "retry because: ${problems[*]}"; problems=()
    fi
    for u in $SRC $PUB; do systemctl reset-failed "$u" 2>/dev/null; done
    systemctl restart $SRC $PUB || problems+=("restart source/publisher failed")
    sleep 60
    for u in $SRV; do systemctl reset-failed "$u" 2>/dev/null; done
    systemctl restart $SRV || problems+=("restart serving failed")
    sleep 60
    check_chain
    ;;
  verify)
    is_trading_day || { echo "not a trading day"; exit 0; }
    cur="$RT/live/reference-slow/current/reference_slow.json"
    [ -f "$cur" ] && [ "$(date -d @"$(stat -c %Y "$cur")" +%F)" = "$TODAY" ] || problems+=("no reference-slow batch captured today (09:20-09:25)")
    sa="$RT/live/reference-slow/serving-authority/current.json"
    grep -q "\"published_at\":\"$(date -u +%F)" "$sa" 2>/dev/null || problems+=("serving-authority not published today")
    for hb in "$RT"/control/reference-slow-sources/*/heartbeats/*.json; do
      grep -q capture_failed "$hb" && problems+=("source heartbeat: $(grep -o 'capture_failed:[A-Za-z]*' "$hb")")
    done
    ;;
  *) echo "unknown mode $MODE"; exit 2 ;;
esac

if [ ${#problems[@]} -gt 0 ]; then
  body="$(printf -- '- %s\n' "${problems[@]}")"
  alert "🚨 [RQ] premarket $MODE: ${#problems[@]} problem(s)" "$body"
  exit 1
fi
echo "premarket $MODE OK"
