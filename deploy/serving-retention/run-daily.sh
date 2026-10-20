#!/bin/bash
# rQuant serving generations -> R2 (rquant-published-archive), daily 15:40 Asia/Shanghai via
# lighthouse's crontab (see crontab.txt). Order inside the script: incremental upload ->
# spot check (download back from R2, sha256 vs local) -> delete (>3 days old AND confirmed
# in R2 file by file). Spot check failure deletes nothing and exits 1.
# Credentials are read by the script from the 0600 file below; nothing secret is passed on
# the command line or in the environment. Never restarts any service.
set -u
export TZ=Asia/Shanghai
OPS=/home/lighthouse/rquant-ops/serving-retention
STATE=/home/lighthouse/rquant/data/runtime/serving-retention
LOG=/home/lighthouse/rquant/logs/serving-retention.log
mkdir -p "$STATE" "$(dirname "$LOG")"
echo "$(date '+%F %T %Z') run-daily start pid=$$" >> "$LOG"
timeout 3h nice -n 15 ionice -c3 /usr/bin/python3.11 -I "$OPS/serving_generations_retention.py" \
  --execute --archive \
  --credentials-file /etc/rquant-r2-published-archive.env \
  --bucket rquant-published-archive --prefix serving-generations \
  --keep-days 3 --keep-min 2 --min-age-minutes 30 --verify-sample 12 \
  --pin-file "$STATE/pins.txt" --state-dir "$STATE" --log-file "$LOG" "$@"
rc=$?
echo "$(date '+%F %T %Z') run-daily end rc=$rc" >> "$LOG"
exit $rc
