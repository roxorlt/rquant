#!/usr/bin/env bash
# ExecCondition helper: exit 0 (run) unless local time is inside [START, END) on a weekday,
# in which case exit 1 so systemd skips the run cleanly (no failure, no OnFailure alert).
#
#   outside-capture-window.sh 0910 0926   # rquant-backup: keep the 8-min gzip out of 09:20-09:25
#   outside-capture-window.sh 0918 0926   # rquant-replica-sync: never swap rquant_ro.duckdb
#                                         # under the reference-slow capture (45 s read deadline)
set -u
START="${1:?start HHMM}"; END="${2:?end HHMM}"
NOW="${RQ_NOW:-$(date +%H%M)}"; DOW="${RQ_DOW:-$(date +%u)}"  # overrides for tests
if [ "$DOW" -le 5 ] && [ "$((10#$NOW))" -ge "$((10#$START))" ] && [ "$((10#$NOW))" -lt "$((10#$END))" ]; then
  echo "skipped: $NOW is inside the pre-open capture window $START-$END"
  exit 1
fi
exit 0
