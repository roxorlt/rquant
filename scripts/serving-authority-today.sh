#!/usr/bin/env bash
# Gate for the 09:29 web switch: exit 0 (and print OK) only if
#   1. live/reference-slow/serving-authority/current.json was published TODAY (Asia/Shanghai), and
#   2. the serving publisher has picked that authority up (its heartbeat's
#      source_generations.reference_slow_authority == the authority's generation_id), and
#   3. that serving heartbeat is fresh (< 300 s) and the unit is active.
# The serving/runtime-health "degraded" status is deliberately ignored: runtime_health is
# permanently degraded by six never-deployed roles (artifact-catalog, artifact-retention,
# orchestrator, lab-jobs, promotions, shadow). Exit 1 = do not switch. Read-only.
set -uo pipefail
RT="${RQUANT_RUNTIME_ROOT:-/home/lighthouse/rquant/data/runtime}"
exec /usr/bin/python3 - "$RT" <<'PY'
import glob, json, subprocess, sys, time
from datetime import datetime, timezone, timedelta
rt = sys.argv[1]; cst = timezone(timedelta(hours=8))
today = datetime.now(cst).date()
def fail(msg): print(f"NO-SWITCH: {msg}"); sys.exit(1)
try:
    sa = json.load(open(f"{rt}/live/reference-slow/serving-authority/current.json"))
except Exception as e:
    fail(f"serving-authority unreadable ({type(e).__name__})")
pub = datetime.fromisoformat(sa["published_at"].replace("Z", "+00:00")).astimezone(cst)
if pub.date() != today:
    fail(f"serving-authority published_at {pub:%F %T} CST is not today")
hbs = glob.glob(f"{rt}/control/serving-publishers/*/heartbeats/*.json")
if len(hbs) != 1:
    fail(f"expected 1 serving heartbeat, found {len(hbs)}")
hb = json.load(open(hbs[0]))
age = time.time() - datetime.fromisoformat(hb["heartbeat_at"].replace("Z", "+00:00")).timestamp()
if age > 300:
    fail(f"serving heartbeat stale ({int(age)} s)")
seen = (hb.get("source_generations") or {}).get("reference_slow_authority")
if seen != sa["generation_id"]:
    fail(f"serving has not picked up today's authority yet (serving={str(seen)[:12]} authority={sa['generation_id'][:12]})")
units = subprocess.run(["systemctl", "list-units", "--state=active", "--plain", "--no-legend",
                        "rquant-runtime-serving@*.service"], capture_output=True, text=True).stdout.split()
if not units:
    fail("serving unit not active")
print(f"OK: serving-authority published {pub:%F %T} CST, generation {sa['generation_id'][:12]} served (serving status={hb.get('status')}, ignored)")
PY
