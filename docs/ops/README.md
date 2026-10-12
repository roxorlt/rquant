# rQuant ops runbook

Single source of truth for what runs where. **Names only — no secret values in this repo.**
Times are Asia/Shanghai (server TZ = CST).

## Servers

| Host | Role | Access |
|---|---|---|
| `lighthouse@82.156.0.68` (Tencent Lighthouse) | prod: web API, runtime, monitor, research, snapshot collector, R2 archive | SSH key (`rquant_deploy`) |
| roxor's Mac | dev, launchd jobs (`deploy/launchd/`), local `.env` | — |

**SSH from the box / executors: always pass the key** — `ssh -i ~/.ssh/rquant_deploy
lighthouse@82.156.0.68` (a bare `ssh lighthouse@82.156.0.68` fails with
`Permission denied (publickey)` because the default identities are not authorised). The box's
`~/.ssh/config` also has a `Host rquant 82.156.0.68` entry (`User lighthouse`,
`IdentityFile ~/.ssh/rquant_deploy`, `IdentitiesOnly yes`), so `ssh rquant` works too. Check
scripts must use the key explicitly (`deploy/web/deploy-from-box.sh` honours
`RQUANT_WEB_SSH_KEY`, default `~/.ssh/rquant_deploy`). To recreate the entry:

```
Host rquant 82.156.0.68
    HostName 82.156.0.68
    User lighthouse
    IdentityFile ~/.ssh/rquant_deploy
    IdentitiesOnly yes
```

## Code releases (web/research code on the server)

- Merge to `main` → **Web MVP** CI (`.github/workflows/web-mvp.yml`) → **Web release** builds
  GitHub release `web-<sha8>` (tarball + `.sha256`).
- Deploy on server: `~/bin/rquant-web-deploy <sha8>` (or `latest`). If GitHub download from
  the server fails, `gh release download web-<sha8>` elsewhere, copy to `/tmp`, then
  `~/bin/rquant-web-deploy --tarball /tmp/rquant-web-<sha8>.tar.gz`.
- Releases live in `~/rquant-web-rel/<sha8>`; `~/rquant-web-rel/current` symlink is what every
  unit's `WorkingDirectory`/`ExecStart` uses. Rollback: `~/bin/rquant-web-deploy --rollback`.
- systemd units are **not** installed by the release; copy changed units manually:
  `sudo install -m 644 deploy/systemd/<unit> /etc/systemd/system/ && sudo systemctl daemon-reload`
  (or `scripts/deploy.sh` for the full sync).

## systemd units & timers (server)

| Unit | Schedule | What | Source in repo |
|---|---|---|---|
| `rquant-web.service` (+ drop-in `secrets.conf`) | always on, :8768 | Web MVP API | `deploy/web/` |
| `rquant-web-commands.service` | always on | web command worker | `deploy/systemd/` |
| `rquant-runtime-*@.service`, `rquant-runtime-daily-orchestrator@.timer`, `rquant-runtime-recovery-rehearsal@.timer` (hourly) | per role | live runtime (`/var/lib/rquant/runtime-authority`) | `deploy/systemd/`, `deploy/root-runtime/` |
| `rquant-monitor.timer` / `-watchdog` / `rquant-surge-watch.timer` | Mon-Fri 09:25 | intraday monitor | `deploy/systemd/` |
| `rquant-pre-market-check.timer`, `rquant-backup.timer` | Mon-Fri 09:00 (+17:30 backup) | checks / DB backup | `deploy/systemd/` |
| `rquant-morning-pulse.timer`, `rquant-midday-report.timer`, `rquant-daily-report.timer` | 10:00…/12:00/15:30 | reports | `deploy/systemd/` |
| `rquant-kpl-snapshot.timer` | Mon-Fri 16:35 | KPL concept members | `deploy/systemd/` |
| `rquant-daily.timer` | Mon-Fri 17:00 | `rquant run-daily` | `deploy/systemd/` |
| `rquant-replica-sync.timer` | several/day | read-only replica copy | `deploy/systemd/` |
| `rquant-research-ingest.timer` | Mon-Fri 18:10 | research minute/auction partitions | `deploy/systemd/` |
| `rquant-research.timer` → `rquant-research.service` | Mon-Fri 18:10 | `python -m rquant.research daily` | `deploy/research/` |
| ↳ drop-in `rquant-research.service.d/minutes-all.conf` | after research | **minute backfill**: `MB_DAILY=1 python ~/rquant-research/minute_backfill.py` (all-A 5-min bars, staged parquet + merge) | `deploy/research/rquant-research.service.d/minutes-all.conf`, `deploy/research/minute_backfill.py` |
| `rquant-snapshot-collector.timer` | Mon-Fri **09:10** | L1 ~3s snapshots → `snapshots/YYYY-MM-DD/`, ticks after close, compacts ~15:45 | `deploy/systemd/` |
| `rquant-report-factors.timer` | Tue-Fri **06:30** | 研报挖掘 recompute: `python -m rquant.research.report_factors run` → `report_factors/{results,latest}.json` (DuckDB read-only ~1 min; validated factors appear in 选股器 as `研报·…`) | `deploy/systemd/` |
| `rquant-report-tracking.timer` | Sat **06:30** | 研报挖掘 weekly: fxbaogao search for new 金工 factor reports → `report_factors/catalog_extra.json`, re-replicate, diff vs last week → **`~/rquant-research/report_factors/weekly_summary.json`** (+ `weekly/YYYY-MM-DD.json`); API `GET /api/v1/report-factors/summary` (`headline` for the daily report) | `deploy/systemd/` |
| `rquant-surge-prior.timer` | Mon-Fri **09:02** | builds today's prior-day cumulative-volume curve → `snapshots/YYYY-MM-DD/surge/prior.npz` (oneshot, ~6s) | `deploy/systemd/` |
| ↳ `rquant-snapshot-collector.service.d/surge-push.conf` | — | sets `SURGE_PUSH_ENV_FILE` (PushDeer key names read from `~/rquant/.env`) for the in-collector surge scanner | `deploy/systemd/rquant-snapshot-collector.service.d/` |
| `rquant-auction-pool-candidates.timer` / `rquant-auction-pool-pick.timer` / `rquant-auction-pool-attrib.timer` | Mon-Fri **08:40** / **09:26:30** / **09:27** | 竞价选股 pool: T-1 candidates (limit-up non-一字, 封单/流通市值 ≤0.94%, non-ST/BJ; plus `base` = same without the seal filter), then rt_k auction picks at 09:26:30 after the auction settles (gap ≥5.37%, auction not already at limit-up, top 3 by auction vol) and `ths_screen` (raw 同花顺 screen: gap >4%, top 5 by vol, 一字 flagged) → `pools/auction/YYYY-MM-DD.json`; 09:27 attribution (`rquant.research.auction_attrib`: Tushare ths_member/ths_index concepts + major_news, Eastmoney news/announcements, DeepSeek via `/etc/rquant-web.secrets.env` or `~/rquant/.env`) stored as `attribution`, shown on the 池子 page and pushed once via PushDeer (`pushed_at`); collector universe (top priority, reloaded 09:26:45), surge push ★ first | `deploy/systemd/`, `rquant.research.auction_pool`, `rquant.research.auction_attrib` |
| `rquant-snapshot-archive.timer` | Mon-Fri **16:20** | tar.gz finished day dirs → R2, verify, prune >14d (verified only) | `deploy/systemd/` |
| `rquant-artifact-retention.timer` | every 5 min | artifact tiering/GC | `deploy/systemd/` |

Also on the server (user crontab, not systemd): `~/itick_probe` collect 09:10 / stop 15:12 / report 15:20.

> **Monday 08:50 restore**: not found as a server systemd timer or Mac launchd/cron entry when
> this doc was written — fill in where it runs.

Status: `systemctl list-timers --all | grep rquant`; logs: `journalctl -u <unit> -n 100`.
Start/stop: `sudo systemctl start|stop <unit>`; disable: `sudo systemctl disable --now <timer>`.
Snapshot collector kill switch for today: `touch ~/rquant-research/snapshots/STOP`.

## Production Serving publication (reference-slow → serving → web)

Chain: `rquant-runtime-reference-slow-publisher@svc-62c9…` writes
`data/runtime/live/reference-slow/serving-authority/current.json` →
`rquant-runtime-serving@svc-63af…` publishes `data/runtime/serving/generations` → `rquant-web`
(`/etc/rquant-web.env`, `RQUANT_SERVING_ROOT`).

- **2026-10-12 root cause of "serving empty since 09-05"**: the publisher unit mounted all of
  `live/reference-slow` read-only (`ReadOnlyPaths=`) and never granted the nested
  `serving-authority` dir, so every round failed with `OSError: [Errno 30] Read-only file system:
  'serving-authority'` and serving reported `current authority is unavailable`. Fixed in
  `deploy/systemd/rquant-runtime-reference-slow-publisher@.service` (nested `ReadWritePaths=`,
  no `-` prefix: a missing dir fails the start loudly). Test:
  `tests/unit/test_runtime_systemd_schema_rollout_paths.py`.
- **Applying it** (unit files are not shipped by `rquant-production-deploy.pyz`, which also
  refuses restarts 09:15–15:10 on weekdays): outside that window,
  `install -d -m 0700 ~/rquant/data/runtime/live/reference-slow/serving-authority`, then
  `sudo install -m 644 deploy/systemd/rquant-runtime-reference-slow-publisher@.service /etc/systemd/system/ && sudo systemctl daemon-reload`.
- The publisher only works if started **before 09:25** on a trading day ("started after 09:25"
  otherwise, all day). Restart only that instance between 08:45 and 09:15 (backup first:
  `tar czf ~/backup-refslow-$(date +%F).tgz -C ~/rquant/data/runtime control/reference-slow-publishers control/serving-publishers live/reference-slow`).
  Do not touch the snapshot collector, surge scanner, auction pool or research timers.
- Verify: publisher heartbeat `last_success_at` set; `serving-authority/current.json` exists;
  serving heartbeat `last_success_at` set and `serving/generations` non-empty;
  `curl 127.0.0.1:8768/api/v1/meta` ready (only after switching the web, below).
- Switch web to production: back up `/etc/rquant-web.env`, set
  `RQUANT_SERVING_ROOT=/home/lighthouse/rquant/data/runtime/serving`, drop `RQUANT_WEB_NOTICE`,
  `sudo systemctl restart rquant-web`. Rollback: restore the backup env (replay path) and restart.

## Surge scanner (量能异动) + auction factors

Runs **inside the snapshot collector** (`rquant.research.surge`), fed every ~3s cycle with the raw
full-A rt_k/rt_tick frame; any error is logged and after 20 errors the scanner disables itself —
it never stops collection.

- Factors: `ratio` = today cum vol / prior trading day cum vol at the same trading minute;
  `acc1`/`acc5` = last ~1/~5-min volume / prior day same interval. Prior curve: prior day's
  snapshot dir if it covers ≥3000 codes, else 5-min `minute_bar` (market.duckdb) interpolated.
- Events (from 09:35): `ratio` ≥ 3 and cum amount ≥ 3000万; `accel`: acc5 ≥ 5, 5-min amount ≥
  1000万, ratio ≥ 1.5. Skips ST names and limit-locked (at limit, empty opposite side). Cooldown
  15 min per stock per kind. Env overrides: `SURGE_RATIO_MIN`, `SURGE_AMOUNT_MIN`, `SURGE_ACC5_MIN`,
  `SURGE_WIN_AMOUNT_MIN`, `SURGE_COOLDOWN_S`.
- Output `snapshots/YYYY-MM-DD/surge/`: `events.jsonl` (live), `factors-HHMM.parquet` → merged
  `factors.parquet` + `events.parquet` at 15:00; `prior.npz`.
- Web: 盯盘与告警 page → 「量能异动」 panel (`GET /api/v1/surge[?day=]`).
- Push: PushDeer (admin keys `PUSHDEER_KEYS`), ≤1 message/min, top 10 by ratio.
  Mute today: `touch ~/rquant-research/snapshots/SURGE_NOPUSH`; disable push: `SURGE_PUSH=0`;
  disable scanner: `SURGE_SCAN=0` (drop-in env).
- Auction factors: after the collector compacts (~15:45) it writes `snapshots/YYYY-MM-DD/auction.parquet`
  (`rquant.research.auction`; columns documented in the module). Manual:
  `.venv/bin/python -m rquant.research.auction ~/rquant-research/snapshots/2026-10-12`.

## Data dirs (server)

| Path | Contents |
|---|---|
| `~/rquant-research/` (`RQUANT_RESEARCH_ROOT`) | research root: `market.duckdb`, `intraday.duckdb`, `factor/`, `condition/`, `strategy/`, … |
| `~/rquant-research/snapshots/YYYY-MM-DD/` | L1 snapshots (`quotes.parquet`, ticks); `.r2_archived.json` once in R2 |
| `~/rquant-research/minute_backfill/` | minute backfill `staging/`, `done/`, `done-daily/`, `backfill.log` |
| `/var/lib/rquant/` | runtime-authority, receipt signer, lab-highwater, shadow-recovery |
| `~/rquant-web-rel/` | code releases (`current` symlink) |

## R2 (Cloudflare) — bucket `rquant-snapshots`

Layout: `snapshots/YYYY-MM-DD/YYYY-MM-DD.tar.gz` (object metadata `x-amz-meta-sha256`);
`snapshots/_selftest/` is transient. Key is scoped to Object Read & Write on this bucket only
(Cloudflare account token `rquant-snapshots-rw`).

Fetch a day back (on the server):

```bash
cd ~/rquant-web-rel/current && set -a && . /etc/rquant-r2.env && set +a
.venv/bin/python -m rquant.research.snapshot_archive fetch 2026-10-12 --dest /tmp/restore
# -> /tmp/restore/2026-10-12/quotes.parquet ...   (sha256 verified)
```

Elsewhere: any S3 client with endpoint `R2_ENDPOINT`, e.g.
`aws s3 cp s3://rquant-snapshots/snapshots/2026-10-12/2026-10-12.tar.gz . --endpoint-url $R2_ENDPOINT`.
Health check: `... snapshot_archive selftest` (put/head/get/delete a random object).
Manual run: `sudo systemctl start rquant-snapshot-archive.service`.

## Env / secret files (names only)

| File | Where | Holds |
|---|---|---|
| `/etc/rquant-web.env` | server | app env (Tushare token, paths) for web + research + collector |
| `/etc/rquant-web.secrets.env` | server | web secrets (drop-in `secrets.conf`) |
| `/home/lighthouse/rquant-research/.tushare.env` | server | Tushare token for `rquant-research.service` |
| `/etc/rquant-r2.env` (600, lighthouse) | server | `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `R2_BUCKET` |
| `/Users/roxor/brain/30-projects/rQuant/.env` (gitignored, 600) | Mac | local dev env incl. `R2_ACCOUNT_ID`, `R2_*` above |
| `deploy/env/*.env.example` | repo | templates only |

## Server-only artifacts

`minute_backfill.py` and `minutes-all.conf` previously existed only on the server; copies now
live in `deploy/research/`. The server still runs `~/rquant-research/minute_backfill.py` (the
drop-in path is unchanged); to update: copy from the release —
`cp ~/rquant-web-rel/current/deploy/research/minute_backfill.py ~/rquant-research/`.
