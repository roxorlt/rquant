# rQuant ops runbook

Single source of truth for what runs where. **Names only — no secret values in this repo.**
Times are Asia/Shanghai (server TZ = CST).

## Servers

| Host | Role | Access |
|---|---|---|
| `lighthouse@82.156.0.68` (Tencent Lighthouse) | prod: web API, runtime, monitor, research, snapshot collector, R2 archive | SSH key (`rquant_deploy`) |
| roxor's Mac | dev, launchd jobs (`deploy/launchd/`), local `.env` | — |

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
| `rquant-snapshot-archive.timer` | Mon-Fri **16:20** | tar.gz finished day dirs → R2, verify, prune >14d (verified only) | `deploy/systemd/` |
| `rquant-artifact-retention.timer` | every 5 min | artifact tiering/GC | `deploy/systemd/` |

Also on the server (user crontab, not systemd): `~/itick_probe` collect 09:10 / stop 15:12 / report 15:20.

> **Monday 08:50 restore**: not found as a server systemd timer or Mac launchd/cron entry when
> this doc was written — fill in where it runs.

Status: `systemctl list-timers --all | grep rquant`; logs: `journalctl -u <unit> -n 100`.
Start/stop: `sudo systemctl start|stop <unit>`; disable: `sudo systemctl disable --now <timer>`.
Snapshot collector kill switch for today: `touch ~/rquant-research/snapshots/STOP`.

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
