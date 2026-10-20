#!/usr/bin/env python3
"""Fetch archived serving generations back from R2 (counterpart of serving_generations_retention.py).

    # one generation (day looked up in the local catalog, else --date, else scan --since days back)
    python serving_generations_fetch.py --credentials-file /etc/rquant-r2-published-archive.env --generation <64-hex id> --dest /tmp/restore
    # every archived generation of a natural day (Asia/Shanghai)
    python serving_generations_fetch.py --credentials-file /etc/rquant-r2-published-archive.env --date 2026-10-14 --dest /tmp/restore

Each bundle is downloaded, its sha256 checked against the index and the R2 HEAD, then
unpacked to ``<dest>/<generation_id>/{manifest.json,serving.duckdb}``; every
serving.duckdb is checked against the manifest ``content_sha256``. It never writes into
the live ``serving/generations`` directory unless you point --dest there yourself (don't:
restore into a scratch dir and open it with ServingReader.acquire_historical_generation
from a copy of the serving root).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from serving_generations_retention import (  # noqa: E402
    CATALOG_NAME, DEFAULT_PREFIX, GEN_RE, TZ, ObjectStore, decompressor, index_key,
    sha256_file, setup_logging, store_from_credentials)

log = logging.getLogger("rquant.serving_fetch")


def catalog_day(state_dir: Path | None, gen_id: str) -> str | None:
    if state_dir is None or not (state_dir / CATALOG_NAME).exists():
        return None
    day = None
    for line in (state_dir / CATALOG_NAME).read_text().splitlines():
        rec = json.loads(line)
        if rec.get("id") == gen_id:
            day = rec["day"]
    return day


def get_index(store: ObjectStore, prefix: str, day: str, tmp: Path) -> dict[str, Any] | None:
    key = index_key(prefix, day)
    if store.head(key) is None:
        return None
    p = tmp / f"index-{day}.json"
    store.get_file(key, p)
    return json.loads(p.read_text())


def fetch_bundle(store: ObjectStore, bundle: dict[str, Any], dest: Path, only: set[str] | None,
                 tmp: Path) -> list[str]:
    head = store.head(bundle["key"])
    if not head or head.get("size") != bundle["size"] or head.get("sha256") != bundle["sha256"]:
        raise RuntimeError(f"R2 object {bundle['key']} missing or differs from index: {head}")
    local = tmp / Path(bundle["key"]).name
    store.get_file(bundle["key"], local)
    if local.stat().st_size != bundle["size"] or sha256_file(local) != bundle["sha256"]:
        raise RuntimeError(f"downloaded {bundle['key']} does not match index size/sha256")
    want = {g["id"]: g for g in bundle["generations"]}
    out: list[str] = []
    proc = subprocess.Popen(decompressor(bundle["key"]) + [str(local)], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
            for m in tar:
                gid, _, name = m.name.partition("/")
                if not GEN_RE.match(gid) or name not in ("manifest.json", "serving.duckdb") or not m.isfile():
                    raise RuntimeError(f"unexpected member {m.name}")
                if only is not None and gid not in only:
                    continue
                target = dest / gid
                target.mkdir(parents=True, exist_ok=True)
                f = tar.extractfile(m)
                assert f is not None
                h = hashlib.sha256()
                with (target / name).open("wb") as w:
                    for chunk in iter(lambda: f.read(1 << 20), b""):
                        h.update(chunk)
                        w.write(chunk)
                if name == "serving.duckdb":
                    if h.hexdigest() != want[gid]["content_sha256"]:
                        raise RuntimeError(f"content sha mismatch for {gid}")
                    out.append(gid)
    finally:
        proc.stdout.close()
        rc = proc.wait()
    local.unlink()
    if rc != 0:
        raise RuntimeError("decompress failed")
    return out


def fetch(store: ObjectStore, dest: Path, *, generation: str | None = None, date: str | None = None,
          prefix: str = DEFAULT_PREFIX, state_dir: Path | None = None, since_days: int = 60,
          today: dt.date | None = None) -> list[str]:
    if not generation and not date:
        raise SystemExit("need --generation or --date")
    if generation and not GEN_RE.match(generation):
        raise SystemExit("--generation must be a 64-hex id")
    dest.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="srvfetch-") as td:
        tmp = Path(td)
        if date:
            days = [date]
        elif (d := catalog_day(state_dir, generation)):
            days = [d]
        else:
            today = today or dt.datetime.now(TZ).date()
            days = [(today - dt.timedelta(days=i)).isoformat() for i in range(since_days + 1)]
        got: list[str] = []
        for day in days:
            index = get_index(store, prefix, day, tmp)
            if not index:
                continue
            for b in index.get("bundles", []):
                ids = {g["id"] for g in b["generations"]}
                if generation and generation not in ids:
                    continue
                got += fetch_bundle(store, b, dest, {generation} if generation else None, tmp)
            if generation and generation in got:
                break
        if generation and generation not in got:
            raise SystemExit(f"generation {generation} not found in R2 under {prefix}/ ({len(days)} day(s) searched)")
        if date and not got:
            raise SystemExit(f"nothing archived for {date}")
    log.info("restored %d generation(s) into %s", len(got), dest)
    return got


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="fetch archived serving generations from R2")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--generation")
    g.add_argument("--date", help="YYYY-MM-DD natural day, Asia/Shanghai")
    ap.add_argument("--dest", type=Path, required=True)
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--credentials-file", type=Path, required=True,
                    help="0600 KEY=VALUE file with R2_ARCHIVE_* (e.g. /etc/rquant-r2-published-archive.env)")
    ap.add_argument("--state-dir", type=Path,
                    default=Path.home() / "rquant/data/runtime/serving-retention")
    ap.add_argument("--since-days", type=int, default=60)
    a = ap.parse_args(argv)
    setup_logging(None, False)
    got = fetch(store_from_credentials(a.credentials_file), a.dest, generation=a.generation, date=a.date,
                prefix=a.prefix, state_dir=a.state_dir, since_days=a.since_days)
    print(json.dumps({"restored": got, "dest": str(a.dest)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
