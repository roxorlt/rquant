#!/usr/bin/env python3
"""Retention for ``~/rquant/data/runtime/serving/generations`` (stdlib only, Python >= 3.11).

Rule (2026-10-20, see /workspace/rquant/ops/generations-retention-20261020.md):
  * Time stamp of a generation = ``built_at`` in its ``manifest.json`` (UTC, written by
    ``serving_publisher._publish_locked``). Generation ids are content hashes, no date.
  * Days are NATURAL days in Asia/Shanghai (generations are published 7x24, weekends too).
    ``--keep-days 3`` keeps everything built at or after 00:00 Asia/Shanghai of
    (today - 3), i.e. three full natural days plus today (always >= 72 h; conservative).
  * Always kept, whatever their age:
      - the generation named by ``current.json`` and its ``previous_generation_id``;
      - every generation named in ``publication-intent.json`` (if an intent exists the
        run deletes nothing at all: a publish is half-way);
      - ids in ``--pin`` / ``--pin-file`` (one id per line, # comments);
      - the newest ``--keep-min`` generations (default 2);
      - generations younger than ``--min-age-minutes`` (default 30; "being written");
      - anything whose manifest is unreadable, whose id does not match its directory, or
        whose built_at is unparsable or in the future (logged as KEEP-UNSURE);
      - any entry whose name is not a 64-hex id (``.candidate-*`` = publish in progress,
        ``.publish.lock`` etc. live one level up and are never touched).
  * Deletion happens in small batches while holding the publisher's own
    ``serving/.publish.lock`` (fcntl.flock, the same lock ``ServingPublisher`` takes), and
    ``current.json`` / intent are re-read inside the lock before each batch.
  * Optional ``--archive`` (default OFF): before deleting, generations are bundled per
    natural day into ``<prefix>/<YYYY-MM-DD>/<YYYY-MM-DD>-<sha8 of id list>.tar.zst``
    (zstd --long=27; gzip fallback), every member's serving.duckdb sha256 is re-checked
    against its manifest ``content_sha256`` inside the bundle, the bundle is PUT to R2
    with a signed payload sha256, HEAD-verified (size + x-amz-meta-sha256), and the
    per-day ``index.json`` is updated. Only generations inside a verified bundle are
    deleted; any upload / verify failure keeps them and the run exits 1.
  * ``--dry-run`` lists what would be deleted (and archived) with sizes and changes
    nothing (no lock, no writes except the log). Re-running gives the same answer for the
    same clock; after a real run, a re-run finds nothing more.

2026-10-20 (roxor "做好了测过了就上"): R2 bucket ``rquant-published-archive``.
  * Credentials come ONLY from ``--credentials-file`` (KEY=VALUE, must be a regular file
    owned by the running user with mode 0600; keys R2_ARCHIVE_ACCESS_KEY_ID,
    R2_ARCHIVE_SECRET_ACCESS_KEY, R2_ARCHIVE_ENDPOINT, R2_ARCHIVE_BUCKET). Values are never
    logged or printed. No dependency on the rquant package (stdlib SigV4 client below).
  * Daily order (cron 15:40): incremental upload -> spot check (``--verify-sample`` files
    over earliest / middle / latest day and every day about to be pruned are downloaded
    back from R2 and their sha256 compared with the local originals) -> delete. A failed
    spot check deletes nothing (exit 1, logged as SPOTCHECK-FAILED).
  * Right before deletion every candidate is re-confirmed against R2: the per-day
    index.json is re-read from R2, its bundle HEAD must match size + sha256, and the
    generation's files must be listed there with the same sizes as on disk.
  * Hard floor: nothing built within the last 72 h is ever deleted, whatever --keep-days.
  * ``--no-delete``: upload (+ spot check) only, used for the first back-fill.

Exit codes: 0 ok, 1 something kept because of an error, 2 refused (bad root / intent /
lock timeout / another retention run).
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import fcntl
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import hmac
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Iterator, Protocol
from zoneinfo import ZoneInfo

log = logging.getLogger("rquant.serving_retention")
TZ = ZoneInfo("Asia/Shanghai")
GEN_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_PREFIX = "serving-generations"
CATALOG_NAME = "catalog.jsonl"
MEMBERS = ("manifest.json", "serving.duckdb")
HARD_FLOOR = dt.timedelta(hours=72)
CRED_KEYS = ("R2_ARCHIVE_ACCESS_KEY_ID", "R2_ARCHIVE_SECRET_ACCESS_KEY",
             "R2_ARCHIVE_ENDPOINT", "R2_ARCHIVE_BUCKET")


# --------------------------------------------------------------------------- model
@dataclasses.dataclass(frozen=True)
class Generation:
    gen_id: str
    path: Path
    built_at: dt.datetime | None  # aware UTC, None = unknown
    content_sha256: str | None
    size: int
    mtime: float
    problem: str | None = None

    @property
    def local_day(self) -> str | None:
        return self.built_at.astimezone(TZ).date().isoformat() if self.built_at else None


@dataclasses.dataclass
class Plan:
    now: dt.datetime
    cutoff: dt.datetime
    delete: list[Generation]
    keep: dict[str, str]  # gen_id -> reason
    unsure: dict[str, str]
    total: int
    refused: str | None = None
    kept: list[Generation] = dataclasses.field(default_factory=list)

    def bytes_delete(self) -> int:
        return sum(g.size for g in self.delete)


class ObjectStore(Protocol):
    bucket: str

    def put_file(self, key: str, src: Path, sha: str) -> None: ...
    def head(self, key: str) -> dict[str, Any] | None: ...
    def get_file(self, key: str, dest: Path) -> None: ...


# --------------------------------------------------------------------------- helpers
def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_ts(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        t = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if t.tzinfo is None:
        return None
    return t.astimezone(dt.UTC)


def _dir_size(p: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(p):
        for name in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(root, name)).st_size
    return total


def read_generation(path: Path) -> Generation:
    gen_id = path.name
    st = os.lstat(path)
    size = _dir_size(path)
    try:
        manifest = json.loads((path / "manifest.json").read_text())
    except (OSError, ValueError) as exc:
        return Generation(gen_id, path, None, None, size, st.st_mtime, f"manifest unreadable: {exc.__class__.__name__}")
    if manifest.get("generation_id") != gen_id:
        return Generation(gen_id, path, None, None, size, st.st_mtime, "manifest generation_id mismatch")
    built = _parse_ts(manifest.get("built_at"))
    if built is None:
        return Generation(gen_id, path, None, None, size, st.st_mtime, "built_at missing/unparsable")
    return Generation(gen_id, path, built, manifest.get("content_sha256"), size, st.st_mtime)


def read_pointer_ids(root: Path) -> tuple[set[str], str | None]:
    """Ids referenced by current.json / publication-intent.json. Returns (ids, refusal)."""
    ids: set[str] = set()
    cur = root / "current.json"
    try:
        p = json.loads(cur.read_text())
    except (OSError, ValueError):
        return ids, "current.json missing or unreadable"
    for k in ("generation_id", "previous_generation_id"):
        if isinstance(p.get(k), str):
            ids.add(p[k])
    if not p.get("generation_id"):
        return ids, "current.json has no generation_id"
    intent = root / "publication-intent.json"
    if intent.exists():
        with contextlib.suppress(OSError, ValueError):
            for v in json.loads(intent.read_text()).values():
                if isinstance(v, dict):
                    for k in ("generation_id", "previous_generation_id"):
                        if isinstance(v.get(k), str):
                            ids.add(v[k])
        return ids, "publication-intent.json present (publish in progress)"
    return ids, None


def read_pins(pins: Iterable[str], pin_files: Iterable[Path]) -> set[str]:
    out = {p.strip() for p in pins if p.strip()}
    for f in pin_files:
        if f.exists():
            for line in f.read_text().splitlines():
                line = line.split("#", 1)[0].strip()
                if line:
                    out.add(line)
    return out


def cutoff_for(now: dt.datetime, keep_days: int) -> dt.datetime:
    local_today = now.astimezone(TZ).date()
    start = dt.datetime.combine(local_today - dt.timedelta(days=keep_days), dt.time(0), TZ)
    return start.astimezone(dt.UTC)


# --------------------------------------------------------------------------- planning
def build_plan(root: Path, *, now: dt.datetime, keep_days: int, keep_min: int,
               min_age_minutes: float, pins: set[str]) -> Plan:
    gens_root = root / "generations"
    cutoff = cutoff_for(now, keep_days)
    if root.is_symlink() or not gens_root.is_dir() or gens_root.is_symlink():
        return Plan(now, cutoff, [], {}, {}, 0, refused=f"bad serving root {root}")
    pointer_ids, refusal = read_pointer_ids(root)
    gens: list[Generation] = []
    keep: dict[str, str] = {}
    unsure: dict[str, str] = {}
    for entry in sorted(os.scandir(gens_root), key=lambda e: e.name):
        if not GEN_RE.match(entry.name):
            continue  # .candidate-* (being written) and anything foreign: never touched
        if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
            unsure[entry.name] = "not a plain directory"
            continue
        g = read_generation(Path(entry.path))
        if g.problem:
            unsure[g.gen_id] = g.problem
            continue
        if g.built_at > now + dt.timedelta(minutes=5):
            unsure[g.gen_id] = "built_at in the future"
            continue
        gens.append(g)
    gens.sort(key=lambda g: (g.built_at, g.gen_id))
    newest = {g.gen_id for g in gens[-keep_min:]} if keep_min > 0 else set()
    min_age = dt.timedelta(minutes=min_age_minutes)
    delete: list[Generation] = []
    kept: list[Generation] = []
    for g in gens:
        if g.gen_id in pointer_ids:
            keep[g.gen_id] = "pointer (current/previous/intent)"
        elif g.gen_id in pins:
            keep[g.gen_id] = "pinned"
        elif g.gen_id in newest:
            keep[g.gen_id] = "newest"
        elif now - g.built_at < min_age or now.timestamp() - g.mtime < min_age.total_seconds():
            keep[g.gen_id] = "too young"
        elif g.built_at >= cutoff:
            keep[g.gen_id] = "within keep window"
        else:
            delete.append(g)
            continue
        kept.append(g)
    total = len(gens) + len(unsure)
    plan = Plan(now, cutoff, delete, keep, unsure, total, kept=kept)
    if refusal:
        plan.refused = refusal
    missing = pointer_ids - {g.gen_id for g in gens} - set(unsure)
    if missing:
        log.warning("pointer ids without a generation dir: %s", sorted(missing))
    return plan


def summarize(plan: Plan) -> dict[str, Any]:
    by_day: dict[str, dict[str, int]] = {}
    for g in plan.delete:
        d = by_day.setdefault(g.local_day or "?", {"generations": 0, "bytes": 0})
        d["generations"] += 1
        d["bytes"] += g.size
    reasons: dict[str, int] = {}
    for r in plan.keep.values():
        reasons[r] = reasons.get(r, 0) + 1
    kept_days: dict[str, dict[str, int]] = {}
    for g in plan.kept:
        d = kept_days.setdefault(g.local_day or "?", {"generations": 0, "bytes": 0})
        d["generations"] += 1
        d["bytes"] += g.size
    pointer_kept = sorted(gid for gid, r in plan.keep.items() if r.startswith("pointer"))
    return {
        "now": plan.now.astimezone(TZ).isoformat(timespec="seconds"),
        "cutoff": plan.cutoff.astimezone(TZ).isoformat(timespec="seconds"),
        "total_generations": plan.total,
        "delete_generations": len(plan.delete),
        "delete_bytes": plan.bytes_delete(),
        "delete_by_day": dict(sorted(by_day.items())),
        "keep_generations": len(plan.keep) + len(plan.unsure),
        "keep_reasons": dict(sorted(reasons.items())),
        "keep_bytes": sum(g.size for g in plan.kept),
        "keep_by_day": dict(sorted(kept_days.items())),
        "keep_pointer_ids": pointer_kept,
        "keep_unsure": len(plan.unsure),
        "refused": plan.refused,
    }


# --------------------------------------------------------------------------- locking
@contextlib.contextmanager
def publish_lock(root: Path, timeout_s: float) -> Iterator[None]:
    """Take the publisher's flock on serving/.publish.lock (never creates it)."""
    path = root / ".publish.lock"
    flags = os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RuntimeError("publish lock is not a regular file")
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("publish lock busy") from None
                time.sleep(0.2)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextlib.contextmanager
def single_run_lock(state_dir: Path) -> Iterator[None]:
    state_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(state_dir / ".retention.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise RuntimeError("another retention run holds the lock") from None
    try:
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def remove_generation(path: Path) -> None:
    # generation dirs are dr-x------ and files r--------: the dir needs u+w to unlink.
    os.chmod(path, 0o700)
    shutil.rmtree(path)


# --------------------------------------------------------------------------- archive
def compressor() -> tuple[list[str], str]:
    if shutil.which("zstd"):
        return ["zstd", "-q", "-3", "--long=27", "-T1", "-c"], ".tar.zst"
    return ["gzip", "-6", "-n", "-c"], ".tar.gz"


def decompressor(key: str) -> list[str]:
    return ["zstd", "-q", "-d", "--long=27", "-c"] if key.endswith(".zst") else ["gzip", "-d", "-c"]


def bundle_key(prefix: str, day: str, ids: list[str], ext: str) -> str:
    digest = hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()[:8]
    return f"{prefix}/{day}/{day}-{digest}{ext}"


def index_key(prefix: str, day: str) -> str:
    return f"{prefix}/{day}/index.json"


def build_bundle(gens: list[Generation], dest: Path) -> tuple[list[str], str]:
    """tar (deterministic order/metadata) | compress  -> dest. Returns (cmd, ext)."""
    cmd, ext = compressor()
    with dest.open("wb") as out:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=out)
        assert proc.stdin is not None
        with tarfile.open(fileobj=proc.stdin, mode="w|", format=tarfile.PAX_FORMAT) as tar:
            for g in sorted(gens, key=lambda g: g.gen_id):
                for name in ("manifest.json", "serving.duckdb"):
                    p = g.path / name
                    info = tar.gettarinfo(str(p), arcname=f"{g.gen_id}/{name}")
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    with p.open("rb") as f:
                        tar.addfile(info, f)
        proc.stdin.close()
        if proc.wait() != 0:
            raise RuntimeError(f"{cmd[0]} failed with {proc.returncode}")
    return cmd, ext


def verify_bundle(bundle: Path, key: str, gens: list[Generation]) -> None:
    """Stream-decompress and check every member against the manifests on disk."""
    want = {g.gen_id: g for g in gens}
    seen: dict[str, set[str]] = {}
    proc = subprocess.Popen(decompressor(key) + [str(bundle)], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
            for m in tar:
                gid, _, name = m.name.partition("/")
                if gid not in want or name not in MEMBERS or not m.isfile():
                    raise RuntimeError(f"unexpected member {m.name}")
                if m.size != os.lstat(want[gid].path / name).st_size:
                    raise RuntimeError(f"size mismatch for {m.name}")
                f = tar.extractfile(m)
                assert f is not None
                h = hashlib.sha256()
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
                if name == "serving.duckdb" and h.hexdigest() != want[gid].content_sha256:
                    raise RuntimeError(f"content sha mismatch for {gid}")
                if name == "manifest.json" and h.hexdigest() != sha256_file(want[gid].path / name):
                    raise RuntimeError(f"manifest mismatch for {gid}")
                seen.setdefault(gid, set()).add(name)
    finally:
        proc.stdout.close()
        rc = proc.wait()
    if rc != 0:
        raise RuntimeError("decompress failed")
    incomplete = [g for g in want if seen.get(g) != {"manifest.json", "serving.duckdb"}]
    if incomplete:
        raise RuntimeError(f"bundle misses members for {len(incomplete)} generations")


def local_files(g: Generation) -> dict[str, int]:
    return {name: os.lstat(g.path / name).st_size for name in MEMBERS}


def load_index(store: ObjectStore, key: str, tmp: Path) -> dict[str, Any]:
    if store.head(key) is None:
        return {"bundles": []}
    p = tmp / "index-old.json"
    store.get_file(key, p)
    return json.loads(p.read_text())


def put_verified(store: ObjectStore, key: str, src: Path, attempts: int = 3,
                 backoff_s: float = 5.0) -> dict[str, Any]:
    sha, size = sha256_file(src), src.stat().st_size
    for i in range(attempts):
        try:
            store.put_file(key, src, sha)
            break
        except Exception as exc:  # noqa: BLE001 - R2 has returned 500 / SSL EOF before
            with contextlib.suppress(Exception):
                got = store.head(key)
                if got and got.get("size") == size and got.get("sha256") == sha:
                    log.warning("PUT %s raised %s but the object is there and matches", key, exc.__class__.__name__)
                    break
            if i == attempts - 1:
                raise
            log.warning("PUT %s failed (%s), retry %d/%d", key, exc.__class__.__name__, i + 1, attempts - 1)
            time.sleep(backoff_s * (i + 1))
    got = store.head(key)
    if not got or got.get("size") != size or got.get("sha256") != sha:
        raise RuntimeError(f"verify failed for {key}: local size={size} sha={sha[:12]} remote={got}")
    return {"key": key, "size": size, "sha256": sha}


def archived_ids(store: ObjectStore, index: dict[str, Any]) -> set[str]:
    """Ids already in a bundle that still HEAD-verifies."""
    ok: set[str] = set()
    for b in index.get("bundles", []):
        got = store.head(b["key"])
        if got and got.get("size") == b["size"] and got.get("sha256") == b["sha256"]:
            ok.update(x["id"] for x in b["generations"])
        else:
            log.error("index lists %s but R2 object is missing/mismatched (%s)", b["key"], got)
    return ok


def archive_day(store: ObjectStore, prefix: str, day: str, gens: list[Generation],
                workdir: Path, catalog: Path | None) -> set[str]:
    """Archive one natural day. Returns ids that are now safely in R2."""
    with tempfile.TemporaryDirectory(prefix="srvret-", dir=workdir) as td:
        tmp = Path(td)
        ikey = index_key(prefix, day)
        index = load_index(store, ikey, tmp)
        done = archived_ids(store, index)
        todo = [g for g in gens if g.gen_id not in done]
        if todo:
            _, ext = compressor()
            key = bundle_key(prefix, day, [g.gen_id for g in todo], ext)
            bundle = tmp / Path(key).name
            build_bundle(todo, bundle)
            verify_bundle(bundle, key, todo)
            info = put_verified(store, key, bundle)
            info["created_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
            info["generations"] = [
                {"id": g.gen_id, "built_at": g.built_at.isoformat(), "content_sha256": g.content_sha256,
                 "files": local_files(g)}
                for g in sorted(todo, key=lambda g: g.gen_id)]
            index.setdefault("bundles", []).append(info)
            index["day"] = day
            ip = tmp / "index.json"
            ip.write_text(json.dumps(index, indent=1, sort_keys=True))
            put_verified(store, ikey, ip)
            log.info("archived %d generations of %s -> %s (%d bytes)", len(todo), day, key, info["size"])
            if catalog is not None:
                with catalog.open("a") as f:
                    for g in todo:
                        f.write(json.dumps({"id": g.gen_id, "day": day, "key": key,
                                            "bucket": store.bucket}) + "\n")
            done |= {g.gen_id for g in todo}
        return {g.gen_id for g in gens if g.gen_id in done}


def confirm_in_r2(store: ObjectStore, prefix: str, gens: list[Generation], workdir: Path) -> set[str]:
    """Per generation: listed in a bundle of the day's index.json (re-read from R2), that
    bundle HEADs with the recorded size + sha256, and every file's recorded size equals
    the size on disk. Returns the confirmed ids; the rest is logged and kept."""
    ok: set[str] = set()
    by_day: dict[str, list[Generation]] = {}
    for g in gens:
        by_day.setdefault(g.local_day or "?", []).append(g)
    with tempfile.TemporaryDirectory(prefix="srvconf-", dir=workdir) as td:
        for day, day_gens in sorted(by_day.items()):
            try:
                index = load_index(store, index_key(prefix, day), Path(td))
                entries: dict[str, dict[str, Any]] = {}
                for b in index.get("bundles", []):
                    got = store.head(b["key"])
                    if got and got.get("size") == b["size"] and got.get("sha256") == b["sha256"]:
                        for e in b["generations"]:
                            entries[e["id"]] = e
                    else:
                        log.error("R2 bundle %s missing or differs from index (%s)", b["key"], got)
            except Exception:  # noqa: BLE001
                log.exception("cannot read R2 index for %s; its generations are kept", day)
                continue
            for g in day_gens:
                e = entries.get(g.gen_id)
                try:
                    local = local_files(g)
                except OSError:
                    local = None
                if e is None:
                    log.error("KEEP-NOT-IN-R2 %s (%s)", g.gen_id, day)
                elif local is None or e.get("files") != local or e.get("content_sha256") != g.content_sha256:
                    log.error("KEEP-R2-SIZE-MISMATCH %s r2=%s local=%s", g.gen_id, e.get("files"), local)
                else:
                    ok.add(g.gen_id)
    return ok


def pick_samples(gens: list[Generation], must_days: Iterable[str], n: int,
                 rng: random.Random) -> list[tuple[Generation, str]]:
    """>= n (generation, file) pairs: earliest, middle and latest day plus every day in
    must_days get at least one, the rest is random over all given generations."""
    by_day: dict[str, list[Generation]] = {}
    for g in gens:
        by_day.setdefault(g.local_day or "?", []).append(g)
    days = sorted(by_day)
    if not days:
        return []
    want_days = [days[0], days[len(days) // 2], days[-1]] + sorted(set(must_days) & set(days))
    picks: list[tuple[Generation, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(g: Generation, name: str) -> bool:
        if (g.gen_id, name) in seen:
            return False
        seen.add((g.gen_id, name))
        picks.append((g, name))
        return True

    for d in want_days:
        g = rng.choice(by_day[d])
        add(g, "serving.duckdb")
    pool = [(g, name) for g in gens for name in MEMBERS]
    rng.shuffle(pool)
    for g, name in pool:
        if len(picks) >= n:
            break
        add(g, name)
    return picks


def spot_check(store: ObjectStore, prefix: str, gens: list[Generation], must_days: Iterable[str],
               n: int, workdir: Path, rng: random.Random) -> tuple[bool, list[dict[str, Any]]]:
    """Download the bundles of the sampled files back from R2, extract those files and
    compare their sha256 with the originals on disk. Any error counts as a failure."""
    picks = pick_samples(gens, must_days, n, rng)
    results: list[dict[str, Any]] = []
    if len(picks) < min(n, 2 * len(gens)):
        log.error("SPOTCHECK-FAILED only %d samples available (want %d)", len(picks), n)
        return False, results
    need: dict[str, dict[str, set[str]]] = {}  # day -> gen -> names
    for g, name in picks:
        need.setdefault(g.local_day or "?", {}).setdefault(g.gen_id, set()).add(name)
    gmap = {g.gen_id: g for g, _ in picks}
    remote: dict[tuple[str, str], str] = {}
    errors: list[str] = []
    with tempfile.TemporaryDirectory(prefix="srvspot-", dir=workdir) as td:
        tmp = Path(td)
        for day, gen_names in sorted(need.items()):
            try:
                index = load_index(store, index_key(prefix, day), tmp)
                for b in index.get("bundles", []):
                    ids = {e["id"] for e in b["generations"]} & set(gen_names)
                    if not ids:
                        continue
                    local = tmp / Path(b["key"]).name
                    store.get_file(b["key"], local)
                    if local.stat().st_size != b["size"] or sha256_file(local) != b["sha256"]:
                        raise RuntimeError(f"downloaded {b['key']} differs from index size/sha256")
                    proc = subprocess.Popen(decompressor(b["key"]) + [str(local)], stdout=subprocess.PIPE)
                    assert proc.stdout is not None
                    try:
                        with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
                            for m in tar:
                                gid, _, name = m.name.partition("/")
                                if gid in ids and name in gen_names[gid] and m.isfile():
                                    f = tar.extractfile(m)
                                    assert f is not None
                                    h = hashlib.sha256()
                                    for chunk in iter(lambda: f.read(1 << 20), b""):
                                        h.update(chunk)
                                    remote[(gid, name)] = h.hexdigest()
                    finally:
                        proc.stdout.close()
                        if proc.wait() != 0:
                            errors.append(f"decompress failed for {b['key']}")
                    local.unlink()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{day}: {exc.__class__.__name__}: {exc}")
    ok = not errors
    for g, name in picks:
        try:
            lsha = sha256_file(g.path / name)
        except OSError:
            lsha = None
        rsha = remote.get((g.gen_id, name))
        match = lsha is not None and lsha == rsha
        ok = ok and match
        r = {"day": g.local_day, "generation": g.gen_id, "file": name,
             "local_sha256": lsha, "r2_sha256": rsha, "match": match}
        results.append(r)
        log.info("SPOTCHECK %s %s/%s local=%s r2=%s %s", g.local_day, g.gen_id, name,
                 lsha, rsha, "OK" if match else "MISMATCH")
    for e in errors:
        log.error("SPOTCHECK error: %s", e)
    log.log(logging.INFO if ok else logging.ERROR, "%s %d/%d files match",
            "SPOTCHECK-PASSED" if ok else "SPOTCHECK-FAILED", sum(r["match"] for r in results), len(results))
    return ok, results


# --------------------------------------------------------------------------- run
def run(root: Path, *, dry_run: bool, now: dt.datetime | None = None, keep_days: int = 3,
        keep_min: int = 2, min_age_minutes: float = 30, pins: set[str] | None = None,
        archive: bool = False, store: ObjectStore | None = None, prefix: str = DEFAULT_PREFIX,
        state_dir: Path | None = None, batch: int = 200, lock_timeout_s: float = 60,
        list_out: Path | None = None, delete: bool = True, verify_sample: int = 0,
        rng: random.Random | None = None) -> tuple[int, dict[str, Any]]:
    now = now or dt.datetime.now(dt.UTC)
    pins = pins or set()
    plan = build_plan(root, now=now, keep_days=keep_days, keep_min=keep_min,
                      min_age_minutes=min_age_minutes, pins=pins)
    summary = summarize(plan)
    summary.update({"mode": "dry-run" if dry_run else "delete", "archive": archive})
    for gid, why in sorted(plan.unsure.items()):
        log.warning("KEEP-UNSURE %s: %s", gid, why)
    if list_out is not None:
        with list_out.open("w") as f:
            for g in plan.delete:
                f.write(f"{g.gen_id}\t{g.built_at.isoformat()}\t{g.local_day}\t{g.size}\n")
    if dry_run:
        for g in plan.delete:
            log.debug("would delete %s built_at=%s size=%d", g.gen_id, g.built_at.isoformat(), g.size)
        if archive:
            settled = [g for g in plan.kept if plan.keep.get(g.gen_id) != "too young"]
            summary["would_archive_generations"] = len(plan.delete) + len(settled)
            summary["would_archive_raw_bytes"] = plan.bytes_delete() + sum(g.size for g in settled)
        log.info("DRY-RUN %s", json.dumps(summary, ensure_ascii=False))
        return (2 if plan.refused else 0), summary
    if plan.refused:
        log.error("REFUSED: %s", plan.refused)
        return 2, summary
    if archive and store is None:
        log.error("REFUSED: --archive without an object store")
        return 2, summary
    state_dir = state_dir or root.parent / "serving-retention"
    rc = 0
    deleted: list[str] = []
    archived: set[str] = set()
    try:
        with single_run_lock(state_dir):
            candidates = plan.delete
            if archive:
                # (3) every settled generation is archived (today's so far + any older day
                # not yet in R2), not only the ones about to go; deletion still needs a
                # verified bundle. Too-young ones wait for the next run.
                to_archive = list(plan.delete) + [
                    g for g in plan.kept if plan.keep.get(g.gen_id) != "too young"]
                by_day: dict[str, list[Generation]] = {}
                for g in to_archive:
                    by_day.setdefault(g.local_day or "?", []).append(g)
                for day, gens in sorted(by_day.items()):
                    try:
                        archived |= archive_day(store, prefix, day, gens, state_dir,
                                                state_dir / CATALOG_NAME)
                    except Exception:  # noqa: BLE001 - keep going, never delete unverified
                        log.exception("archive failed for %s; its %d generations are kept", day, len(gens))
                        rc = 1
                candidates = [g for g in candidates if g.gen_id in archived]
                local_archived = [g for g in to_archive if g.gen_id in archived]
                summary["archived_raw_bytes"] = sum(g.size for g in local_archived)
                if verify_sample > 0:
                    ok, results = spot_check(
                        store, prefix, local_archived,
                        {g.local_day for g in candidates if g.local_day},
                        verify_sample, state_dir, rng or random.SystemRandom())
                    summary["spot_check"] = {"passed": ok, "files": len(results),
                                             "matched": sum(r["match"] for r in results)}
                    if not ok:
                        log.error("SPOTCHECK-FAILED: deleting nothing in this run")
                        rc = 1
                        candidates = []
                elif delete:
                    log.error("archive mode without --verify-sample: deleting nothing")
                    rc = 1
                    candidates = []
                if candidates and delete:
                    confirmed = confirm_in_r2(store, prefix, candidates, state_dir)
                    if len(confirmed) < len(candidates):
                        log.warning("%d of %d candidates not confirmed in R2; kept",
                                    len(candidates) - len(confirmed), len(candidates))
                    candidates = [g for g in candidates if g.gen_id in confirmed]
                    summary["confirmed_in_r2"] = len(confirmed)
            if not delete:
                log.info("--no-delete: %d generations would be eligible; nothing deleted", len(candidates))
                candidates = []
            for i in range(0, len(candidates), batch):
                chunk = candidates[i:i + batch]
                try:
                    with publish_lock(root, lock_timeout_s):
                        protected, refusal = read_pointer_ids(root)
                        if refusal:
                            log.error("stop: %s", refusal)
                            rc = max(rc, 2)
                            break
                        protected |= pins
                        for g in chunk:
                            if g.built_at is None or now - g.built_at < HARD_FLOOR:
                                log.error("keep %s: younger than 72 h (hard floor)", g.gen_id)
                                continue
                            if g.gen_id in protected:
                                log.info("keep %s: became a pointer/pin", g.gen_id)
                                continue
                            try:
                                remove_generation(g.path)
                                deleted.append(g.gen_id)
                                log.info("deleted %s built_at=%s size=%d", g.gen_id, g.built_at.isoformat(), g.size)
                            except OSError as exc:
                                log.error("could not delete %s: %s", g.gen_id, exc)
                                rc = max(rc, 1)
                except TimeoutError:
                    log.error("publish lock busy for %ss; stopping, rest kept", lock_timeout_s)
                    rc = max(rc, 2)
                    break
    except RuntimeError as exc:
        log.error("REFUSED: %s", exc)
        return 2, summary
    summary.update({"deleted_generations": len(deleted),
                    "deleted_bytes": sum(g.size for g in plan.delete if g.gen_id in set(deleted)),
                    "archived_generations": len(archived), "rc": rc})
    log.info("DONE %s", json.dumps(summary, ensure_ascii=False))
    return rc, summary


class R2Client:
    """Minimal SigV4 S3 client for one R2 bucket (PUT/HEAD/GET single objects). The
    secret is kept in a private attribute and never appears in repr / logs."""

    def __init__(self, endpoint: str, bucket: str, key_id: str, secret: str, timeout: float = 600) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.host = urllib.parse.urlparse(self.endpoint).netloc
        self.bucket = bucket
        self._key_id, self._secret, self.timeout = key_id, secret, timeout

    def __repr__(self) -> str:
        return f"R2Client(bucket={self.bucket!r})"

    def _request(self, method: str, key: str, *, body: Any = None, payload_sha: str | None = None,
                 headers: dict[str, str] | None = None):
        now = dt.datetime.now(dt.UTC)
        amz = now.strftime("%Y%m%dT%H%M%SZ")
        day = amz[:8]
        path = "/" + self.bucket + "/" + urllib.parse.quote(key, safe="/-_.~")
        h = {k.lower(): v for k, v in (headers or {}).items()}
        h.update({"host": self.host, "x-amz-date": amz,
                  "x-amz-content-sha256": payload_sha or hashlib.sha256(b"").hexdigest()})
        names = sorted(h)
        canon = "\n".join([method, path, "", "".join(f"{k}:{h[k].strip()}\n" for k in names),
                           ";".join(names), h["x-amz-content-sha256"]])
        scope = f"{day}/auto/s3/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canon.encode()).hexdigest()])
        k = ("AWS4" + self._secret).encode()
        for part in (day, "auto", "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        h["authorization"] = (f"AWS4-HMAC-SHA256 Credential={self._key_id}/{scope}, "
                              f"SignedHeaders={';'.join(names)}, Signature={sig}")
        del h["host"]
        req = urllib.request.Request(self.endpoint + path, data=body, method=method, headers=h)
        return urllib.request.urlopen(req, timeout=self.timeout)

    def put_file(self, key: str, src: Path, sha: str) -> None:
        with src.open("rb") as f:
            self._request("PUT", key, body=f, payload_sha=sha, headers={
                "content-length": str(src.stat().st_size), "x-amz-meta-sha256": sha,
                "content-type": "application/octet-stream"}).close()

    def head(self, key: str) -> dict[str, Any] | None:
        try:
            with self._request("HEAD", key) as r:
                return {"size": int(r.headers["Content-Length"]),
                        "sha256": r.headers.get("x-amz-meta-sha256")}
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def get_file(self, key: str, dest: Path) -> None:
        with self._request("GET", key) as r, dest.open("wb") as f:
            shutil.copyfileobj(r, f, 1 << 20)


def load_credentials(path: Path) -> dict[str, str]:
    """KEY=VALUE file; must be a regular file owned by us with no group/other bits.
    Error messages name keys and the path only, never values."""
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise SystemExit(f"credentials file {path} is not a regular file")
        if st.st_uid != os.getuid():
            raise SystemExit(f"credentials file {path} is not owned by the running user")
        if st.st_mode & 0o077:
            raise SystemExit(f"credentials file {path} must be mode 0600 (is {stat.S_IMODE(st.st_mode):o})")
        with os.fdopen(os.dup(fd), "r") as f:
            text = f.read()
    finally:
        os.close(fd)
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().removeprefix("export ").strip()
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
            v = v[1:-1]
        out[k] = v
    missing = [k for k in CRED_KEYS if not out.get(k)]
    if missing:
        raise SystemExit(f"credentials file {path} lacks {missing}")
    return out


def store_from_credentials(path: Path, expect_bucket: str | None = None) -> R2Client:
    c = load_credentials(path)
    if expect_bucket and c["R2_ARCHIVE_BUCKET"] != expect_bucket:
        raise SystemExit(f"credentials file is for bucket {c['R2_ARCHIVE_BUCKET']!r}, not {expect_bucket!r}")
    return R2Client(c["R2_ARCHIVE_ENDPOINT"], c["R2_ARCHIVE_BUCKET"],
                    c["R2_ARCHIVE_ACCESS_KEY_ID"], c["R2_ARCHIVE_SECRET_ACCESS_KEY"])


def setup_logging(log_file: Path | None, verbose: bool) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(message)s", force=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", type=Path, default=Path.home() / "rquant/data/runtime/serving")
    ap.add_argument("--keep-days", type=int, default=3)
    ap.add_argument("--keep-min", type=int, default=2)
    ap.add_argument("--min-age-minutes", type=float, default=30)
    ap.add_argument("--pin", action="append", default=[])
    ap.add_argument("--pin-file", action="append", type=Path, default=[])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true", help="really delete (required unless --dry-run)")
    ap.add_argument("--archive", action="store_true", help="archive to R2 before deleting (default off)")
    ap.add_argument("--credentials-file", type=Path, help="0600 KEY=VALUE file with R2_ARCHIVE_* (archive mode)")
    ap.add_argument("--bucket", help="refuse unless the credentials file is for this bucket")
    ap.add_argument("--no-delete", action="store_true", help="upload (+ spot check) only")
    ap.add_argument("--verify-sample", type=int, default=12,
                    help="files to download back from R2 and sha256-compare before deleting (archive mode)")
    ap.add_argument("--seed", type=int, help="sample seed (tests)")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--state-dir", type=Path)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--lock-timeout", type=float, default=60)
    ap.add_argument("--log-file", type=Path)
    ap.add_argument("--list-out", type=Path, help="write the delete list (tsv) here")
    ap.add_argument("--now", help="ISO timestamp with zone (tests / reproducible dry-run)")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    setup_logging(a.log_file, a.verbose)
    if a.dry_run == a.execute:
        log.error("pass exactly one of --dry-run / --execute")
        return 2
    if a.now and a.execute:
        log.error("--now is only for --dry-run (the 72 h hard floor uses the real clock)")
        return 2
    now = _parse_ts(a.now) if a.now else None
    store = None
    if a.archive and a.execute:
        if not a.credentials_file:
            log.error("--archive needs --credentials-file")
            return 2
        store = store_from_credentials(a.credentials_file, a.bucket)
    rc, summary = run(a.root, dry_run=a.dry_run, now=now, keep_days=a.keep_days,
                      keep_min=a.keep_min, min_age_minutes=a.min_age_minutes,
                      pins=read_pins(a.pin, a.pin_file), archive=a.archive, store=store,
                      prefix=a.prefix, state_dir=a.state_dir, batch=a.batch,
                      lock_timeout_s=a.lock_timeout, list_out=a.list_out, delete=not a.no_delete,
                      verify_sample=a.verify_sample if a.archive else 0,
                      rng=random.Random(a.seed) if a.seed is not None else None)
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return rc


if __name__ == "__main__":
    sys.exit(main())
