"""Unit tests on a simulated serving root + an in-memory object store (no R2, no network)."""
from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import serving_generations_fetch as fetchmod  # noqa: E402
import serving_generations_retention as ret  # noqa: E402

UTC = dt.UTC
NOW = dt.datetime(2026, 10, 21, 7, 45, tzinfo=UTC)  # 2026-10-21 15:45 Asia/Shanghai


class MemStore:
    """S3-ish store: put/head/get; can be told to fail or to lie."""

    def __init__(self, bucket: str = "mock-serving") -> None:
        self.bucket = bucket
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.fail_put: set[str] = set()   # key substrings whose PUT raises
        self.lie_head: set[str] = set()   # key substrings whose HEAD reports a wrong sha
        self.puts: list[str] = []
        self.corrupt_get: set[str] = set()  # key substrings whose GET returns flipped bytes
        self.fail_put_once: set[str] = set()

    def put_file(self, key: str, src: Path, sha: str) -> None:
        if any(s in key for s in self.fail_put):
            raise OSError("simulated HTTP 500")
        hit = [s for s in self.fail_put_once if s in key]
        if hit:
            self.fail_put_once.difference_update(hit)
            raise OSError("simulated SSL EOF")
        data = src.read_bytes()
        if hashlib.sha256(data).hexdigest() != sha:  # R2 rejects a body that misses its signed sha
            raise OSError("payload sha mismatch")
        self.objects[key] = (data, sha)
        self.puts.append(key)

    def head(self, key: str) -> dict[str, Any] | None:
        if key not in self.objects:
            return None
        data, sha = self.objects[key]
        if any(s in key for s in self.lie_head):
            sha = "0" * 64
        return {"size": len(data), "sha256": sha}

    def get_file(self, key: str, dest: Path) -> None:
        data = self.objects[key][0]
        if any(s in key for s in self.corrupt_get):
            data = bytes([data[0] ^ 0xFF]) + data[1:]
        dest.write_bytes(data)


def _gid(n: int) -> str:
    return hashlib.sha256(f"gen-{n}".encode()).hexdigest()


def make_gen(gens: Path, n: int, built_at: dt.datetime, payload: bytes | None = None) -> str:
    gid = _gid(n)
    d = gens / gid
    d.mkdir()
    db = payload if payload is not None else (b"DUCK" + bytes([n % 251]) * 4000 + b"common" * 500)
    (d / "serving.duckdb").write_bytes(db)
    (d / "manifest.json").write_text(json.dumps({
        "generation_id": gid, "built_at": built_at.isoformat().replace("+00:00", "Z"),
        "content_sha256": hashlib.sha256(db).hexdigest()}))
    os.chmod(d / "serving.duckdb", 0o400)
    os.chmod(d / "manifest.json", 0o400)
    os.chmod(d, 0o500)
    os.utime(d, (built_at.timestamp(), built_at.timestamp()))
    return gid


def write_current(root: Path, cur: str, prev: str | None) -> None:
    (root / "current.json").write_text(json.dumps({
        "generation_id": cur, "previous_generation_id": prev, "manifest_sha256": "x" * 64,
        "published_at": NOW.isoformat()}))


@pytest.fixture
def serving(tmp_path: Path):
    """10-13 .. 10-21 (Shanghai), 6 generations a day, current = newest, previous = 2nd newest."""
    root = tmp_path / "serving"
    gens = root / "generations"
    gens.mkdir(parents=True)
    (root / "receipts").mkdir()
    (root / ".publish.lock").write_text("")
    ids: list[tuple[str, dt.datetime]] = []
    n = 0
    start = dt.datetime(2026, 10, 12, 16, 30, tzinfo=UTC)  # 10-13 00:30 Shanghai
    t = start
    while t <= NOW - dt.timedelta(minutes=40):
        ids.append((make_gen(gens, n, t), t))
        n += 1
        t += dt.timedelta(hours=4)
    write_current(root, ids[-1][0], ids[-2][0])
    cand = gens / ".candidate-0123abcd"
    cand.mkdir()
    (cand / "serving.duckdb").write_bytes(b"half-written")
    yield root, ids
    for p in sorted(root.rglob("*"), reverse=True):  # let pytest clean read-only dirs
        if p.is_dir() and not p.is_symlink():
            os.chmod(p, 0o700)


def listing(root: Path) -> set[str]:
    return {p.name for p in (root / "generations").iterdir()}


def test_cutoff_is_natural_days_shanghai() -> None:
    c = ret.cutoff_for(NOW, 3)
    assert c.astimezone(ret.TZ).isoformat() == "2026-10-18T00:00:00+08:00"
    # just after local midnight the window moves with the local date, not UTC
    late = dt.datetime(2026, 10, 20, 16, 5, tzinfo=UTC)  # 10-21 00:05 Shanghai
    assert ret.cutoff_for(late, 3).astimezone(ret.TZ).date().isoformat() == "2026-10-18"


def test_dry_run_lists_old_and_changes_nothing(serving, tmp_path: Path) -> None:
    root, ids = serving
    before = listing(root)
    cur_bytes = (root / "current.json").read_bytes()
    out = tmp_path / "list.tsv"
    rc, s = ret.run(root, dry_run=True, now=NOW, list_out=out)
    assert rc == 0
    cutoff = ret.cutoff_for(NOW, 3)
    expect = {g for g, t in ids if t < cutoff}
    assert s["delete_generations"] == len(expect) > 0
    assert {line.split("\t")[0] for line in out.read_text().splitlines()} == expect
    assert listing(root) == before and (root / "current.json").read_bytes() == cur_bytes
    assert set(s["delete_by_day"]) == {"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"}
    assert min(s["keep_by_day"]) == "2026-10-18"


def test_dry_run_is_repeatable(serving) -> None:
    root, _ = serving
    a = ret.run(root, dry_run=True, now=NOW)[1]
    b = ret.run(root, dry_run=True, now=NOW)[1]
    assert a == b


def test_execute_deletes_only_old_and_is_idempotent(serving, tmp_path: Path) -> None:
    root, ids = serving
    cutoff = ret.cutoff_for(NOW, 3)
    rc, s = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "state", batch=3)
    assert rc == 0 and s["deleted_generations"] == sum(1 for _, t in ids if t < cutoff)
    left = listing(root)
    assert ".candidate-0123abcd" in left
    assert all((g in left) == (t >= cutoff) for g, t in ids)
    assert (root / "current.json").exists() and (root / ".publish.lock").exists()
    rc2, s2 = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "state")
    assert rc2 == 0 and s2["deleted_generations"] == 0 and listing(root) == left


def test_current_previous_and_pins_survive_any_age(serving, tmp_path: Path) -> None:
    root, ids = serving
    old_cur, old_prev, pinned = ids[0][0], ids[1][0], ids[2][0]
    write_current(root, old_cur, old_prev)
    rc, s = ret.run(root, dry_run=False, now=NOW, pins={pinned}, state_dir=tmp_path / "st")
    left = listing(root)
    assert {old_cur, old_prev, pinned} <= left
    assert ids[3][0] not in left
    assert s["keep_pointer_ids"] == sorted([old_cur, old_prev])


def test_intent_present_refuses(serving, tmp_path: Path) -> None:
    root, _ = serving
    (root / "publication-intent.json").write_text(json.dumps({"expected_pointer": {"generation_id": _gid(1)}}))
    before = listing(root)
    rc, s = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "st")
    assert rc == 2 and listing(root) == before and "intent" in s["refused"]


def test_unreadable_or_young_generations_are_kept(serving, tmp_path: Path) -> None:
    root, ids = serving
    gens = root / "generations"
    bad = gens / _gid(999)
    bad.mkdir()
    (bad / "manifest.json").write_text("{not json")
    young = make_gen(gens, 1000, NOW - dt.timedelta(minutes=5))
    rc, s = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "st")
    left = listing(root)
    assert bad.name in left and young in left and s["keep_unsure"] == 1


def test_publish_lock_is_respected(serving, tmp_path: Path) -> None:
    root, _ = serving
    before = listing(root)
    fd = os.open(root / ".publish.lock", os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        rc, _ = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "st", lock_timeout_s=0.5)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert rc == 2 and listing(root) == before


def test_pointer_switch_between_plan_and_delete_is_honoured(serving, tmp_path: Path, monkeypatch) -> None:
    """current.json is re-read inside the lock before each batch."""
    root, ids = serving
    victim = ids[0][0]
    real = ret.publish_lock

    import contextlib

    @contextlib.contextmanager
    def switching_lock(r, t):
        write_current(r, victim, ids[-1][0])  # e.g. a compensation rolled current back
        with real(r, t):
            yield

    monkeypatch.setattr(ret, "publish_lock", switching_lock)
    ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "st")
    assert victim in listing(root)


def test_archive_uploads_verifies_then_deletes_and_fetch_restores(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    cutoff = ret.cutoff_for(NOW, 3)
    old = [(g, t) for g, t in ids if t < cutoff]
    snapshot = {g: (root / "generations" / g / "serving.duckdb").read_bytes() for g, _ in old}
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 0
    assert s["deleted_generations"] == len(old)
    # every settled generation archived, also the kept ones (today included)
    assert s["archived_generations"] == len(ids)
    days = {k.split("/")[1] for k in store.objects}
    assert "2026-10-21" in days and "2026-10-13" in days
    assert all(k.endswith((".tar.zst", ".tar.gz", "index.json")) for k in store.objects)
    # fetch one deleted generation by id (via local catalog) and a whole day
    dest = tmp_path / "restore"
    got = fetchmod.fetch(store, dest, generation=old[0][0], state_dir=tmp_path / "st")
    assert got == [old[0][0]]
    assert (dest / old[0][0] / "serving.duckdb").read_bytes() == snapshot[old[0][0]]
    day = old[-1][1].astimezone(ret.TZ).date().isoformat()
    got_day = fetchmod.fetch(store, tmp_path / "r2", date=day)
    assert set(got_day) == {g for g, t in ids if t.astimezone(ret.TZ).date().isoformat() == day}
    # without catalog: scan back by date
    got_scan = fetchmod.fetch(store, tmp_path / "r3", generation=old[1][0], state_dir=None,
                              today=NOW.astimezone(ret.TZ).date())
    assert got_scan == [old[1][0]]


def test_archive_rerun_does_not_reupload(serving, tmp_path: Path) -> None:
    root, _ = serving
    store = MemStore()
    ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    n = len(store.puts)
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 0 and len(store.puts) == n and s["deleted_generations"] == 0


def test_upload_failure_keeps_that_day_and_exits_1(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    store.fail_put.add("/2026-10-14/2026-10-14-")
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    left = listing(root)
    day14 = [g for g, t in ids if t.astimezone(ret.TZ).date().isoformat() == "2026-10-14"]
    day13 = [g for g, t in ids if t.astimezone(ret.TZ).date().isoformat() == "2026-10-13"]
    assert rc == 1
    assert all(g in left for g in day14)
    assert not any(g in left for g in day13)


def test_verify_mismatch_keeps_and_exits_1(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    store.lie_head.add("/2026-10-15/2026-10-15-")
    rc, _ = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    left = listing(root)
    assert rc == 1
    assert all(g in left for g, t in ids if t.astimezone(ret.TZ).date().isoformat() == "2026-10-15")


def test_corrupt_local_generation_is_not_archived_nor_deleted(serving, tmp_path: Path) -> None:
    root, ids = serving
    victim = ids[0][0]
    d = root / "generations" / victim
    os.chmod(d, 0o700)
    os.chmod(d / "serving.duckdb", 0o600)
    (d / "serving.duckdb").write_bytes(b"bitrot")
    os.chmod(d, 0o500)
    store = MemStore()
    rc, _ = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 1 and victim in listing(root)


def test_archive_without_store_refuses(serving, tmp_path: Path) -> None:
    root, _ = serving
    before = listing(root)
    rc, _ = ret.run(root, dry_run=False, now=NOW, archive=True, store=None, state_dir=tmp_path / "st")
    assert rc == 2 and listing(root) == before


def test_cli_requires_explicit_mode(serving) -> None:
    root, _ = serving
    assert ret.main(["--root", str(root)]) == 2
    assert ret.main(["--root", str(root), "--dry-run", "--now", NOW.isoformat()]) == 0


def test_concurrent_publisher_candidate_is_never_touched(serving, tmp_path: Path) -> None:
    root, _ = serving
    stop = threading.Event()
    gens = root / "generations"

    def publisher() -> None:
        i = 0
        while not stop.is_set():
            c = gens / f".candidate-{i:08x}"
            c.mkdir()
            (c / "serving.duckdb").write_bytes(b"x")
            time.sleep(0.001)
            shutil.rmtree(c)
            i += 1

    t = threading.Thread(target=publisher)
    t.start()
    try:
        rc, _ = ret.run(root, dry_run=False, now=NOW, state_dir=tmp_path / "st")
    finally:
        stop.set()
        t.join()
    assert rc == 0 and ".candidate-0123abcd" in listing(root)


def test_gzip_fallback_when_no_zstd(serving, tmp_path: Path, monkeypatch) -> None:
    root, ids = serving
    monkeypatch.setattr(ret.shutil, "which", lambda name: None)
    store = MemStore()
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 0 and s["deleted_generations"] > 0
    assert any(k.endswith(".tar.gz") for k in store.objects)
    got = fetchmod.fetch(store, tmp_path / "r", generation=ids[0][0], state_dir=tmp_path / "st")
    assert got == [ids[0][0]]


# ---------------------------------------------------------------- 2026-10-20 additions
import random  # noqa: E402

DAY = lambda t: t.astimezone(ret.TZ).date().isoformat()  # noqa: E731


def test_spot_check_failure_deletes_nothing(serving, tmp_path: Path) -> None:
    """Upload works, but what comes back from R2 differs -> not a single file deleted."""
    root, ids = serving
    before = listing(root)
    store = MemStore()
    store.corrupt_get.add(".tar.")
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store,
                    state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 1 and listing(root) == before
    assert s["deleted_generations"] == 0 and s["spot_check"]["passed"] is False
    assert any(k.endswith(".tar.zst") or k.endswith(".tar.gz") for k in store.objects)


def test_spot_check_covers_early_middle_late_and_matches(serving, tmp_path: Path, caplog) -> None:
    root, ids = serving
    store = MemStore()
    with caplog.at_level("INFO"):
        rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, delete=False,
                        state_dir=tmp_path / "st", verify_sample=10, rng=random.Random(7))
    assert rc == 0 and s["spot_check"] == {"passed": True, "files": s["spot_check"]["files"],
                                          "matched": s["spot_check"]["files"]}
    assert s["spot_check"]["files"] >= 10
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("SPOTCHECK 20")]
    days = sorted({ln.split()[1] for ln in lines})
    all_days = sorted({DAY(t) for _, t in ids})
    assert all_days[0] in days and all_days[len(all_days) // 2] in days and all_days[-1] in days
    assert all(ln.endswith(" OK") for ln in lines)


def test_pick_samples_includes_every_prune_day() -> None:
    gens = [ret.Generation(_gid(i), Path("/x"), dt.datetime(2026, 10, 13 + i // 3, 1, tzinfo=UTC), "a", 1, 0)
            for i in range(24)]
    picks = ret.pick_samples(gens, {"2026-10-14", "2026-10-15"}, 10, random.Random(1))
    days = {g.local_day for g, _ in picks}
    assert len(picks) >= 10 and {"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-17", "2026-10-20"} <= days
    assert len({(g.gen_id, n) for g, n in picks}) == len(picks)


def test_no_delete_uploads_everything_and_deletes_nothing(serving, tmp_path: Path) -> None:
    root, ids = serving
    before = listing(root)
    store = MemStore()
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store, delete=False,
                    state_dir=tmp_path / "st", verify_sample=10)
    assert rc == 0 and listing(root) == before and s["archived_generations"] == len(ids)


def test_only_deletes_older_than_three_days(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store,
                    state_dir=tmp_path / "st", verify_sample=10)
    left = listing(root)
    gone = [(g, t) for g, t in ids if g not in left]
    assert rc == 0 and gone
    assert all(NOW - t >= dt.timedelta(hours=72) for _, t in gone)
    assert all(g in left for g, t in ids if NOW - t < dt.timedelta(hours=72))


def test_hard_floor_72h_even_with_keep_days_0(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    rc, s = ret.run(root, dry_run=False, now=NOW, keep_days=0, archive=True, store=store,
                    state_dir=tmp_path / "st", verify_sample=10)
    left = listing(root)
    assert all(g in left for g, t in ids if NOW - t < dt.timedelta(hours=72))
    assert any(g not in left for g, t in ids)


def test_size_mismatch_in_r2_index_keeps_that_generation(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    ret.run(root, dry_run=False, now=NOW, archive=True, store=store, delete=False,
            state_dir=tmp_path / "st", verify_sample=10)
    victim, t = ids[0]
    ikey = ret.index_key(ret.DEFAULT_PREFIX, DAY(t))
    idx = json.loads(store.objects[ikey][0])
    for b in idx["bundles"]:
        for e in b["generations"]:
            if e["id"] == victim:
                e["files"]["serving.duckdb"] += 1
    raw = json.dumps(idx).encode()
    store.objects[ikey] = (raw, hashlib.sha256(raw).hexdigest())
    rc, s = ret.run(root, dry_run=False, now=NOW, archive=True, store=store,
                    state_dir=tmp_path / "st", verify_sample=10)
    left = listing(root)
    assert victim in left
    assert ids[1][0] not in left  # its neighbours were confirmed and deleted


def test_confirm_in_r2_rejects_missing_bundle(serving, tmp_path: Path) -> None:
    root, ids = serving
    store = MemStore()
    ret.run(root, dry_run=False, now=NOW, archive=True, store=store, delete=False,
            state_dir=tmp_path / "st", verify_sample=10)
    plan = ret.build_plan(root, now=NOW, keep_days=3, keep_min=2, min_age_minutes=30, pins=set())
    day = plan.delete[0].local_day
    for k in [k for k in store.objects if f"/{day}/{day}-" in k]:
        del store.objects[k]
    (tmp_path / "st").mkdir(exist_ok=True)
    ok = ret.confirm_in_r2(store, ret.DEFAULT_PREFIX, plan.delete, tmp_path / "st")
    assert ok and not any(g.local_day == day for g in plan.delete if g.gen_id in ok)
    assert all(g.gen_id in ok for g in plan.delete if g.local_day != day)


def test_put_retries_then_succeeds(serving, tmp_path: Path) -> None:
    root, _ = serving
    store = MemStore()
    store.fail_put_once.add("/2026-10-13/2026-10-13-")
    import serving_generations_retention as m
    m_put = m.put_verified
    m.put_verified = lambda st, k, src: m_put(st, k, src, backoff_s=0)
    try:
        rc, _ = ret.run(root, dry_run=False, now=NOW, archive=True, store=store,
                        state_dir=tmp_path / "st", verify_sample=10)
    finally:
        m.put_verified = m_put
    assert rc == 0 and any("/2026-10-13/2026-10-13-" in k for k in store.objects)


def test_credentials_file_must_be_0600_and_values_never_leak(tmp_path: Path) -> None:
    secret = "S3CR3T-value-should-not-appear"
    f = tmp_path / "r2.env"
    f.write_text(f"# x\nR2_ARCHIVE_ACCESS_KEY_ID=AKID-xyz\nR2_ARCHIVE_SECRET_ACCESS_KEY={secret}\n"
                 "R2_ARCHIVE_ENDPOINT=https://acct.r2.cloudflarestorage.com\nR2_ARCHIVE_BUCKET=rquant-published-archive\n")
    os.chmod(f, 0o644)
    with pytest.raises(SystemExit) as e:
        ret.load_credentials(f)
    assert "0600" in str(e.value) and secret not in str(e.value)
    os.chmod(f, 0o600)
    c = ret.store_from_credentials(f, "rquant-published-archive")
    assert c.bucket == "rquant-published-archive" and secret not in repr(c) and secret not in str(vars(c).keys())
    with pytest.raises(SystemExit) as e2:
        ret.store_from_credentials(f, "other-bucket")
    assert secret not in str(e2.value)
    g = tmp_path / "bad.env"
    g.write_text(f"R2_ARCHIVE_SECRET_ACCESS_KEY={secret}\n")
    os.chmod(g, 0o600)
    with pytest.raises(SystemExit) as e3:
        ret.load_credentials(g)
    assert "R2_ARCHIVE_ACCESS_KEY_ID" in str(e3.value) and secret not in str(e3.value)


def test_cli_refuses_now_with_execute_and_archive_without_credentials(serving) -> None:
    root, _ = serving
    assert ret.main(["--root", str(root), "--execute", "--now", NOW.isoformat()]) == 2
    assert ret.main(["--root", str(root), "--execute", "--archive"]) == 2
