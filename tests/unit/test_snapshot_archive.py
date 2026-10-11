from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

from rquant.research import snapshot_archive as sa


class FakeR2:
    bucket = "rquant-snapshots"

    def __init__(self):
        self.objs: dict[str, bytes] = {}

    def put_file(self, key, src: Path, sha):
        data = src.read_bytes()
        assert hashlib.sha256(data).hexdigest() == sha
        self.objs[key] = data

    def head(self, key):
        d = self.objs.get(key)
        return None if d is None else {"size": len(d), "sha256": hashlib.sha256(d).hexdigest()}

    def get_file(self, key, dest: Path):
        dest.write_bytes(self.objs[key])

    def delete(self, key):
        self.objs.pop(key, None)


def _day(root: Path, name: str) -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "quotes.parquet").write_bytes(b"x" * 100)
    return d


def test_archive_prune_fetch(tmp_path, monkeypatch):
    monkeypatch.setattr(sa, "collector_active", lambda: False)
    root = tmp_path / "snapshots"
    old, recent, unverified = (_day(root, "2026-09-01"), _day(root, "2026-10-09"),
                               _day(root, "2026-09-02"))
    (root / "STOP").write_text("")
    r2 = FakeR2()
    # pretend 09-02 was archived but the object vanished -> must NOT be pruned
    (unverified / sa.MARKER).write_text(json.dumps(
        {"key": sa.object_key("2026-09-02"), "size": 1, "sha256": "bad"}))
    assert sa.run(root, 14, today=dt.date(2026, 10, 12), client=r2) == 0
    assert set(r2.objs) == {sa.object_key("2026-09-01"), sa.object_key("2026-10-09")}
    assert not old.exists() and recent.exists() and unverified.exists()
    assert (recent / sa.MARKER).exists()
    out = sa.fetch("2026-09-01", tmp_path / "back", client=r2)
    assert (out / "quotes.parquet").read_bytes() == b"x" * 100


def test_today_skipped_while_collector_runs(tmp_path, monkeypatch):
    monkeypatch.setattr(sa, "collector_active", lambda: True)
    root = tmp_path / "snapshots"
    _day(root, "2026-10-12")
    r2 = FakeR2()
    sa.run(root, 14, today=dt.date(2026, 10, 12), client=r2)
    assert r2.objs == {}


def test_failed_verify_keeps_unmarked(tmp_path, monkeypatch):
    monkeypatch.setattr(sa, "collector_active", lambda: False)
    root = tmp_path / "snapshots"
    d = _day(root, "2026-09-01")
    r2 = FakeR2()
    r2.head = lambda key: None
    assert sa.run(root, 14, today=dt.date(2026, 10, 12), client=r2) == 1
    assert d.exists() and not (d / sa.MARKER).exists()


def test_sigv4_known_shape():
    c = sa.R2("https://acct.r2.cloudflarestorage.com", "b", "AK", "SK")
    captured = {}

    class Resp:
        def close(self):
            pass

    import urllib.request
    orig = urllib.request.urlopen
    urllib.request.urlopen = lambda req, timeout: captured.setdefault("r", req) and Resp()
    try:
        c._request("DELETE", "snapshots/x", now=dt.datetime(2026, 1, 1, tzinfo=dt.UTC))
    finally:
        urllib.request.urlopen = orig
    auth = captured["r"].headers["Authorization"]
    assert auth.startswith("AWS4-HMAC-SHA256 Credential=AK/20260101/auto/s3/aws4_request")
    assert captured["r"].full_url == "https://acct.r2.cloudflarestorage.com/b/snapshots/x"
