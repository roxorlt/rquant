from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

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
    sa.run(root, 14, today=dt.date(2026, 10, 12), client=r2, wait_s=0)
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


def test_wait_for_collector_tmp(tmp_path, monkeypatch):
    from rquant.research import snapshot_archive as sa

    d = tmp_path / "2026-10-12"
    d.mkdir()
    (d / "quotes.tmp").write_text("x")
    monkeypatch.setattr(sa, "collector_active", lambda: False)
    assert not sa._ready(d, "2026-10-12")
    assert sa.wait_for_collector(d, wait_s=0, poll_s=0) is False
    calls = []
    monkeypatch.setattr(sa.time, "sleep", lambda s: (calls.append(s), (d / "quotes.tmp").unlink()))
    assert sa.wait_for_collector(d, wait_s=60, poll_s=1) is True
    assert calls == [1]


def test_r2_bounded_get_closes_and_removes_oversize_partial(
    tmp_path: Path, monkeypatch: Any
) -> None:
    import io

    import pytest

    response = io.BytesIO(b"x" * 101)
    monkeypatch.setattr(sa.R2, "_request", lambda *a, **kw: response)
    dest = tmp_path / "bounded"
    with pytest.raises(ValueError):
        sa.R2("https://synthetic.invalid", "b", "k", "s").get_file("x", dest, maximum_bytes=100)
    assert response.closed and not dest.exists()


def test_r2_conditional_headers_and_optional_head_etag(tmp_path: Path, monkeypatch: Any) -> None:
    import io

    import pytest

    captured = []

    class Response(io.BytesIO):
        headers = {"Content-Length": "3", "x-amz-meta-sha256": "a" * 64, "ETag": '"opaque"'}

    def request(self: Any, method: str, key: str, **kwargs: Any) -> Any:
        captured.append((method, kwargs))
        return Response(b"abc")

    monkeypatch.setattr(sa.R2, "_request", request)
    client = sa.R2("https://synthetic.invalid", "b", "k", "s")
    src = tmp_path / "src"
    src.write_bytes(b"abc")
    client.put_file("x", src, "a" * 64, if_none_match="*")
    assert captured[-1][1]["headers"]["if-none-match"] == "*"
    client.put_file("x", src, "a" * 64, if_match='"opaque"')
    assert captured[-1][1]["headers"]["if-match"] == '"opaque"'
    assert client.head("x") == {"size": 3, "sha256": "a" * 64}
    assert client.head("x", include_etag=True)["etag"] == '"opaque"'
    with pytest.raises(ValueError):
        client.put_file("x", src, "a" * 64, if_match="x", if_none_match="*")


def test_r2_bounded_and_conditional_http_errors_close_response(
    tmp_path: Path, monkeypatch: Any
) -> None:
    import io
    import urllib.error

    import pytest

    handles = []
    errors = []

    def request(*args: Any, **kwargs: Any) -> Any:
        handle = io.BytesIO(b"synthetic refusal")
        handles.append(handle)
        error = urllib.error.HTTPError("synthetic", 412, "conflict", {}, handle)
        errors.append(error)
        raise error

    monkeypatch.setattr(sa.R2, "_request", request)
    client = sa.R2("https://synthetic.invalid", "b", "k", "s")
    src = tmp_path / "src"
    src.write_bytes(b"abc")
    for call in (
        lambda: client.put_file("x", src, "a" * 64, if_none_match="*"),
        lambda: client.get_file("x", tmp_path / "dest", maximum_bytes=3),
        lambda: client.head("x", include_etag=True),
    ):
        with pytest.raises(urllib.error.HTTPError):
            call()
    assert all(handle.closed for handle in handles)
