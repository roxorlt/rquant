from __future__ import annotations

import datetime as dt
import hashlib
import http.client
import io
import json
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from rquant.research import snapshot_archive as sa


class Response(io.BytesIO):
    """Minimal urlopen() replacement for error/retry tests."""

    def __init__(self, data: bytes = b"", headers: dict[str, str] | None = None) -> None:
        super().__init__(data)
        self.headers = headers or {}


def _client(**kwargs: Any) -> sa.R2:
    return sa.R2("https://synthetic.invalid", "b", "k", "s", **kwargs)


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


def test_r2_retries_transient_failures_with_exponential_backoff(monkeypatch: Any) -> None:
    calls: list[float] = []
    sleeps: list[float] = []

    def urlopen(request: Any, timeout: float) -> Response:
        calls.append(timeout)
        if len(calls) < 3:
            raise TimeoutError("synthetic timeout")
        return Response(b"", {"Content-Length": "3", "x-amz-meta-sha256": "a" * 64})

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", sleeps.append)
    client = _client()
    assert client.head("x") == {"size": 3, "sha256": "a" * 64}
    assert len(calls) == 3 and sleeps == [1.0, 2.0]
    assert calls == [client.timeout_s, client.timeout_s, client.timeout_s]


def test_r2_does_not_retry_4xx(monkeypatch: Any) -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def urlopen(request: Any, timeout: float) -> Response:
        calls.append(1)
        raise urllib.error.HTTPError(request.full_url, 403, "denied", {}, io.BytesIO(b""))

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", sleeps.append)
    with pytest.raises(urllib.error.HTTPError) as info:
        _client().head("x")
    assert info.value.code == 403 and len(calls) == 1 and sleeps == []


def test_r2_retries_5xx(monkeypatch: Any) -> None:
    calls: list[int] = []

    def urlopen(request: Any, timeout: float) -> Response:
        calls.append(1)
        if len(calls) == 1:
            raise urllib.error.HTTPError(request.full_url, 503, "slow down", {}, io.BytesIO(b""))
        return Response(b"", {"Content-Length": "0", "x-amz-meta-sha256": "a" * 64})

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", lambda _seconds: None)
    assert _client().head("x") == {"size": 0, "sha256": "a" * 64}
    assert len(calls) == 2


def test_r2_retry_exhaustion_uses_five_attempts_and_full_backoff(monkeypatch: Any) -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def urlopen(request: Any, timeout: float) -> Response:
        calls.append(1)
        raise ConnectionResetError("synthetic reset")

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", sleeps.append)
    with pytest.raises(ConnectionResetError, match="synthetic reset"):
        _client().head("x")
    assert len(calls) == sa.DEFAULT_ATTEMPTS == 5
    assert sleeps == [1.0, 2.0, 4.0, 8.0]


def test_r2_put_retry_rewinds_the_file_body(tmp_path: Path, monkeypatch: Any) -> None:
    payload = b"x" * 4096
    src = tmp_path / "src"
    src.write_bytes(payload)
    bodies: list[bytes] = []

    def urlopen(request: Any, timeout: float) -> Response:
        bodies.append(request.data.read())
        if len(bodies) == 1:
            raise TimeoutError("synthetic timeout")
        return Response()

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", lambda _seconds: None)
    _client(attempts=2, backoff_s=(0.0,)).put_file(
        "x", src, hashlib.sha256(payload).hexdigest()
    )
    assert bodies == [payload, payload]


def test_r2_conditional_put_ambiguity_surfaces_transport_error(
    tmp_path: Path, monkeypatch: Any
) -> None:
    src = tmp_path / "src"
    src.write_bytes(b"abc")
    calls: list[int] = []

    def urlopen(request: Any, timeout: float) -> Response:
        calls.append(1)
        if len(calls) == 1:
            raise TimeoutError("unknown outcome")
        raise urllib.error.HTTPError(request.full_url, 412, "precondition", {}, io.BytesIO(b""))

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", lambda _seconds: None)
    with pytest.raises(TimeoutError, match="unknown outcome"):
        _client(attempts=3, backoff_s=(0.0, 0.0)).put_file(
            "x", src, hashlib.sha256(b"abc").hexdigest(), if_none_match="*"
        )
    assert len(calls) == 2


def test_r2_get_retries_interrupted_response_body(
    tmp_path: Path, monkeypatch: Any
) -> None:
    payload = b"z" * 1000

    class Broken(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            raise http.client.IncompleteRead(b"partial")

    responses = [Broken(payload), Response(payload)]

    def urlopen(request: Any, timeout: float) -> Response:
        return responses.pop(0)

    monkeypatch.setattr(sa.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sa.time, "sleep", lambda _seconds: None)
    dest = tmp_path / "out"
    _client(attempts=2, backoff_s=(0.0,)).get_file("x", dest)
    assert dest.read_bytes() == payload


def test_r2_get_exhaustion_removes_partial_download(
    tmp_path: Path, monkeypatch: Any
) -> None:
    class Broken(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            raise ConnectionResetError("synthetic reset")

    monkeypatch.setattr(sa.urllib.request, "urlopen", lambda request, timeout: Broken(b"abc"))
    monkeypatch.setattr(sa.time, "sleep", lambda _seconds: None)
    dest = tmp_path / "out"
    with pytest.raises(ConnectionResetError):
        _client(attempts=2, backoff_s=(0.0,)).get_file("x", dest)
    assert not dest.exists()


def test_r2_timeout_is_configurable_via_environment(monkeypatch: Any) -> None:
    monkeypatch.delenv("R2_TIMEOUT_S", raising=False)
    assert _client().timeout_s == sa.DEFAULT_TIMEOUT_S == 600.0
    monkeypatch.setenv("R2_TIMEOUT_S", "12.5")
    assert _client().timeout_s == 12.5
    assert _client(timeout_s=3.0).timeout_s == 3.0
    monkeypatch.setenv("R2_TIMEOUT_S", "not-a-number")
    with pytest.raises(ValueError, match="R2_TIMEOUT_S"):
        _client()
