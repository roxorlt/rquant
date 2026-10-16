from __future__ import annotations

import builtins
import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from tests.unit.test_l2_cache import observation


def test_existing_app_opt_in_reads_cache_without_upstream(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus
    from rquant.web.app import create_app

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        cache.write_status(FeedStatus(state="seeded_waiting"))

    def forbidden(*args, **kwargs):
        pytest.fail("cached HTTP attempted upstream IO")

    original = builtins.__import__

    def imports(name, *args, **kwargs):
        assert name != "rquant.l2.feed", "HTTP imported the collector"
        return original(name, *args, **kwargs)

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(builtins, "__import__", imports)
    app = create_app(source=object(), dist=None, l2_cache_root=cache.root)
    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: client.get("/api/v1/l2/batches"), range(16)))
        assert all(result.status_code == 200 for result in results)
        payload = results[0].json()
        assert payload["next_sequence"] == 1
        assert payload["batches"][0]["table"]["rows"][0]["13"]["value"] == "2"
        status = client.get("/api/v1/l2/status").json()
        assert status["subscription_state"] == "requested_unverified"
        assert status["last_market_received_at"] == "2026-10-15T07:00:00Z"
        for params in ({"after_sequence": -1}, {"limit": 0}, {"limit": 9}):
            assert client.get("/api/v1/l2/batches", params=params).status_code == 422


def test_app_l2_registration_is_opt_in_and_empty_cache_creates_nothing(tmp_path: Path, monkeypatch):
    from rquant.web.app import create_app

    monkeypatch.delenv("RQUANT_L2_RAW_ROOT", raising=False)
    with TestClient(create_app(source=object(), dist=None)) as client:
        assert client.get("/api/v1/l2/status").status_code == 404
    root = tmp_path / "absent"
    monkeypatch.setenv("RQUANT_L2_RAW_ROOT", str(root))
    with TestClient(create_app(source=object(), dist=None)) as client:
        assert client.get("/api/v1/l2/status").json()["state"] == "unconfigured"
        assert client.get("/api/v1/l2/batches").json()["batches"] == []
    assert not root.exists()


def test_cache_error_response_is_fixed_and_contains_no_payload(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.web.app import create_app

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
    path = cache.root / "batches" / f"{1:020d}" / "batch.json"
    path.write_text("synthetic_auth_secret")
    with TestClient(create_app(source=object(), dist=None, l2_cache_root=cache.root)) as client:
        response = client.get("/api/v1/l2/batches")
    assert response.status_code == 503
    assert response.json() == {"detail": {"code": "invalid_batch"}}
    assert "synthetic_auth_secret" not in response.text


def test_future_cursor_is_rejected_without_skipping_later_batches(tmp_path: Path):
    from rquant.l2.cache import RawL2Cache
    from rquant.web.app import create_app

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        with pytest.raises(ValueError):
            cache.read_batches(after_sequence=2)
        with TestClient(create_app(source=object(), dist=None, l2_cache_root=cache.root)) as client:
            result = client.get("/api/v1/l2/batches?after_sequence=2")
            assert result.status_code == 400
            assert result.json() == {"detail": {"code": "invalid_cursor"}}
            cache.append(observation())
            page = client.get("/api/v1/l2/batches?after_sequence=1").json()
            assert page["batches"][0]["local_batch_sequence"] == 2


def test_collector_cli_is_lazy_and_passes_signal_event(tmp_path: Path, monkeypatch, capsys):
    from rquant.l2.__main__ import main
    from rquant.l2.models import FeedStatus

    calls = []

    class Collector:
        def __init__(self, cache, **kwargs):
            calls.append((cache, kwargs))

        def run(self):
            return FeedStatus(state="stopped", error_code="duration_limit")

    monkeypatch.setitem(sys.modules, "rquant.l2.feed", SimpleNamespace(FeedCollector=Collector))
    args = ["--cache-root", str(tmp_path / "cache"), "--bootstrap-file", str(tmp_path / "private"),
            "--order-template", str(tmp_path / "orders"), "--trade-template", str(tmp_path / "trades"),
            "--duration-seconds", "1"]
    assert main(args) == 0
    assert len(calls) == 1
    options = calls[0][1]
    assert options["max_reconnects"] == 0
    assert options["reconnect_delay_seconds"] >= 60
    assert options["bootstrap_path"] == tmp_path / "private"
    assert options["stop_event"].is_set() is False
    assert json.loads(capsys.readouterr().out)["state"] == "stopped"
    assert not (tmp_path / "cache").exists()


def test_cli_exception_text_and_private_inputs_are_never_printed(tmp_path: Path, monkeypatch, capsys):
    from rquant.l2.__main__ import main

    class Collector:
        def __init__(self, *args, **kwargs):
            pass

        def run(self):
            raise RuntimeError("synthetic_auth_secret")

    monkeypatch.setitem(sys.modules, "rquant.l2.feed", SimpleNamespace(FeedCollector=Collector))
    args = ["--cache-root", str(tmp_path / "cache"), "--bootstrap-file", "private",
            "--order-template", "orders", "--trade-template", "trades"]
    assert main(args) == 1
    captured = capsys.readouterr()
    assert "synthetic_auth_secret" not in captured.out + captured.err
    assert json.loads(captured.out) == {"error_code": "collector_error"}
