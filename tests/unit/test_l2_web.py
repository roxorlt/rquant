from __future__ import annotations

import builtins
import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from tests.unit.test_l2_cache import observation
from tests.unit.test_l2_codec import module


def test_standalone_api_preserves_cache_cursor_concurrent_reads_and_errors(tmp_path: Path, monkeypatch):
    from rquant.l2.cache import RawL2Cache

    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.append(observation())
        cache.append(observation())
    before = {p.relative_to(cache.root): p.read_bytes() for p in cache.root.rglob("*") if p.is_file()}

    def forbidden(*args, **kwargs):
        pytest.fail("standalone cache API attempted upstream IO")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    app = module("api").create_app(cache.root)
    assert {path: set(methods) for path, methods in app.openapi()["paths"].items()} == {
        "/api/v1/l2/status": {"get"}, "/api/v1/l2/batches": {"get"}}
    with TestClient(app) as client:
        first = client.get("/api/v1/l2/batches?limit=1").json()
        assert first["next_sequence"] == 1 and len(first["batches"]) == 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda _: client.get("/api/v1/l2/batches?after_sequence=1"), range(12)))
        assert all(r.status_code == 200 and r.json()["next_sequence"] == 2 for r in responses)
        assert all(r.json()["batches"][0]["local_batch_sequence"] == 2 for r in responses)
        assert client.get("/api/v1/l2/status").json()["last_sequence"] == 2
        assert client.get("/api/v1/l2/batches?after_sequence=2").json()["batches"] == []
        assert client.get("/api/v1/l2/batches?after_sequence=3").json() == {
            "detail": {"code": "invalid_cursor"}}
        assert client.get("/api/v1/l2/batches?after_sequence=3").status_code == 400
        for query in ("after_sequence=-1", "limit=0", "limit=129"):
            assert client.get("/api/v1/l2/batches?" + query).status_code == 422
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert client.get(path).status_code == 404
        assert client.post("/api/v1/l2/status").status_code == 405
        assert before == {p.relative_to(cache.root): p.read_bytes() for p in cache.root.rglob("*") if p.is_file()}
        (cache.root / "batches" / f"{1:020d}" / "batch.json").write_text("synthetic_auth_secret")
        response = client.get("/api/v1/l2/batches")
        assert response.status_code == 503 and response.json() == {"detail": {"code": "invalid_batch"}}
        assert "synthetic_auth_secret" not in response.text


def test_standalone_api_cold_import_never_loads_settings_db_feed_or_private_inputs(tmp_path: Path):
    assert importlib.util.find_spec("rquant.l2.api") is not None
    script = textwrap.dedent('''
        import builtins, importlib.abc, socket, sys
        from pathlib import Path
        class Guard(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                assert fullname not in {"rquant.config", "rquant.web.app", "rquant.web.source", "rquant.l2.feed"}
                assert fullname.split(".")[0] not in {"duckdb", "dotenv", "pydantic_settings"}
                return None
        sys.meta_path.insert(0, Guard())
        original_open = builtins.open
        def guarded_open(file, *args, **kwargs):
            assert not str(file).endswith((".env", ".duckdb", "bootstrap.bin", "order-template.bin", "trade-template.bin"))
            return original_open(file, *args, **kwargs)
        def forbidden(*args, **kwargs):
            raise AssertionError("API attempted network")
        builtins.open = guarded_open
        socket.create_connection = forbidden
        socket.socket.connect = forbidden
        from rquant.l2.api import create_app
        from fastapi.testclient import TestClient
        root = Path(sys.argv[1])
        with TestClient(create_app(root)) as client:
            assert client.get("/api/v1/l2/status").json()["state"] == "unconfigured"
            assert client.get("/api/v1/l2/batches").json()["batches"] == []
        assert not root.exists()
    ''')
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "absent")],
                            cwd=tmp_path, env={"PATH": os.defpath,
                            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
                            "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_standalone_api_cli_uses_only_fixed_loopback_port(tmp_path: Path, monkeypatch):
    api = module("api")
    calls = []
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=lambda app, **kwargs: calls.append((app, kwargs))))
    root = tmp_path / "absent"
    assert api.main(["--cache-root", str(root)]) == 0
    assert len(calls) == 1 and calls[0][1] == {"host": "127.0.0.1", "port": 8771}
    assert not root.exists()
    for extra in (["--host", "0.0.0.0"], ["--port", "9000"]):
        with pytest.raises(SystemExit) as error:
            api.main(["--cache-root", str(root), *extra])
        assert error.value.code == 2
    assert len(calls) == 1


@pytest.mark.parametrize("resident", [False, True])
def test_collector_cli_default_or_resident_sigterm_and_handler_restoration(tmp_path: Path, monkeypatch, capsys,
                                                                         resident: bool):
    from rquant.l2.__main__ import main
    from rquant.l2.models import FeedStatus

    calls = []
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    class Collector:
        def __init__(self, cache, **kwargs):
            calls.append(kwargs)
        def run(self):
            signal.raise_signal(signal.SIGTERM)
            assert calls[0]["stop_event"].is_set()
            return FeedStatus(state="stopped", error_code="signal_stop")
    monkeypatch.setitem(sys.modules, "rquant.l2.feed", SimpleNamespace(FeedCollector=Collector))
    args = ["--cache-root", str(tmp_path / "cache"), "--bootstrap-file", "synthetic-bootstrap",
            "--order-template", "synthetic-order", "--trade-template", "synthetic-trade"]
    assert main(args + (["--resident"] if resident else [])) == 0
    assert calls[0]["duration_seconds"] == (None if resident else 3600)
    assert calls[0]["max_reconnects"] == 0
    assert all(signal.getsignal(sig) == handler for sig, handler in previous.items())
    assert json.loads(capsys.readouterr().out)["error_code"] == "signal_stop"
    assert not (tmp_path / "cache").exists()


@pytest.mark.parametrize("options", [
    ["--resident", "--duration-seconds", "1"], ["--duration-seconds", "nan"],
    ["--duration-seconds", "inf"], ["--duration-seconds=-inf"], ["--duration-seconds", "0"],
    ["--duration-seconds=-1"], ["--duration-seconds", "21601"],
])
def test_collector_cli_rejects_ambiguous_or_invalid_runtime(tmp_path: Path, options):
    from rquant.l2.__main__ import main
    args = ["--cache-root", str(tmp_path / "cache"), "--bootstrap-file", "synthetic-bootstrap",
            "--order-template", "synthetic-order", "--trade-template", "synthetic-trade"]
    with pytest.raises(SystemExit) as error:
        main(args + options)
    assert error.value.code == 2 and not (tmp_path / "cache").exists()


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
        for params in ({"after_sequence": -1}, {"limit": 0}, {"limit": 129}):
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
