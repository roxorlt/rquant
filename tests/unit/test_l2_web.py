from __future__ import annotations

import asyncio
import builtins
from contextlib import ExitStack
import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

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
        socket.socket.connect_ex = forbidden
        from rquant.l2.api import create_app
        from fastapi.testclient import TestClient
        root = Path(sys.argv[1])
        with TestClient(create_app(root)) as client:
            assert client.get("/api/v1/l2/status").json()["state"] == "unconfigured"
            assert client.get("/api/v1/l2/batches").json()["batches"] == []
            with client.websocket_connect("/api/v1/l2/stream") as stream:
                assert stream.receive_json()["batches"] == []
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
    assert len(calls) == 1 and calls[0][1] == {"host": "127.0.0.1", "port": 8771,
                                            "ws": "websockets", "ws_max_size": 1024, "ws_max_queue": 1}
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


@pytest.fixture
def websocket_network_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail('WS attempted upstream I/O')
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)


def test_websocket_replays_then_follows_and_matches_http(tmp_path: Path, websocket_network_guard: None) -> None:
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
        app = module('api').create_app(cache.root)
        with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream?limit=1') as stream:
            for sequence in (1, 2):
                page = stream.receive_json()
                assert page['next_sequence'] == sequence
                assert page['batches'] == client.get(f'/api/v1/l2/batches?after_sequence={sequence-1}').json()['batches']
            cache.append(observation())
            page = stream.receive_json()
            assert page['next_sequence'] == 3
            assert page['batches'] == client.get('/api/v1/l2/batches?after_sequence=2').json()['batches']
    assert app.state.l2_ws_connections == 0


def test_actual_763_row_trade_push_survives_cache_http_and_websocket(
    tmp_path: Path, websocket_network_guard: None,
) -> None:
    import hashlib
    from datetime import UTC, datetime

    from rquant.l2._codec import decode_trade_response
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import Binding, MarketObservation, RawL2Batch

    body = (Path(__file__).parents[1] / 'fixtures' / 'l2_trade_push_763.bin').read_bytes()
    body_sha = 'f7c0734e1de817820745374ca311dab9ca936a52706179b89a9dd85d5f78024b'
    table_sha = '43be6a8dc34261bafa8e546c09b76ec590b021aff34687c2d6092869b59d4ffb'
    row_sha = '5b967e2cdd8a92581e0be872c9971b28955bab09dbc86bd5b8f6dd785b7f59c6'
    assert hashlib.sha256(body).hexdigest() == body_sha
    decoded = decode_trade_response(body, request_id=0xFFFFFFFF, page_id=10237,
                                    code='601899', market=17, source_fd_ordinal=4953)
    observed = MarketObservation(
        epoch_id='large_trade_push',
        binding=Binding(page_id=10237, kind='vendor4013', code='601899', market=17),
        received_at=datetime(2026, 10, 19, 6, tzinfo=UTC), request_id=0xFFFFFFFF,
        source_fd_ordinal=4953, delivery='candidate_push', response_body_hex=body.hex(),
        raw_table_hex=decoded['raw_table'].hex(), table=decoded['table'],
        response_sha256=decoded['response_sha256'],
    )
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        committed = cache.append(observed)
    expected_sha = hashlib.sha256(committed.model_dump_json().encode()).hexdigest()
    before = {p.relative_to(cache.root): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in cache.root.rglob('*') if p.is_file()}
    app = module('api').create_app(cache.root)
    with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream?limit=1') as stream:
        response = client.get('/api/v1/l2/batches?after_sequence=0&limit=1')
        assert response.status_code == 200
        for page in (response.json(), stream.receive_json()):
            assert page['next_sequence'] == 1 and len(page['batches']) == 1
            returned = RawL2Batch.model_validate(page['batches'][0])
            assert returned.table.row_count == len(returned.table.rows) == 763
            assert returned.response_sha256 == body_sha
            assert returned.table.source_sha256 == table_sha
            assert returned.table.row_data_sha256 == row_sha
            assert hashlib.sha256(returned.model_dump_json().encode()).hexdigest() == expected_sha
    assert app.state.l2_ws_connections == 0
    assert before == {p.relative_to(cache.root): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in cache.root.rglob('*') if p.is_file()}


def test_websocket_independent_cursors_and_reconnect_from_client_commit(tmp_path: Path,
                                                                     websocket_network_guard: None) -> None:
    from rquant.l2.cache import RawL2Cache
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.append(observation())
        app = module('api').create_app(cache.root)
        with TestClient(app) as client:
            with client.websocket_connect('/api/v1/l2/stream') as first, \
                    client.websocket_connect('/api/v1/l2/stream?after_sequence=1') as second:
                assert first.receive_json()['next_sequence'] == 1
                assert first.receive_json()['next_sequence'] == 2
                assert second.receive_json()['next_sequence'] == 2
                cache.append(observation())
                assert first.receive_json()['next_sequence'] == 3
                assert second.receive_json()['next_sequence'] == 3
            with client.websocket_connect('/api/v1/l2/stream?after_sequence=2') as resumed:
                page = resumed.receive_json()
                assert page['next_sequence'] == 3
                assert page['batches'] == client.get('/api/v1/l2/batches?after_sequence=2').json()['batches']
    assert app.state.l2_ws_connections == 0


@pytest.mark.parametrize('query', ['after_sequence=-1', 'after_sequence=1.5', 'after_sequence=',
    'after_sequence=' + '9'*100, 'limit=0', 'limit=129', 'limit=1&limit=2',
    'after_sequence=0&after_sequence=0', 'symbols=600000', 'limit=1&path=secret'])
def test_websocket_rejects_bad_query_before_cache_or_accept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                          query: str, websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    def forbidden(**kwargs: object) -> None:
        pytest.fail('invalid handshake touched cache')
    monkeypatch.setattr(app.state.l2_cache, 'read_batches', forbidden)
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as error:
        with client.websocket_connect('/api/v1/l2/stream?' + query):
            pytest.fail('invalid query accepted')
    assert error.value.code == 1008 and 'secret' not in error.value.reason
    assert not (tmp_path / 'absent').exists()


@pytest.mark.parametrize('origin', ['', 'null', 'https://outside.invalid', 'http://127.0.0.1:8771'])
def test_websocket_rejects_any_origin_before_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                  origin: str, websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    monkeypatch.setattr(app.state.l2_cache, 'read_batches', lambda **kwargs: pytest.fail('Origin touched cache'))
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect) as error:
        with client.websocket_connect('/api/v1/l2/stream', headers={'Origin': origin}):
            pytest.fail('browser Origin accepted')
    assert error.value.code == 1008
    assert not (tmp_path / 'absent').exists()


def test_websocket_future_cursor_and_cache_error_are_fixed(tmp_path: Path, websocket_network_guard: None) -> None:
    from rquant.l2.cache import RawL2Cache
    app = module('api').create_app(tmp_path / 'absent')
    with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream?after_sequence=1') as stream:
        message = stream.receive()
        assert message['type'] == 'websocket.close' and message['code'] == 1008
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer(): cache.append(observation())
    (cache.root / 'batches' / f'{1:020d}' / 'batch.json').write_text('synthetic_private_payload')
    app = module('api').create_app(cache.root)
    with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream') as stream:
        message = stream.receive()
        assert message['type'] == 'websocket.close' and message['code'] == 1011
        assert 'synthetic_private_payload' not in str(message) and str(cache.root) not in str(message)
    assert app.state.l2_ws_connections == 0


@pytest.mark.parametrize('payload', ['synthetic_private_input', b'synthetic_private_input'])
def test_websocket_rejects_client_application_data(tmp_path: Path, payload: str | bytes,
                                                  websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream') as stream:
        assert stream.receive_json()['batches'] == []
        if isinstance(payload, str): stream.send_text(payload)
        else: stream.send_bytes(payload)
        message = stream.receive()
        assert message['type'] == 'websocket.close' and message['code'] == 1008
        assert 'synthetic_private_input' not in str(message)
    assert app.state.l2_ws_connections == 0 and not (tmp_path / 'absent').exists()


def test_websocket_empty_heartbeat_preserves_market_receipt_and_stopped_status(tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch, websocket_network_guard: None) -> None:
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import FeedStatus
    api = module('api')
    monkeypatch.setattr(api, '_WS_HEARTBEAT_SECONDS', .02, raising=False)
    monkeypatch.setattr(api, '_WS_POLL_SECONDS', .005, raising=False)
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer():
        cache.append(observation()); cache.write_status(FeedStatus(state='stopped', gap=True))
    app = api.create_app(cache.root)
    with TestClient(app) as client, client.websocket_connect('/api/v1/l2/stream') as stream:
        first, heartbeat = stream.receive_json(), stream.receive_json()
        assert heartbeat['batches'] == [] and first['next_sequence'] == heartbeat['next_sequence'] == 1
        assert heartbeat['status']['state'] == 'stopped' and heartbeat['status']['cache_stale'] is True
        assert heartbeat['status']['gap'] is True
        assert heartbeat['status']['last_market_received_at'] == first['status']['last_market_received_at']


def test_websocket_eight_slots_and_disconnect_release(tmp_path: Path, websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    with TestClient(app) as client:
        with ExitStack() as stack:
            streams = [stack.enter_context(client.websocket_connect('/api/v1/l2/stream')) for _ in range(8)]
            for stream in streams: assert stream.receive_json()['batches'] == []
            assert app.state.l2_ws_connections == 8
            with pytest.raises(WebSocketDisconnect) as error:
                with client.websocket_connect('/api/v1/l2/stream'): pytest.fail('ninth stream accepted')
            assert error.value.code == 1013
        assert app.state.l2_ws_connections == 0
        with client.websocket_connect('/api/v1/l2/stream') as replacement:
            assert replacement.receive_json()['batches'] == []
    assert app.state.l2_ws_connections == 0


def websocket_scope() -> dict[str, Any]:
    return {'type': 'websocket', 'asgi': {'version': '3.0', 'spec_version': '2.4'}, 'scheme': 'ws',
        'path': '/api/v1/l2/stream', 'raw_path': b'/api/v1/l2/stream', 'root_path': '',
        'query_string': b'', 'headers': [], 'client': ('127.0.0.1', 12345),
        'server': ('127.0.0.1', 8771), 'subprotocols': [], 'state': {}}


def test_websocket_slow_send_isolated_and_all_tasks_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                         websocket_network_guard: None) -> None:
    from rquant.l2.cache import RawL2Cache
    api = module('api'); monkeypatch.setattr(api, '_WS_SEND_TIMEOUT_SECONDS', .03, raising=False)
    cache = RawL2Cache(tmp_path / 'cache')
    with cache.writer(): cache.append(observation())
    app = api.create_app(cache.root)

    async def scenario() -> None:
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        fast_in, fast_out = asyncio.Queue(), asyncio.Queue()
        blocked, cancelled = asyncio.Event(), asyncio.Event()
        await incoming.put({'type': 'websocket.connect'}); await fast_in.put({'type': 'websocket.connect'})
        async def slow_send(message: dict[str, Any]) -> None:
            if message['type'] == 'websocket.send':
                blocked.set()
                try: await asyncio.Event().wait()
                finally: cancelled.set()
            else: await outgoing.put(message)
        slow = asyncio.create_task(app(websocket_scope(), incoming.get, slow_send))
        fast = None
        try:
            assert (await asyncio.wait_for(outgoing.get(), 1))['type'] == 'websocket.accept'
            await asyncio.wait_for(blocked.wait(), 1)
            fast = asyncio.create_task(app(websocket_scope(), fast_in.get, fast_out.put))
            assert (await asyncio.wait_for(fast_out.get(), 1))['type'] == 'websocket.accept'
            assert json.loads((await asyncio.wait_for(fast_out.get(), 1))['text'])['next_sequence'] == 1
            closed = await asyncio.wait_for(outgoing.get(), 1)
            assert closed['type'] == 'websocket.close' and closed['code'] == 1013
            await asyncio.wait_for(slow, 1)
            assert cancelled.is_set() and not fast.done() and app.state.l2_ws_connections == 1
        finally:
            await fast_in.put({'type': 'websocket.disconnect', 'code': 1000})
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.wait_for(asyncio.gather(*(task for task in (slow, fast) if task is not None)), 2)
        assert app.state.l2_ws_connections == 0
    asyncio.run(scenario())


def test_websocket_disconnect_awaits_read_without_blocking_other_client(tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch, websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = app.state.l2_cache.read_batches
    calls = []
    def blocked_read(**kwargs: Any) -> Any:
        calls.append(kwargs)
        if len(calls) == 1:
            started.set()
            try: assert release.wait(2)
            finally: finished.set()
        return original(**kwargs)
    monkeypatch.setattr(app.state.l2_cache, 'read_batches', blocked_read)

    async def scenario() -> None:
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        fast_in, fast_out = asyncio.Queue(), asyncio.Queue()
        await incoming.put({'type': 'websocket.connect'}); await fast_in.put({'type': 'websocket.connect'})
        pending = asyncio.create_task(app(websocket_scope(), incoming.get, outgoing.put)); fast = None
        try:
            assert (await asyncio.wait_for(outgoing.get(), 1))['type'] == 'websocket.accept'
            while not started.is_set(): await asyncio.sleep(.001)
            fast = asyncio.create_task(app(websocket_scope(), fast_in.get, fast_out.put))
            assert (await asyncio.wait_for(fast_out.get(), 1))['type'] == 'websocket.accept'
            assert json.loads((await asyncio.wait_for(fast_out.get(), 1))['text'])['batches'] == []
            await fast_in.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.wait_for(fast, 1)
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.sleep(.02)
            assert not pending.done() and not finished.is_set() and app.state.l2_ws_connections == 1
        finally:
            release.set()
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await fast_in.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.wait_for(asyncio.gather(*(task for task in (pending, fast) if task is not None)), 2)
        assert finished.is_set() and app.state.l2_ws_connections == 0
    asyncio.run(scenario())


def test_websocket_direct_task_cancel_waits_for_read_and_releases_slot(tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch, websocket_network_guard: None) -> None:
    app = module('api').create_app(tmp_path / 'absent')
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = app.state.l2_cache.read_batches

    def blocked_read(**kwargs: Any) -> Any:
        started.set()
        try:
            assert release.wait(2)
            return original(**kwargs)
        finally:
            finished.set()
    monkeypatch.setattr(app.state.l2_cache, 'read_batches', blocked_read)

    async def scenario() -> None:
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        await incoming.put({'type': 'websocket.connect'})
        pending = asyncio.create_task(app(websocket_scope(), incoming.get, outgoing.put))
        try:
            assert (await asyncio.wait_for(outgoing.get(), 1))['type'] == 'websocket.accept'
            while not started.is_set(): await asyncio.sleep(.001)
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.sleep(.02)
            assert not pending.done() and app.state.l2_ws_connections == 1
            pending.cancel()
            await asyncio.sleep(.02)
            assert not pending.done() and not finished.is_set() and app.state.l2_ws_connections == 1
        finally:
            release.set()
            result = await asyncio.wait_for(asyncio.gather(pending, return_exceptions=True), 2)
        assert isinstance(result[0], asyncio.CancelledError)
        assert finished.is_set() and app.state.l2_ws_connections == 0
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
    asyncio.run(scenario())


def test_websocket_is_not_exposed_by_platform_cache_router(tmp_path: Path, websocket_network_guard: None) -> None:
    from fastapi import FastAPI
    from rquant.l2.cache import RawL2Cache
    from rquant.web.l2 import router
    app = FastAPI(); app.state.l2_cache = RawL2Cache(tmp_path / 'absent'); app.include_router(router)
    with TestClient(app) as client, pytest.raises(WebSocketDisconnect):
        with client.websocket_connect('/api/v1/l2/stream'): pytest.fail('platform exposed WS')
