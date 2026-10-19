"""The explicit loopback control app admits durable unions without upstream I/O."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
from pathlib import Path
import socket
from types import SimpleNamespace
import sys

from fastapi.testclient import TestClient
import pytest

from rquant.l2.api import create_app


def forbidden(*args: object, **kwargs: object) -> None:
    pytest.fail("control API attempted upstream I/O")


def test_explicit_control_root_commits_additive_union_and_duplicates(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    cache, control = tmp_path / "cache", tmp_path / "control"
    app = create_app(cache, control_root=control, max_symbols=3)
    assert not cache.exists() and not control.exists()
    with TestClient(app) as client:
        initial = client.get("/api/v1/l2/subscriptions").json()
        assert initial["desired"] == {"revision": 0, "symbols": [], "max_symbols": 3}
        assert initial["runtime"] is None and initial["runtime_current_epoch"] is False
        assert not control.exists()
        first = client.post("/api/v1/l2/subscriptions", json={"symbols": ["600000", "000001", "600000"]})
        assert first.status_code == 202
        assert first.json() == {"revision": 1, "symbols": ["600000", "000001"], "max_symbols": 3,
                                "added": ["600000", "000001"]}
        duplicate = client.post("/api/v1/l2/subscriptions", json={"symbols": ["000001", "600000"]})
        assert duplicate.status_code == 200 and duplicate.json()["revision"] == 1
        assert duplicate.json()["added"] == []
        assert client.post("/api/v1/l2/subscriptions", json={"symbols": ["300006"]}).status_code == 202
        before = (control / "desired.json").read_bytes()
        rejected = client.post("/api/v1/l2/subscriptions", json={"symbols": ["603258", "600000"]})
        assert rejected.status_code == 429 and rejected.json() == {"detail": {"code": "capacity_exceeded"}}
        assert (control / "desired.json").read_bytes() == before
        assert client.get("/api/v1/l2/subscriptions").json()["desired"]["symbols"] == ["600000", "000001", "300006"]
    assert not cache.exists()


@pytest.mark.parametrize("body", [
    {"symbols": []}, {"symbols": [600000]}, {"symbols": ["60000"]}, {"symbols": ["600000 "]},
    {"symbols": ["６０００００"]}, {"symbols": ["900001"]}, {"symbols": ["600000", "bad"]},
    {"symbols": ["600000"], "max_symbols": 100}, {"symbols": ["600000"], "endpoint": "127.0.0.1"},
])
def test_control_rejects_entire_invalid_request_without_writes(tmp_path: Path, body: dict) -> None:
    control = tmp_path / "control"
    with TestClient(create_app(tmp_path / "cache", control_root=control)) as client:
        assert client.post("/api/v1/l2/subscriptions", json=body).status_code == 422
    assert not control.exists()


def test_concurrent_http_union_and_capacity_are_atomic(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "cache", control_root=tmp_path / "control", max_symbols=4)) as client:
        assert client.post("/api/v1/l2/subscriptions", json={"symbols": ["600000"]}).status_code == 202
        requests = [["000001"], ["300006"], ["603258"], ["600000", "000001"]] * 3
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(lambda codes: client.post("/api/v1/l2/subscriptions", json={"symbols": codes}), requests))
        assert sum(r.status_code == 202 for r in responses) == 3
        assert all(r.status_code in {200, 202} for r in responses)
        desired = client.get("/api/v1/l2/subscriptions").json()["desired"]
        assert desired["revision"] == 4 and desired["symbols"][0] == "600000"
        assert set(desired["symbols"]) == {"600000", "000001", "300006", "603258"}
        assert client.post("/api/v1/l2/subscriptions", json={"symbols": ["601318"]}).status_code == 429
        assert client.get("/api/v1/l2/subscriptions").json()["desired"] == desired


def test_platform_get_router_never_exposes_control(tmp_path: Path) -> None:
    from fastapi import FastAPI
    from rquant.l2.cache import RawL2Cache
    from rquant.web.l2 import router
    app = FastAPI(); app.state.l2_cache = RawL2Cache(tmp_path / "cache"); app.include_router(router)
    with TestClient(app) as client:
        assert client.post("/api/v1/l2/subscriptions", json={"symbols": ["600000"]}).status_code == 404
        assert client.get("/api/v1/l2/subscriptions").status_code == 404
        assert client.get("/api/v1/l2/status").status_code == 200


@pytest.mark.parametrize("receipt_epoch,current", [("old", False), ("new", True)])
def test_runtime_snapshot_is_epoch_qualified_and_does_not_scan_history(tmp_path: Path, monkeypatch,
                                                                     receipt_epoch: str, current: bool) -> None:
    from rquant.l2.cache import RawL2Cache
    from rquant.l2.models import Binding, FeedStatus, SubscriptionReceipt, SubscriptionStream
    cache = RawL2Cache(tmp_path / "cache")
    with cache.writer():
        cache.write_status(FeedStatus(state="listening_candidate", epoch_id="new"))
        cache.write_subscription_status(SubscriptionReceipt(epoch_id=receipt_epoch, desired_revision=1,
            collector_state="listening_candidate", streams=(SubscriptionStream(
                binding=Binding(page_id=10117, code="600000", market=17, kind="vendor4013"), state="seeded",
                request_id=3, seed_sequence=1, seed_received_at=datetime.now(UTC)),)))
    monkeypatch.setattr(RawL2Cache, "_directories", forbidden)
    with TestClient(create_app(cache.root, control_root=tmp_path / "control")) as client:
        value = client.get("/api/v1/l2/subscriptions").json()
        assert value["runtime"]["epoch_id"] == receipt_epoch and value["runtime_current_epoch"] is current
        assert value["status"]["epoch_id"] == "new"


def test_corrupt_control_diagnostic_is_fixed(tmp_path: Path) -> None:
    control = tmp_path / "control"; control.mkdir(mode=0o700)
    path = control / "desired.json"; path.write_text("synthetic_private_control_value"); path.chmod(0o600)
    with TestClient(create_app(tmp_path / "cache", control_root=control)) as client:
        for response in (client.get("/api/v1/l2/subscriptions"),
                         client.post("/api/v1/l2/subscriptions", json={"symbols": ["600000"]})):
            assert response.status_code == 503
            assert "synthetic_private" not in response.text and str(control) not in response.text


def test_api_cli_capacity_policy_is_explicit_and_mutually_exclusive(tmp_path: Path, monkeypatch) -> None:
    from rquant.l2.api import main
    calls = []; monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=lambda app, **kwargs: calls.append(app)))
    policy = tmp_path / "capacity.json"
    policy.write_text(json.dumps({"max_symbols": 40, "source_tree": "a" * 40, "acceptance_sha256": "b" * 64}))
    policy.chmod(0o600); tmp_path.chmod(0o700)
    args = ["--cache-root", str(tmp_path / "cache"), "--control-root", str(tmp_path / "control")]
    assert main(args + ["--capacity-file", str(policy)]) == 0
    with TestClient(calls[0]) as client:
        assert client.get("/api/v1/l2/subscriptions").json()["desired"]["max_symbols"] == 40
    with pytest.raises(SystemExit): main(args + ["--capacity-file", str(policy), "--max-symbols", "100"])
    assert len(calls) == 1 and not (tmp_path / "control").exists()
