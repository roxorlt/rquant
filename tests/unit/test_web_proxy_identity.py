"""A user name has authority only when a configured private proxy proves it."""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import socket
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from rquant.watchlist_admission import WatchlistAdmissionClient
from rquant.web.app import create_app
from rquant.web.ingress import private_web_ingress_socket
from rquant.web.security import current_user
from rquant.web.settings import WebSettings

SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def _proof_file(tmp_path: Path) -> tuple[Path, str]:
    token = secrets.token_hex(32)
    path = tmp_path / "proxy-proof"
    path.write_text(token, encoding="ascii")
    path.chmod(0o400)
    return path, token


def _private_app(tmp_path: Path, path: Path) -> FastAPI:
    return create_app(
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=tmp_path / "web.sock",
            proxy_proof_file=path,
        ),
        background=False,
    )


def test_unconfigured_user_header_cannot_create_a_viewer(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path), background=False)
    with TestClient(app) as client:
        response = client.get("/api/v1/meta", headers={"X-Rquant-User": "alice"})
    assert response.status_code == 200
    assert response.json()["data"]["viewer"] is None


def test_private_ingress_without_proxy_proof_cannot_read_a_watchlist(tmp_path: Path) -> None:
    settings = WebSettings(
        serving_root=tmp_path,
        ingress_socket_path=tmp_path / "web.sock",
    )
    app = create_app(settings, background=False)
    with TestClient(app) as client:
        response = client.get("/api/v1/watchlist", headers={"X-Rquant-User": "alice"})
    assert response.status_code == 401


def test_valid_proxy_proof_creates_only_the_named_owner(tmp_path: Path) -> None:
    path, token = _proof_file(tmp_path)
    with TestClient(_private_app(tmp_path, path)) as client:
        valid = client.get(
            "/api/v1/meta",
            headers={"X-Rquant-User": "alice", "X-Rquant-Proxy-Proof": token},
        )
        private = client.get(
            "/api/v1/watchlist",
            headers={"X-Rquant-User": "alice", "X-Rquant-Proxy-Proof": token},
        )
    assert valid.json()["data"]["viewer"] == "alice"
    assert private.status_code == 200


@pytest.mark.parametrize(
    "headers",
    (
        [("X-Rquant-User", "alice")],
        [("X-Rquant-Proxy-Proof", "__VALID__")],
        [("X-Rquant-User", "alice"), ("X-Rquant-Proxy-Proof", "wrong")],
        [("X-Rquant-User", ""), ("X-Rquant-Proxy-Proof", "__VALID__")],
        [
            ("X-Rquant-User", "alice"),
            ("x-rquant-user", "bob"),
            ("X-Rquant-Proxy-Proof", "__VALID__"),
        ],
        [("X-Rquant-User", "alice"), ("X-Rquant-Proxy-Proof", "short")],
    ),
)
def test_missing_wrong_or_duplicate_identity_headers_fail_closed(
    tmp_path: Path, headers: list[tuple[str, str]]
) -> None:
    path, token = _proof_file(tmp_path)
    actual = [(key, token if value == "__VALID__" else value) for key, value in headers]
    with TestClient(_private_app(tmp_path, path)) as client:
        response = client.get("/api/v1/watchlist", headers=actual)
    assert response.status_code == 401


def test_duplicate_proxy_proof_and_bad_user_bytes_fail_closed(tmp_path: Path) -> None:
    path, token = _proof_file(tmp_path)
    with TestClient(_private_app(tmp_path, path)) as client:
        duplicate = client.get(
            "/api/v1/watchlist",
            headers=[
                ("X-Rquant-User", "alice"),
                ("X-Rquant-Proxy-Proof", token),
                ("x-rquant-proxy-proof", token),
            ],
        )
        bad_encoding = client.get(
            "/api/v1/watchlist",
            headers=[(b"X-Rquant-User", b"\xff"), (b"X-Rquant-Proxy-Proof", token.encode())],
        )
    assert duplicate.status_code == 401
    assert bad_encoding.status_code == 401


@pytest.mark.parametrize(
    "fault", ("missing", "writable", "group_readable", "symlink", "short", "parent")
)
def test_bad_runtime_credential_never_activates_identity(tmp_path: Path, fault: str) -> None:
    path, token = _proof_file(tmp_path)
    if fault == "missing":
        path.unlink()
    elif fault == "writable":
        path.chmod(0o600)
    elif fault == "group_readable":
        path.chmod(0o440)
    elif fault == "symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(path)
        path = alias
    elif fault == "short":
        path.chmod(0o600)
        path.write_text("short", encoding="ascii")
        path.chmod(0o400)
    else:
        os.chmod(tmp_path, 0o777)
    try:
        with TestClient(_private_app(tmp_path, path)) as client:
            response = client.get(
                "/api/v1/meta",
                headers={"X-Rquant-User": "alice", "X-Rquant-Proxy-Proof": token},
            )
        assert response.status_code == 200
        assert response.json()["data"]["viewer"] is None
    finally:
        if fault == "parent":
            os.chmod(tmp_path, 0o700)


def test_replacing_a_loaded_credential_invalidates_it_until_restart(tmp_path: Path) -> None:
    path, token = _proof_file(tmp_path)
    app = _private_app(tmp_path, path)
    with TestClient(app) as client:
        headers = {"X-Rquant-User": "alice", "X-Rquant-Proxy-Proof": token}
        assert client.get("/api/v1/meta", headers=headers).json()["data"]["viewer"] == "alice"
        new_dir = tmp_path / "new"
        new_dir.mkdir()
        replacement, _new_token = _proof_file(new_dir)
        replacement.replace(path)
        assert client.get("/api/v1/meta", headers=headers).json()["data"]["viewer"] is None


def test_proxy_proof_path_requires_an_explicit_private_ingress(tmp_path: Path) -> None:
    path, _token = _proof_file(tmp_path)
    with pytest.raises(ValueError, match="private Web ingress"):
        WebSettings(serving_root=tmp_path, proxy_proof_file=path)
    loaded = WebSettings.from_env(
        {
            "RQUANT_SERVING_ROOT": str(tmp_path),
            "RQUANT_WEB_INGRESS_SOCKET": str(tmp_path / "ingress" / "web.sock"),
            "RQUANT_WEB_PROXY_PROOF_FILE": str(path),
        }
    )
    assert loaded.proxy_proof_file == path
    with pytest.raises(ValueError, match="absolute"):
        WebSettings.from_env(
            {
                "RQUANT_WEB_INGRESS_SOCKET": str(tmp_path / "ingress" / "web.sock"),
                "RQUANT_WEB_PROXY_PROOF_FILE": "relative-proof",
            }
        )


@pytest.mark.parametrize(
    "route",
    (
        "/api/v1/pools/editor",
        "/api/v1/paper/accounts",
        "/api/v1/monitor/timeline",
        "/api/v1/monitor/price-rules",
        "/api/v1/monitor/price-rules/head?rule_id=synthetic-rule",
        "/api/v1/strategy-templates",
        "/api/v1/strategy-templates/sources",
        "/api/v1/strategy-templates/template_synthetic",
        "/api/v1/strategy-templates/template_synthetic/versions",
        "/api/v1/tasks/jobs",
        "/api/v1/backtests",
        "/api/v1/backtests/portfolio/capabilities",
        "/api/v1/backtests/portfolio/runs",
        "/api/v1/backtests/portfolio/runs/12345678-1234-5678-9abc-123456789012",
        "/api/v1/backtests/portfolio/runs/12345678-1234-5678-9abc-123456789012/nav",
        "/api/v1/backtests/portfolio/runs/12345678-1234-5678-9abc-123456789012/rows",
        "/api/v1/backtests/portfolio/runs/12345678-1234-5678-9abc-123456789012/report.html",
        "/api/v1/backtests/portfolio/runs/12345678-1234-5678-9abc-123456789012/exports/12345678-1234-5678-9abc-123456789013.zip",
        "/api/v1/screen/tdx/market/jobs",
        "/api/v1/pools/formula",
        "/api/v1/data/report",
        "/api/v1/data/audit-report/calendar",
        "/api/v1/data/backfill-plans",
    ),
)
def test_private_reads_reject_a_bare_user_header(tmp_path: Path, route: str) -> None:
    path, _token = _proof_file(tmp_path)
    with TestClient(_private_app(tmp_path, path)) as client:
        response = client.get(route, headers={"X-Rquant-User": "alice"})
    assert response.status_code == 401


def test_public_non_identity_read_still_works_without_proxy_proof(tmp_path: Path) -> None:
    path, _token = _proof_file(tmp_path)
    with TestClient(_private_app(tmp_path, path)) as client:
        response = client.get("/api/v1/overview", headers={"X-Rquant-User": "alice"})
    assert response.status_code == 200


def test_unproved_watchlist_write_never_reaches_admission(tmp_path: Path) -> None:
    class NeverAdmit:
        calls = 0

        def lookup(self, *_args: object, **_kwargs: object) -> None:
            self.calls += 1
            pytest.fail("unproved command reached private admission")

    path, _token = _proof_file(tmp_path)
    admission = NeverAdmit()
    app = create_app(
        WebSettings(
            serving_root=tmp_path / "serving",
            ingress_socket_path=tmp_path / "ingress" / "web.sock",
            proxy_proof_file=path,
            watchlist_admission_socket_path=tmp_path / "admission" / "watchlist.sock",
        ),
        watchlist_admission_client=cast(WatchlistAdmissionClient, admission),
        background=False,
    )
    body = {
        "command_id": "forged-command",
        "requested_at": datetime.now(UTC).isoformat(),
        "generation_id": "a" * 64,
        "ts_code": "600001.SH",
        "action": "add",
        "source": "detail",
    }
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/watchlist/commands",
            json=body,
            headers={
                "X-Rquant-User": "alice",
                "X-Rquant-Csrf": "1",
                "Origin": "http://testserver",
            },
        )
    assert response.status_code == 401
    assert admission.calls == 0


@pytest.mark.parametrize(
    ("route", "proved_status"),
    (
        ("/api/v1/screen/tdx/parse", 200),
        ("/api/v1/screen/tdx/preview", 422),
        ("/api/v1/screen/run", 422),
    ),
)
def test_screen_computation_requires_a_proved_identity(
    tmp_path: Path, route: str, proved_status: int
) -> None:
    path, token = _proof_file(tmp_path)
    with TestClient(_private_app(tmp_path, path)) as client:
        unproved = client.post(
            route,
            json={},
            headers={"X-Rquant-User": "alice", "X-Rquant-Csrf": "1"},
        )
        proved = client.post(
            route,
            json={},
            headers={
                "X-Rquant-User": "alice",
                "X-Rquant-Proxy-Proof": token,
                "X-Rquant-Csrf": "1",
            },
        )
    assert unproved.status_code == 401
    assert proved.status_code == proved_status


def test_current_user_route_inventory_matches_documented_categories(tmp_path: Path) -> None:
    document = (
        Path(__file__).resolve().parents[2]
        / "docs/plans/2026-09-29-web-private-identity-route-inventory.md"
    ).read_text(encoding="utf-8")
    documented = re.findall(r"\| (GET|POST) \| `(/api/v1[^`]+)` \|", document)
    assert len(documented) == len(set(documented)) == 110

    app = create_app(WebSettings(serving_root=tmp_path), background=False)

    def consumes_identity(route: APIRoute) -> bool:
        def uses(dependency: object) -> bool:
            return getattr(dependency, "call", None) is current_user or any(
                uses(child) for child in getattr(dependency, "dependencies", ())
            )

        return uses(route.dependant)

    actual = {
        (method, included.include_context.prefix + route.path)
        for included in app.router.routes
        if hasattr(included, "original_router")
        for route in included.original_router.routes
        if isinstance(route, APIRoute) and consumes_identity(route)
        for method in route.methods
    }
    assert set(documented) == actual


def test_real_private_socket_accepts_only_the_proved_proxy_identity(tmp_path: Path) -> None:
    proof_path, token = _proof_file(tmp_path)
    with TemporaryDirectory(prefix="rqi-", dir=SHORT_TMP) as directory:
        ingress = Path(directory) / "ingress"
        ingress.mkdir(mode=0o710)
        os.chown(ingress, -1, os.getegid())
        ingress.chmod(0o710)
        socket_path = ingress / "web.sock"
        app = create_app(
            WebSettings(
                serving_root=tmp_path / "serving",
                ingress_socket_path=socket_path,
                proxy_proof_file=proof_path,
            ),
            background=False,
        )
        with private_web_ingress_socket(socket_path, nginx_group_gid=os.getegid()) as listener:
            server = uvicorn.Server(
                uvicorn.Config(
                    app,
                    fd=listener.fileno(),
                    workers=1,
                    lifespan="off",
                    log_level="critical",
                    proxy_headers=False,
                )
            )

            async def request(*, supplied_proof: bytes | None) -> dict[str, object]:
                reader, writer = await asyncio.open_unix_connection(str(socket_path))
                headers = [
                    b"GET /api/v1/meta HTTP/1.1",
                    b"Host: test",
                    b"X-Rquant-User: alice",
                    b"Connection: close",
                ]
                if supplied_proof is not None:
                    headers.append(b"X-Rquant-Proxy-Proof: " + supplied_proof)
                writer.write(b"\r\n".join(headers) + b"\r\n\r\n")
                await writer.drain()
                response = await reader.read()
                writer.close()
                await writer.wait_closed()
                assert response.startswith(b"HTTP/1.1 200 OK")
                return json.loads(response.split(b"\r\n\r\n", 1)[1])

            async def exercise() -> None:
                task = asyncio.create_task(server.serve())
                try:
                    for _ in range(100):
                        if server.started:
                            break
                        await asyncio.sleep(0.01)
                    assert server.started
                    assert len(server.servers) == 1
                    assert server.servers[0].sockets[0].family == socket.AF_UNIX
                    verified = await request(supplied_proof=token.encode("ascii"))
                    forged = await request(supplied_proof=None)
                    wrong = await request(supplied_proof=b"0" * 64)
                    assert verified["data"]["viewer"] == "alice"
                    assert forged["data"]["viewer"] is None
                    assert wrong["data"]["viewer"] is None
                finally:
                    server.should_exit = True
                    await asyncio.wait_for(task, timeout=5)

            asyncio.run(exercise())
