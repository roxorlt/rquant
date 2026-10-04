"""RQ-01/07: Web API identity/role/CSRF and trusted actor forwarding."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, create_proof_test_app
from tests.unit.test_research_query import _published
from tests.unit.test_research_query_saved import _command, _service


def test_complete_http_envelope_also_respects_16mib(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.research_query import QueryColumn, QueryPrivateClient, QueryResult
    from rquant.web.routes import research_query as route

    settings = WebSettings(
        serving_root=tmp_path / "serving",
        ingress_socket_path=tmp_path / "ingress/web.sock",
        research_query_users=frozenset({"alice"}),
        research_query_socket_path=Path("/private/tmp/rq-absent-wire/service.sock"),
        research_query_service_uid=99999,
        research_query_shared_gid=99999,
    )
    value = QueryResult(
        status="ready",
        columns=(QueryColumn(name="value", data_type="VARCHAR"),),
        rows=(("x" * (16 * 2**20 - 2048),),),
        elapsed_ms=1,
    )

    def result(*args: object, **kwargs: object) -> QueryResult:
        return value

    monkeypatch.setattr(QueryPrivateClient, "execute", result)
    original_meta = route._meta

    def large_metadata(request):
        return original_meta(request).model_copy(update={"detail": "y" * 4096})

    monkeypatch.setattr(route, "_meta", large_metadata)
    with ProofTestClient(create_proof_test_app(settings, background=False)) as client:
        response = client.post(
            "/api/v1/research/query",
            json={"sql": "SELECT 1"},
            headers={"x-rquant-user": "alice", "x-rquant-csrf": "1", "origin": "http://testserver"},
        )
    assert len(response.content) <= 16 * 2**20
    assert response.status_code == 503


def test_query_routes_require_verified_user_role_and_csrf(tmp_path: Path) -> None:
    assert "research_query_users" in WebSettings.model_fields, (
        "query API settings are not implemented"
    )
    settings = WebSettings(
        serving_root=tmp_path / "serving",
        ingress_socket_path=tmp_path / "ingress" / "web.sock",
        research_query_users=frozenset({"alice"}),
        research_query_socket_path=Path("/private/tmp/rq-absent-query/service.sock"),
        research_query_service_uid=99999,
        research_query_shared_gid=99999,
    )
    app = create_proof_test_app(settings, background=False)
    with ProofTestClient(app) as client:
        assert client.post("/api/v1/research/query", json={"sql": "SELECT 1"}).status_code in {
            401,
            403,
        }
        assert (
            client.post(
                "/api/v1/research/query",
                headers={"x-rquant-user": "viewer", "x-rquant-csrf": "1"},
                json={"sql": "SELECT 1"},
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/v1/research/query",
                headers={"x-rquant-user": "alice"},
                json={"sql": "SELECT 1"},
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/v1/research/query",
                headers={
                    "x-rquant-user": "alice",
                    "x-rquant-csrf": "1",
                    "origin": "http://evil.example",
                },
                json={"sql": "SELECT 1"},
            ).status_code
            == 403
        )
        allowed = {"x-rquant-user": "alice", "x-rquant-csrf": "1", "origin": "http://testserver"}
        assert (
            client.post(
                "/api/v1/research/query",
                headers=allowed,
                json={"sql": "SELECT 1", "owner_id": "bob"},
            ).status_code
            == 422
        )
        response = client.post("/api/v1/research/query", headers=allowed, json={"sql": "SELECT 1"})
        assert response.status_code == 503
        assert str(tmp_path) not in response.text
        assert (
            client.get("/api/v1/research/queries", headers={"x-rquant-user": "viewer"}).status_code
            == 403
        )
    default = create_proof_test_app(WebSettings(serving_root=tmp_path), background=False)
    with ProofTestClient(default) as client:
        assert (
            client.post(
                "/api/v1/research/query",
                headers={"x-rquant-user": "alice", "x-rquant-csrf": "1"},
                json={"sql": "SELECT 1"},
            ).status_code
            == 401
        )


def test_settings_private_socket_and_exact_user_list_fail_closed(tmp_path: Path) -> None:
    assert "research_query_users" in WebSettings.model_fields
    with pytest.raises(ValueError):
        WebSettings(serving_root=tmp_path, research_query_users=frozenset({"alice"}))
    with pytest.raises(ValueError):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=tmp_path / "web.sock",
            research_query_socket_path=tmp_path / "query.sock",
            research_query_service_uid=99999,
            research_query_shared_gid=99999,
            research_query_users=frozenset({"alice"}),
        )


def test_verified_api_forwards_actual_uds_and_owner_bound_save(tmp_path: Path) -> None:
    from rquant.research_query import QueryExecutor, QueryPrivateClient, QueryPrivateServer

    snapshot, _, _, _ = _published(tmp_path)
    control = _service(tmp_path / "control")
    with TemporaryDirectory(
        prefix="rq-api-", dir="/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
    ) as directory:
        root = Path(directory)
        sockets = []
        servers = []
        threads = []
        clients = []
        try:
            for role in ("query", "save"):
                parent = root / role
                parent.mkdir(mode=0o710)
                os.chown(parent, os.geteuid(), os.getegid())
                parent.chmod(0o710)
                path = parent / "service.sock"
                sockets.append(path)
                server = QueryPrivateServer(
                    path,
                    allowed_users=frozenset({"alice", "bob"}),
                    trusted_web_uid=os.geteuid(),
                    shared_gid=os.getegid(),
                    executor=QueryExecutor(snapshot, root / "scratch") if role == "query" else None,
                    control=control if role == "save" else None,
                )
                servers.append(server)
                thread = threading.Thread(target=server.serve_forever)
                threads.append(thread)
                thread.start()
                clients.append(
                    QueryPrivateClient(
                        path,
                        expected_service_uid=os.geteuid(),
                        shared_gid=os.getegid(),
                        client_uid=lambda: os.geteuid() + 1,
                    )
                )
            # These local sockets test RPC. Distinct OS users remain an installation gate.
            settings = WebSettings(
                serving_root=tmp_path / "serving",
                ingress_socket_path=tmp_path / "ingress/web.sock",
                research_query_users=frozenset({"alice", "bob"}),
                research_query_socket_path=sockets[0],
                research_query_service_uid=99998,
                research_query_shared_gid=os.getegid(),
                research_query_save_socket_path=sockets[1],
                research_query_save_service_uid=99999,
            )
            app = create_proof_test_app(
                settings,
                background=False,
                research_query_client=clients[0],
                research_query_save_client=clients[1],
            )
            with ProofTestClient(app) as client:
                alice = {
                    "x-rquant-user": "alice",
                    "x-rquant-csrf": "1",
                    "origin": "http://testserver",
                }
                bob = {**alice, "x-rquant-user": "bob"}
                catalog = client.get("/api/v1/research/catalog", headers=alice)
                assert catalog.status_code == 200 and catalog.json()["data"]["save_enabled"]
                assert catalog.json()["data"]["tables"][0]["columns"][0]["description"]
                run = client.post("/api/v1/research/query", headers=alice, json={"sql": "SELECT 1"})
                assert run.status_code == 200
                assert run.json()["data"]["status"] in {"ready", "unavailable"}
                command = _command().model_dump(mode="json")
                response = client.post("/api/v1/research/queries/save", headers=alice, json=command)
                assert (
                    response.status_code == 200
                    and response.json()["data"]["receipt"]["status"] == "succeeded"
                )
                assert (
                    client.post(
                        "/api/v1/research/queries/resume", headers=alice, json=command
                    ).json()
                    == response.json()
                )
                assert (
                    client.get("/api/v1/research/queries", headers=bob).json()["data"]["items"]
                    == []
                )
                assert (
                    len(
                        client.get("/api/v1/research/queries", headers=alice).json()["data"][
                            "items"
                        ]
                    )
                    == 1
                )
                assert (
                    client.post(
                        "/api/v1/research/queries/resume", headers=bob, json=command
                    ).status_code
                    == 409
                )
                assert (
                    client.post(
                        "/api/v1/research/queries/save",
                        headers=alice,
                        json={**command, "owner_id": "bob"},
                    ).status_code
                    == 422
                )
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=2)
        assert all(not item.exists() for item in sockets)
        assert all(not item.is_alive() for item in threads)
