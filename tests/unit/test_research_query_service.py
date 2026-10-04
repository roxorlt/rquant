"""RQ-01/05/07: actual private socket, actor admission and bounded messages."""

from __future__ import annotations

import importlib
import os
import socket
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from tests.unit.test_research_query import _published
from tests.unit.test_research_query_saved import _command, _service


def _api():
    api = importlib.import_module("rquant.research_query")
    assert hasattr(api, "QueryPrivateServer"), "private query socket is not implemented"
    return api


def test_actual_uds_catalog_save_recovery_and_endpoint_identity(tmp_path: Path) -> None:
    api = _api()
    snapshot, _, _, _ = _published(tmp_path)
    executor = api.QueryExecutor(snapshot, tmp_path / "scratch")
    control = _service(tmp_path / "control")
    with TemporaryDirectory(
        prefix="rq-uds-", dir="/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
    ) as directory:
        parent = Path(directory)
        os.chown(parent, os.geteuid(), os.getegid())
        os.chmod(parent, 0o710)
        path = parent / "query.sock"
        server = api.QueryPrivateServer(
            path,
            executor=executor,
            allowed_users=frozenset({"alice"}),
            trusted_web_uid=os.geteuid(),
            shared_gid=os.getegid(),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = api.QueryPrivateClient(
            path,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: os.geteuid() + 1,
        )
        try:
            catalog = client.catalog(authenticated_actor_id="alice")
            assert [item.name for item in catalog.tables] == [
                "daily_bar",
                "adj_factor",
                "trade_calendar",
            ]
            assert all(column.description for item in catalog.tables for column in item.columns)
            assert catalog.tables[0].columns[0].description == "股票代码，如 600001.SH"
            with pytest.raises(api.QueryAdmissionRejectedError):
                client.catalog(authenticated_actor_id="viewer")
            with pytest.raises(TypeError):
                client.catalog(authenticated_actor_id="alice", owner_id="bob")
            result = client.execute(
                api.QueryRequest(sql="SELECT 1"), authenticated_actor_id="alice"
            )
            assert result.status in {"ready", "unavailable"}
            os.chmod(path, 0o666)
            with pytest.raises(api.QueryAdmissionUnavailableError):
                client.catalog(authenticated_actor_id="alice")
            os.chmod(path, 0o660)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        assert not path.exists() and not thread.is_alive()
        path = parent / "save.sock"
        server = api.QueryPrivateServer(
            path,
            control=control,
            allowed_users=frozenset({"alice", "bob"}),
            trusted_web_uid=os.geteuid(),
            shared_gid=os.getegid(),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = api.QueryPrivateClient(
            path,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: os.geteuid() + 1,
        )
        try:
            saved = client.save(_command(), authenticated_actor_id="alice")
            assert saved.receipt.status == "succeeded"
            assert client.save(_command(), authenticated_actor_id="alice", resume=True) == saved
            assert len(client.saved(authenticated_actor_id="alice").items) == 1
            assert client.saved(authenticated_actor_id="bob").items == ()
            with pytest.raises(api.QueryAdmissionRejectedError):
                client.save(_command(), authenticated_actor_id="bob", resume=True)
            with pytest.raises(api.QueryAdmissionRejectedError):
                client.execute(api.QueryRequest(sql="SELECT 1"), authenticated_actor_id="alice")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        assert not path.exists() and not thread.is_alive()


def test_wrong_peer_is_rejected_before_any_request_is_read(tmp_path: Path) -> None:
    api = _api()
    control = _service(tmp_path)
    with TemporaryDirectory(
        prefix="rq-deny-", dir="/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
    ) as directory:
        parent = Path(directory)
        os.chown(parent, os.geteuid(), os.getegid())
        os.chmod(parent, 0o710)
        path = parent / "deny.sock"
        server = api.QueryPrivateServer(
            path,
            control=control,
            allowed_users=frozenset({"alice"}),
            trusted_web_uid=os.geteuid() + 1,
            shared_gid=os.getegid(),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with socket.socket(socket.AF_UNIX) as connection:
                connection.settimeout(1)
                connection.connect(str(path))
                assert connection.recv(1) == b""
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        assert not path.exists()
